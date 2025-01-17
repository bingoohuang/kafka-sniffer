package stream

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/bingoohuang/kafka-sniffer/kafka"
	"io"
	"log"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
)

// KafkaPrintStreamFactory implements tcpassembly.StreamFactory
type KafkaPrintStreamFactory struct {
	ClientStat        *ClientStat
	printJsonDuration time.Duration
	printType         string
}

// NewKafkaClientPrintStreamFactory assembles streams
func NewKafkaClientPrintStreamFactory(printJsonDuration time.Duration, printType string) *KafkaPrintStreamFactory {
	f := &KafkaPrintStreamFactory{
		ClientStat:        NewClientStat(),
		printJsonDuration: printJsonDuration,
		printType:         strings.ToLower(printType),
	}

	go func() {
		d := 1 * time.Minute
		t := time.NewTicker(d)
		defer t.Stop()

		for range t.C {
			f.ClientStat.Recycle(d)
		}
	}()

	return f
}

// New assembles new stream
func (h *KafkaPrintStreamFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	s := &kafkaStreamPrinter{
		net:       net,
		transport: transport,
		r:         tcpreader.NewReaderStream(),
		factory:   h,
	}

	go s.run() // Important... we must guarantee that data from the reader stream is read.

	return &s.r
}

func (h *kafkaStreamPrinter) run() {
	buf := bufio.NewReaderSize(&h.r, 2<<15) // 65k
	src := fmt.Sprintf("%s:%s", h.net.Src(), h.transport.Src())
	dst := fmt.Sprintf("%s:%s", h.net.Dst(), h.transport.Dst())

	start := time.Now()
	for {
		r, n, err := kafka.DecodeRequest(buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			log.Printf("conn: %s -> %s EOF", src, dst)
			h.factory.ClientStat.Clear(src, dst)
			return
		}

		if err != nil {
			if _, ok := err.(kafka.PacketDecodingError); ok {
				if _, err := buf.Discard(n); err != nil {
					log.Printf("could not discard: %s\n", err)
				}
			}

			continue
		}

		typ := reflect.TypeOf(r.Body).String()
		isPrintType := strings.Contains(strings.ToLower(typ), h.factory.printType)

		if t, ok := r.Body.(interface {
			ExtractTopics() []string
		}); ok {
			topics := t.ExtractTopics()
			if h.factory.ClientStat.Stat(src, dst, r.ClientID, typ, topics, n) {
				if isPrintType {
					// CorrelationId，int32类型，由客户端指定的一个数字唯一标示这次请求的id，
					// 服务器端在处理完请求后也会把同样的CorrelationId写到Response中，这样客户端就能把某个请求和响应对应起来了
					log.Printf("conn: %s -> %s, type: %s topics: %s, correlationID: %d, clientID: %s",
						src, dst, typ, topics, r.CorrelationID, r.ClientID)
				}
			}
		}

		if isPrintType {
			if h.factory.printJsonDuration > 0 && time.Since(start) > h.factory.printJsonDuration {
				start = time.Now()

				log.Printf("conn: %s -> %s, correlationID: %d, clientID: %s", src, dst, r.CorrelationID, r.ClientID)
				if b, err := json.Marshal(r.Body); err != nil {
					log.Printf("json marshal failed: %s", err)
				} else {
					log.Printf("%s: %s", typ, b)
				}
			}
		}
	}
}

// kafkaStreamPrinter will handle the actual decoding of http requests.
type kafkaStreamPrinter struct {
	net, transport gopacket.Flow
	r              tcpreader.ReaderStream
	factory        *KafkaPrintStreamFactory
}

type Key struct {
	Src     string
	Dst     string
	ReqType string
}

type ReqTypeStatItemSnapshot struct {
	Key

	Start     time.Time
	ClientID  string
	Requests  int
	BytesRead int
	Topics    []string
}

type ReqTypeStatItem struct {
	ReqTypeStatItemSnapshot

	topicsMap map[string]bool
	Eof       bool
	EofTime   *time.Time
	Update    time.Time
}

type ClientStat struct {
	lock sync.Mutex
	Map  map[Key]*ReqTypeStatItem
}

func NewClientStat() *ClientStat {
	return &ClientStat{
		Map: map[Key]*ReqTypeStatItem{},
	}
}

func (s *ClientStat) Snapshot() (ret []ReqTypeStatItemSnapshot) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, v := range s.Map {
		ret = append(ret, v.ReqTypeStatItemSnapshot)
	}

	return
}

func (s *ClientStat) Stat(src, dst, clientID, typ string, topics []string, n int) (newTyp bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	k := Key{
		Src:     src,
		Dst:     dst,
		ReqType: typ,
	}
	r, ok := s.Map[k]
	if !ok {
		r = &ReqTypeStatItem{
			ReqTypeStatItemSnapshot: ReqTypeStatItemSnapshot{
				Key:      k,
				Start:    time.Now(),
				ClientID: clientID,
			},
			topicsMap: map[string]bool{}}
		s.Map[k] = r
	}

	r.Requests++
	r.BytesRead += n
	r.ClientID = clientID
	r.Update = time.Now()

	for _, topic := range topics {
		if !r.topicsMap[topic] {
			r.topicsMap[topic] = true
			r.Topics = append(r.Topics, topic)
		}
	}

	return !ok
}

func (s *ClientStat) Clear(src, dst string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	now := time.Now()
	for k, v := range s.Map {
		if k.Src == src && k.Dst == dst {
			v.Eof = true
			v.EofTime = &now
		}
	}
}

func (s *ClientStat) Recycle(expire time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for k, v := range s.Map {
		if v.Eof && time.Since(*v.EofTime) > expire {
			delete(s.Map, k)
		}
	}
}

func ServeClientStatHandler(stat *ClientStat) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s := stat.Snapshot()
		sort.SliceStable(s, func(i, j int) bool {
			return s[i].Src < s[j].Src ||
				s[i].Dst < s[j].Dst ||
				s[i].ReqType < s[j].ReqType ||
				s[i].ClientID < s[j].ClientID
		})

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(s)
	}
}
