package stream

import (
	"bufio"
	"io"
	"log"
	"reflect"

	"github.com/bingoohuang/kafka-sniffer/kafka"

	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
)

// KafkaStreamPrintFactory implements tcpassembly.StreamFactory
type KafkaStreamPrintFactory struct {
}

// NewKafkaStreamClientPrintFactory assembles streams
func NewKafkaStreamClientPrintFactory() *KafkaStreamPrintFactory {
	return &KafkaStreamPrintFactory{}
}

// New assembles new stream
func (h *KafkaStreamPrintFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	s := &kafkaStreamPrinter{
		net:       net,
		transport: transport,
		r:         tcpreader.NewReaderStream(),
	}

	go s.run() // Important... we must guarantee that data from the reader stream is read.

	return &s.r
}

// kafkaStreamPrinter will handle the actual decoding of http requests.
type kafkaStreamPrinter struct {
	net, transport gopacket.Flow
	r              tcpreader.ReaderStream
}

func (h *kafkaStreamPrinter) run() {
	buf := bufio.NewReaderSize(&h.r, 2<<15) // 65k
	reqTepePrintedMap := map[string]bool{}

	for {
		req, n, err := kafka.DecodeRequest(buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
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

		if t, ok := req.Body.(interface {
			ExtractTopics() []string
		}); ok {
			typ := reflect.TypeOf(req.Body).String()
			if reqTepePrintedMap[typ] {
				continue
			}

			reqTepePrintedMap[typ] = true

			// CorrelationId，int32类型，由客户端指定的一个数字唯一标示这次请求的id，服务器端在处理完请求后也会把同样的CorrelationId写到Response中，这样客户端就能把某个请求和响应对应起来了
			log.Printf("client %s:%s-%s:%s type: %s topic %s, correlationID: %d, clientID: %s",
				h.net.Src(), h.transport.Src(), h.net.Dst(), h.transport.Dst(),
				typ, t.ExtractTopics(), req.CorrelationID, req.ClientID)
		}
	}
}
