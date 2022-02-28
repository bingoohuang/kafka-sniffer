package flowd

import (
	"encoding/json"
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"net/http"
	"sync"
	"time"
)

type Flow struct {
	Src        string
	Dst        string
	PayloadSum int
	Closed     bool
	Update     time.Time
}

type FlowTable struct {
	lock sync.Mutex
	Map  map[string]*Flow
}

func (t *FlowTable) Snapshot() (flows []Flow) {
	t.lock.Lock()
	defer t.lock.Unlock()

	for _, v := range t.Map {
		flows = append(flows, *v)
	}
	return
}

func (t *FlowTable) Update(src, dst string, payloadSize int, closed bool) {
	t.lock.Lock()
	defer t.lock.Unlock()

	k := src + "->" + dst
	flow, ok := t.Map[k]
	if !ok {
		flow = &Flow{Src: src, Dst: dst}
		t.Map[k] = flow
	}

	flow.PayloadSum += payloadSize
	flow.Closed = closed
	flow.Update = time.Now()
}

func (t *FlowTable) Clear(expire time.Duration) {
	t.lock.Lock()
	defer t.lock.Unlock()

	for k, v := range t.Map {
		if v.Closed && time.Since(v.Update) >= expire {
			delete(t.Map, k)
		}
	}
}

func RunNetworkAnalyzer(device, bpf string, snaplen int32) (http.HandlerFunc, error) {
	handle, err := pcap.OpenLive(device, snaplen /*1600*/, true, pcap.BlockForever)
	if err != nil {
		return nil, err
	}

	if bpf != "" {
		if err := handle.SetBPFFilter(bpf); err != nil {
			return nil, err
		}
	}

	flowTable := &FlowTable{Map: map[string]*Flow{}}

	go func() {
		ticker := time.Tick(time.Minute)
		packets := gopacket.NewPacketSource(handle, handle.LinkType()).Packets()
		for {
			select {
			case p := <-packets:
				ipLayer := p.Layer(layers.LayerTypeIPv4)
				tcpLayer := p.Layer(layers.LayerTypeTCP)

				if ipLayer != nil && tcpLayer != nil {
					ip, _ := ipLayer.(*layers.IPv4)
					tcp, _ := tcpLayer.(*layers.TCP)

					src := fmt.Sprintf("%s:%d", ip.SrcIP, tcp.SrcPort)
					dst := fmt.Sprintf("%s:%d", ip.DstIP, tcp.DstPort)

					// terminate c
					closed := tcp.FIN || tcp.RST
					flowTable.Update(src, dst, len(tcp.Payload), closed)
				}
			case <-ticker:
				flowTable.Clear(time.Minute)
			}
		}
	}()

	type FlowResponse struct {
		Total int
		Flows []Flow
	}

	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		flows := flowTable.Snapshot()
		json.NewEncoder(w).Encode(FlowResponse{
			Total: len(flows),
			Flows: flows,
		})
	}, nil
}
