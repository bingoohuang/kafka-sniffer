package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/bingoohuang/kafka-sniffer/metrics"
	"github.com/bingoohuang/kafka-sniffer/stream"

	"github.com/google/gopacket"
	"github.com/google/gopacket/examples/util"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/google/gopacket/tcpassembly"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	iface      = flag.String("i", "eth0", "Interface to get packets from")
	bpf        = flag.String("bpf", "tcp and dst port 9092", "BPF expr")
	snaplen    = flag.Int("snap", 16<<10, "SnapLen for pcap packet capture")
	verbose    = flag.Bool("v", false, "Logs every packet in great detail")
	rwPrint    = flag.Bool("s", true, "Print the read and write clients")
	listenAddr = flag.String("addr", ":9870", "Address on which sniffer listen the requests, e.g. :9870")
	expireTime = flag.Duration("metrics.expire-time", 5*time.Minute, "Expiration time of metric.")

	clientStat *stream.ClientStat
)

func main() {
	defer util.Run()()
	log.Printf("starting capture on interface %q", *iface)

	// Set up pcap packet capture
	handle, err := pcap.OpenLive(*iface, int32(*snaplen), true, pcap.BlockForever)
	if err != nil {
		panic(err)
	}

	if err := handle.SetBPFFilter(*bpf); err != nil {
		panic(err)
	}

	// init metrics storage
	metricsStorage := metrics.NewStorage(prometheus.DefaultRegisterer, *expireTime)

	// Set up assembly
	var f tcpassembly.StreamFactory
	if *rwPrint {
		clientStat = stream.NewClientStat()
		f = stream.NewKafkaStreamClientPrintFactory(clientStat)
	} else {
		f = stream.NewKafkaStreamFactory(metricsStorage, *verbose)
	}

	go runTelemetry()

	streamPool := tcpassembly.NewStreamPool(f)
	assembler := tcpassembly.NewAssembler(streamPool)

	// Auto-flushing connection state to get packets
	// without waiting SYN
	assembler.MaxBufferedPagesTotal = 1000
	assembler.MaxBufferedPagesPerConnection = 1

	if *verbose {
		log.Println("reading in packets")
	}

	// Read in packets, pass to assembler.
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	packets := packetSource.Packets()
	ticker := time.Tick(time.Minute)

	for {
		select {
		case p := <-packets:
			if *verbose {
				log.Println(p)
			}

			n := p.NetworkLayer()
			t := p.TransportLayer()
			if n == nil || t == nil || t.LayerType() != layers.LayerTypeTCP {
				if *verbose {
					log.Println("Unusable packet")
				}
				continue
			}

			tcp := t.(*layers.TCP)
			assembler.AssembleWithTimestamp(n.NetworkFlow(), tcp, p.Metadata().Timestamp)

		case <-ticker:
			// Every minute, flush connections that haven't seen activity in the past 2 minutes.
			assembler.FlushOlderThan(time.Now().Add(time.Minute * -2))
			if *verbose {
				log.Println("---- FLUSHING ----")
			}
		}
	}
}

func runTelemetry() {
	fmt.Printf("serving metrics on %s\n", *listenAddr)

	http.Handle("/metrics", promhttp.Handler())
	if clientStat != nil {
		http.Handle("/client", stream.ServeClientStatHandler(clientStat))
	}

	if err := http.ListenAndServe(*listenAddr, nil); err != nil {
		panic(err)
	}
}
