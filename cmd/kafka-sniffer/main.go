package main

import (
	"flag"
	"github.com/bingoohuang/kafka-sniffer/stream/flowd"
	"log"
	"net/http"
	"os"
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
	pType      = flag.String("t", "", "req types filter, e.g. FetchRequest, ProduceRequest")
	iface      = flag.String("i", "eth0", "Interface to get packets from")
	bpf        = flag.String("bpf", "tcp and dst port 9092", "BPF expr")
	snaplen    = flag.Int("snap", 16<<10, "SnapLen for pcap packet capture")
	verbose    = flag.Bool("v", false, "Logs every packet in great detail")
	rwPrint    = flag.Bool("s", true, "Print the read and write clients")
	connTrack  = flag.Bool("flow", false, "Captures TCP/IP traffic and keeps conns track")
	listenAddr = flag.String("addr", ":9870", "Address on which sniffer listen the requests, e.g. :9870")
	expireTime = flag.Duration("metrics.expire-time", 5*time.Minute, "Expiration time of metric.")

	printJsonDuration = flag.Duration("p", 0, "Print the request json")

	clientStat *stream.ClientStat
)

func init() {
	flag.Parse()
	log.SetOutput(os.Stdout)
}

func main() {
	defer util.Run()()

	log.Printf("starting capture on interface %q", *iface)

	if *connTrack {
		handler, err := flowd.RunNetworkAnalyzer(*iface, *bpf, int32(*snaplen))
		if err != nil {
			log.Fatalf("failed to create network analyzer, err: %v", err)
		}
		log.Printf("start to captures TCP/IP traffic and keeps conns track, using bpf %q on device %q", *bpf, *iface)
		http.HandleFunc("/flow", handler)
	} else {
		go handlerKafka()
	}

	runTelemetry()
}

func handlerKafka() {
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
		xf := stream.NewKafkaClientPrintStreamFactory(*printJsonDuration, *pType)
		clientStat = xf.ClientStat
		f = xf
	} else {
		f = stream.NewKafkaStreamFactory(metricsStorage, *verbose)
	}

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
	packets := gopacket.NewPacketSource(handle, handle.LinkType()).Packets()
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
	log.Printf("serving metrics and api on %s\n", *listenAddr)

	http.Handle("/metrics", promhttp.Handler())
	if clientStat != nil {
		http.Handle("/client", stream.ServeClientStatHandler(clientStat))
	}

	if err := http.ListenAndServe(*listenAddr, nil); err != nil {
		panic(err)
	}
}
