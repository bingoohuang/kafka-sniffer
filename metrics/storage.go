package metrics

import (
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "kafka_sniffer"
)

// Storage contains prometheus metrics that have expiration time. When expiration time is succeeded,
// metric with specific labels will be removed from storage. It is needed to keep only fresh producer,
// topic and consumer relations.
type Storage struct {
	registerer prometheus.Registerer

	producerTopicRelationInfo *metric
	consumerTopicRelationInfo *metric

	requestsReceivedTotal    prometheus.Counter
	requestDecodeTimeSeconds prometheus.Summary
	requestSizeBytes         prometheus.Summary
	connectionsTotal         prometheus.Gauge
}

func NewStorage(registerer prometheus.Registerer, expireTime time.Duration) *Storage {
	var s = &Storage{
		registerer: registerer,

		producerTopicRelationInfo: newMetric(prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "producer_topic_relation_info",
			Help:      "Relation information between producer and topic",
		}, []string{"producer", "topic"}), expireTime),
		consumerTopicRelationInfo: newMetric(prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "consumer_topic_relation_info",
			Help:      "Relation information between consumer and topic",
		}, []string{"consumer", "topic"}), expireTime),

		requestsReceivedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "requests_received_total",
			Help:      "Total received requests",
		}),
		requestDecodeTimeSeconds: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  namespace,
			Name:       "request_decode_time_seconds",
			Help:       "Spent time to decode request in seconds",
			Objectives: map[float64]float64{0.9: 0.01, 0.95: 0.005},
		}),
		requestSizeBytes: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  namespace,
			Name:       "request_size_bytes",
			Help:       "Request size in bytes",
			Objectives: map[float64]float64{0.9: 0.01, 0.95: 0.005},
		}),
		connectionsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "connections_count",
			Help:      "Connections count",
		}),
	}

	s.registerer.MustRegister(
		s.producerTopicRelationInfo.promMetric,
		s.consumerTopicRelationInfo.promMetric,
		s.requestsReceivedTotal,
		s.requestDecodeTimeSeconds,
		s.requestSizeBytes,
		s.connectionsTotal,
	)

	go s.producerTopicRelationInfo.runExpiration()
	go s.consumerTopicRelationInfo.runExpiration()

	return s
}

func (s *Storage) AddProducerTopicRelationInfo(producer, topic string) {
	s.producerTopicRelationInfo.update(producer, topic)
}

func (s *Storage) AddConsumerTopicRelationInfo(consumer, topic string) {
	s.consumerTopicRelationInfo.update(consumer, topic)
}

func (s *Storage) IncReceivedTotal() {
	s.requestsReceivedTotal.Inc()
}

func (s *Storage) ObserveRequestDecodeTimeSeconds(value float64) {
	s.requestDecodeTimeSeconds.Observe(value)
}

func (s *Storage) ObserverRequestSizeBytes(value float64) {
	s.requestSizeBytes.Observe(value)
}

func (s *Storage) IncConnectionsTotal() {
	s.connectionsTotal.Inc()
}

// metric contains expiration functionality
type metric struct {
	promMetric *prometheus.GaugeVec
	expireTime time.Duration

	expCh chan []string

	mux       sync.Mutex
	relations map[string]*relation
}

func newMetric(promMetric *prometheus.GaugeVec, expireTime time.Duration) *metric {
	return &metric{
		promMetric: promMetric,
		expireTime: expireTime,

		relations: make(map[string]*relation),
		expCh:     make(chan []string),
	}
}

// update updates relations or creates new one
func (m *metric) update(labels ...string) {
	m.promMetric.WithLabelValues(labels...).Set(float64(1))

	m.mux.Lock()
	if r, ok := m.relations[genLabelKey(labels...)]; ok {
		r.refresh()
	} else {
		m.relations[genLabelKey(labels...)] = newRelation(m.expireTime, labels, m.expCh)
	}
	m.mux.Unlock()
}

// runExpiration removes metric by specific label values and removes relation
func (m *metric) runExpiration() {
	for {
		select {
		case labels := <-m.expCh:
			m.promMetric.DeleteLabelValues(labels...)

			// remove relation
			m.mux.Lock()
			delete(m.relations, genLabelKey(labels...))
			m.mux.Unlock()
		}
	}
}

// relation contains metric labels and expiration time
type relation struct {
	expireTime time.Duration

	labels []string
	expCh  chan []string

	mux   sync.Mutex
	timer *time.Timer
}

func newRelation(expireTime time.Duration, labels []string, expCh chan []string) *relation {
	var rel = relation{
		expireTime: expireTime,
		labels:     labels,
		expCh:      expCh,
	}

	go rel.run()

	return &rel
}

// run runs expiration with specific timer
func (c *relation) run() {
	c.refresh()

	for {
		select {
		case <-c.timer.C:
			c.expCh <- c.labels
			return
		}
	}
}

// refresh resets timer or create new one
func (c *relation) refresh() {
	c.mux.Lock()
	if c.timer == nil {
		c.timer = time.NewTimer(c.expireTime)
	} else {
		c.timer.Reset(c.expireTime)
	}
	c.mux.Unlock()
}

func genLabelKey(labels ...string) string {
	return strings.Join(labels, "_")
}