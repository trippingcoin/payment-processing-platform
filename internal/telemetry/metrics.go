package telemetry

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"triple-p/internal/eventerror"
)

// Metrics owns a process-local registry. No default/global registrations.
type Metrics struct {
	Registry           *prometheus.Registry
	httpRequests       *prometheus.CounterVec
	httpDuration       *prometheus.HistogramVec
	payments           *prometheus.CounterVec
	attempts           *prometheus.CounterVec
	attemptDuration    *prometheus.HistogramVec
	retries            *prometheus.CounterVec
	dlq                prometheus.Counter
	outboxAttempts     *prometheus.CounterVec
	outboxDuration     prometheus.Histogram
	outboxTransactions *prometheus.CounterVec
}

func New() *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry()}
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ppp_" + name, Help: help}, labels)
		m.Registry.MustRegister(c)
		return c
	}
	histogram := func(name, help string, labels ...string) *prometheus.HistogramVec {
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "ppp_" + name, Help: help, Buckets: prometheus.DefBuckets}, labels)
		m.Registry.MustRegister(h)
		return h
	}
	m.httpRequests = counter("http_requests_total", "Completed application HTTP requests.", "method", "route", "status")
	m.httpDuration = histogram("http_request_duration_seconds", "Application HTTP request duration.", "method", "route")
	m.payments = counter("payment_transitions_total", "Payment status transitions observed after confirmed commit.", "status")
	m.attempts = counter("consumer_attempts_total", "Consumer stage attempts, including failed attempts.", "stage", "result")
	m.attemptDuration = histogram("consumer_attempt_duration_seconds", "Consumer stage duration; fetch includes idle time.", "stage")
	m.retries = counter("consumer_retries_total", "Additional consumer attempts actually started.", "stage")
	m.dlq = prometheus.NewCounter(prometheus.CounterOpts{Name: "ppp_consumer_dlq_published_total", Help: "Acknowledged DLQ publications, including duplicates."})
	m.outboxAttempts = counter("outbox_publish_attempts_total", "Outbox broker publication attempts.", "result")
	m.outboxDuration = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "ppp_outbox_publish_duration_seconds", Help: "Outbox broker publication duration.", Buckets: prometheus.DefBuckets})
	m.outboxTransactions = counter("outbox_transactions_total", "Outbox transaction outcomes; empty polls excluded.", "result")
	m.Registry.MustRegister(m.dlq, m.outboxDuration, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2}))
	return mux
}

func (m *Metrics) PaymentTransition(status string) {
	if m == nil {
		return
	}
	switch status {
	case "pending", "processing", "completed", "failed":
		m.payments.WithLabelValues(status).Inc()
	}
}

func result(err error) string {
	if err == nil {
		return "success"
	}
	switch kind := eventerror.Classify(err).Kind; kind {
	case eventerror.Transient, eventerror.Permanent, eventerror.Canceled:
		return string(kind)
	default:
		return "unknown"
	}
}

func (m *Metrics) ConsumerAttempt(stage string, err error, duration time.Duration, retry bool) {
	if m == nil {
		return
	}
	switch stage {
	case "fetch", "handle", "dlq", "commit":
	default:
		stage = "unknown"
	}
	m.attempts.WithLabelValues(stage, result(err)).Inc()
	m.attemptDuration.WithLabelValues(stage).Observe(duration.Seconds())
	if retry {
		m.retries.WithLabelValues(stage).Inc()
	}
	if stage == "dlq" && err == nil {
		m.dlq.Inc()
	}
}

func (m *Metrics) OutboxPublish(err error, duration time.Duration) {
	if m == nil {
		return
	}
	m.outboxAttempts.WithLabelValues(result(err)).Inc()
	m.outboxDuration.Observe(duration.Seconds())
}
func (m *Metrics) OutboxTransaction(outcome string) {
	if m == nil {
		return
	}
	switch outcome {
	case "published", "retry_scheduled", "error":
		m.outboxTransactions.WithLabelValues(outcome).Inc()
	}
}

// TrackHTTP is called after any request cloning and before recovery is deferred.
// The returned function runs after recovery, observing the actual written status.
func (m *Metrics) TrackHTTP(w *http.ResponseWriter, r *http.Request, routes map[string]struct{}) func() {
	if m == nil {
		return func() {}
	}
	started := time.Now()
	recorder := &responseWriter{ResponseWriter: *w, status: 200}
	*w = recorder
	return func() {
		route := r.Pattern
		if _, known := routes[route]; !known {
			route = "unmatched"
		}
		method := r.Method
		switch method {
		case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE":
		default:
			method = "OTHER"
		}
		m.httpRequests.WithLabelValues(method, route, strconv.Itoa(recorder.status)).Inc()
		m.httpDuration.WithLabelValues(method, route).Observe(time.Since(started).Seconds())
	}
}

type responseWriter struct {
	http.ResponseWriter
	status  int
	written bool
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *responseWriter) WriteHeader(status int) {
	if w.written {
		return
	}
	if status >= 200 {
		w.status = status
		w.written = true
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *responseWriter) Write(b []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

type OutboxStats func(context.Context) (pending int64, oldestAgeSeconds float64, err error)

func (m *Metrics) RegisterOutbox(stats OutboxStats) {
	m.Registry.MustRegister(&outboxCollector{stats: stats,
		pending: prometheus.NewDesc("ppp_outbox_pending_events", "All unpublished Outbox records, including delayed retries.", nil, nil),
		age:     prometheus.NewDesc("ppp_outbox_oldest_pending_age_seconds", "Age of oldest unpublished Outbox record; zero when empty.", nil, nil),
		up:      prometheus.NewDesc("ppp_outbox_scrape_success", "Whether the Outbox database snapshot succeeded.", nil, nil)})
}

type outboxCollector struct {
	stats            OutboxStats
	pending, age, up *prometheus.Desc
}

func (c *outboxCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.pending
	ch <- c.age
	ch <- c.up
}
func (c *outboxCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pending, age, err := c.stats(ctx)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(c.pending, prometheus.GaugeValue, float64(pending))
	ch <- prometheus.MustNewConstMetric(c.age, prometheus.GaugeValue, max(0, age))
}
