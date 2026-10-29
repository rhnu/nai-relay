package relay

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const micro = int64(1_000_000)

var errSubscriptionUnavailable = errors.New("upstream subscription is unavailable")

type queuedRequest struct {
	user   int
	id     string
	r      *http.Request
	w      http.ResponseWriter
	body   *requestBody
	info   RequestInfo
	queued time.Time
	done   chan struct{}
}

// relayServer serializes ledger changes and dispatches finite, fair batches.
type relayServer struct {
	cfg        Config
	client     *http.Client
	ledger     *Ledger
	store      *Store
	mu         sync.Mutex
	fatal      error
	ready      bool
	incoming   chan *queuedRequest
	completed  chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	workerDone chan struct{}
	lastStart  time.Time
	cooldown   time.Time
	throttles  int
}

// Run opens durable state, obtains a real subscription snapshot, and serves until shutdown.
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	store, err := OpenStore(cfg.Database, len(cfg.Keys), cfg.Reserve, cfg.DeficitPolicy)
	if err != nil {
		return fmt.Errorf("open ledger: %w", err)
	}
	defer store.Close()
	ledger, err := store.Load()
	if err != nil {
		return fmt.Errorf("load ledger: %w", err)
	}
	ledger.FreshWindow = cfg.SnapshotMaxAge
	if err = ledger.RecoverPending(); err != nil {
		return fmt.Errorf("recover ledger: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	defer transport.CloseIdleConnections()
	s := &relayServer{cfg: cfg, ledger: ledger, store: store, ctx: runCtx, cancel: cancel,
		client:   &http.Client{Transport: transport, Timeout: cfg.RequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		incoming: make(chan *queuedRequest), completed: make(chan struct{}, cfg.Concurrency), workerDone: make(chan struct{})}
	if err = s.refresh(); err != nil {
		return fmt.Errorf("initial subscription: %w", err)
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 1 << 20}
	go s.schedule()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	slog.Info("relay listening", "address", listener.Addr().String(), "users", len(cfg.Keys), "concurrency", cfg.Concurrency)
	select {
	case <-ctx.Done():
	case err = <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			cancel()
		}
	case <-runCtx.Done():
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
		_ = server.Close()
	}
	select {
	case <-s.workerDone:
	case <-shutdownCtx.Done():
		return fmt.Errorf("shutdown: %w", shutdownCtx.Err())
	}
	s.mu.Lock()
	fatal := s.fatal
	s.mu.Unlock()
	if fatal != nil {
		return fatal
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *relayServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		if r.Method != http.MethodGet {
			reject(w, 405, "method not allowed")
			return
		}
		writeJSON(w, 200, map[string]bool{"ok": true})
		return
	}
	if r.URL.Path == "/readyz" {
		if r.Method != http.MethodGet {
			reject(w, 405, "method not allowed")
			return
		}
		s.mu.Lock()
		snap := s.ledger.Current()
		ready := s.ready && s.fatal == nil && time.Since(snap.At) <= s.cfg.SnapshotMaxAge && snap.Active && (snap.ExpiresAt <= 0 || time.Now().Before(time.Unix(snap.ExpiresAt, 0)))
		s.mu.Unlock()
		if !ready {
			reject(w, 503, "upstream subscription is unavailable")
			return
		}
		writeJSON(w, 200, map[string]bool{"ready": true})
		return
	}
	user := authenticate(r.Header.Get("Authorization"), s.cfg.Keys)
	if user < 0 {
		w.Header().Set("WWW-Authenticate", "Bearer")
		reject(w, 401, "invalid relay credential")
		return
	}
	if r.URL.Path == "/quota" || r.URL.Path == "/user/subscription" {
		if r.Method != http.MethodGet {
			reject(w, 405, "method not allowed")
			return
		}
		s.mu.Lock()
		if s.fatal != nil {
			s.mu.Unlock()
			reject(w, 503, "ledger unavailable")
			return
		}
		if s.ready && time.Since(s.ledger.Current().At) <= s.cfg.SnapshotMaxAge {
			if err := s.ledger.Advance(time.Now()); err != nil {
				s.mu.Unlock()
				reject(w, 503, "cannot update quota")
				return
			}
			if err := s.persistLocked(); err != nil {
				s.mu.Unlock()
				reject(w, 503, "ledger unavailable")
				return
			}
		}
		view := s.ledger.View(user)
		ready := s.ready
		s.mu.Unlock()
		if !ready || time.Since(view.Snapshot.At) > s.cfg.SnapshotMaxAge {
			view.NormalRefillPerSecond = 0
			view.Spendable = PoolSpendability{}
		}
		if r.URL.Path == "/quota" {
			quota := personalQuota(view, s.cfg.SnapshotMaxAge)
			quota["snapshot_fresh"] = ready && time.Since(view.Snapshot.At) <= s.cfg.SnapshotMaxAge
			if !ready {
				quota["status"] = "upstream_unavailable"
			}
			writeJSON(w, 200, quota)
		} else {
			writeJSON(w, 200, personalSubscription(view))
		}
		return
	}
	if !allowedImagePath(r.URL.Path) {
		reject(w, 404, "not found")
		return
	}
	tags := r.URL.Path == "/ai/generate-image/suggest-tags"
	if (!tags && r.Method != http.MethodPost) || (tags && r.Method != http.MethodGet) {
		reject(w, 405, "method not allowed")
		return
	}
	body, err := spoolBody(r.Body, s.cfg.MemoryBodyBytes, s.cfg.MaxBodyBytes, s.cfg.TempDir)
	if err != nil {
		code := 400
		if errors.Is(err, errBodyTooLarge) {
			code = 413
		}
		reject(w, code, "cannot read request body")
		return
	}
	defer body.Close()
	reader, err := body.Reader()
	if err != nil {
		reject(w, 500, "cannot read request body")
		return
	}
	info := RequestInfo{}
	if !tags {
		info, err = ExtractRequest(r.Header.Get("Content-Type"), reader)
		if err != nil {
			reject(w, 400, err.Error())
			return
		}
	}
	if r.URL.Path == "/ai/generate-image-stream" && info.StreamMode == "" {
		info.StreamMode = "sse"
	}
	id, err := requestID()
	if err != nil {
		reject(w, 500, "cannot create request identifier")
		return
	}
	job := &queuedRequest{user: user, id: id, r: r, w: w, body: body, info: info, queued: time.Now(), done: make(chan struct{})}
	select {
	case s.incoming <- job:
	case <-r.Context().Done():
		return
	case <-s.ctx.Done():
		reject(w, 503, "relay is shutting down")
		return
	}
	// The scheduler owns the writer until completion, including cancellation cleanup.
	<-job.done
}

func authenticate(header string, keys []string) int {
	if !strings.HasPrefix(header, "Bearer ") {
		return -1
	}
	token := strings.TrimPrefix(header, "Bearer ")
	matched := -1
	for i, key := range keys {
		if subtle.ConstantTimeCompare([]byte(token), []byte(key)) == 1 {
			matched = i
		}
	}
	return matched
}

func allowedImagePath(path string) bool {
	switch path {
	case "/ai/generate-image", "/ai/generate-image-stream", "/ai/augment-image", "/ai/encode-vibe", "/ai/upscale", "/ai/generate-image/suggest-tags":
		return true
	}
	return false
}

func (s *relayServer) schedule() {
	defer close(s.workerDone)
	queues := make([][]*queuedRequest, len(s.cfg.Keys))
	next := 0
	active := 0
	pending := make([]*queuedRequest, 0, s.cfg.Concurrency)
	lastSync := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	finish := func(j *queuedRequest, status int, msg string) {
		if j.r.Context().Err() == nil {
			reject(j.w, status, msg)
		}
		close(j.done)
	}
	for {
		// Drop cancelled and expired requests before quoting or reserving funds.
		now := time.Now()
		for user := range queues {
			remaining := queues[user][:0]
			for _, j := range queues[user] {
				if j.r.Context().Err() != nil {
					close(j.done)
				} else if now.Sub(j.queued) >= s.cfg.QueueTimeout {
					finish(j, 504, "queue wait timed out")
				} else {
					remaining = append(remaining, j)
				}
			}
			queues[user] = remaining
		}
		remaining := pending[:0]
		for _, j := range pending {
			if j.r.Context().Err() != nil {
				close(j.done)
			} else if now.Sub(j.queued) >= s.cfg.QueueTimeout {
				finish(j, 504, "queue wait timed out")
			} else {
				remaining = append(remaining, j)
			}
		}
		pending = remaining
		if s.ctx.Err() != nil {
			for _, queue := range queues {
				for _, j := range queue {
					finish(j, 503, "relay is shutting down")
				}
			}
			for _, j := range pending {
				finish(j, 503, "relay is shutting down")
			}
			pending = nil
			for active > 0 {
				<-s.completed
				active--
			}
			return
		}
		s.mu.Lock()
		cooldown := s.cooldown
		s.mu.Unlock()
		if active == 0 && len(pending) == 0 {
			s.mu.Lock()
			fatal := s.fatal
			fresh := now.Sub(s.ledger.Current().At) <= s.cfg.SnapshotMaxAge && s.ready
			s.mu.Unlock()
			if fatal != nil {
				s.cancel()
				continue
			}
			queued := false
			for _, queue := range queues {
				queued = queued || len(queue) > 0
			}
			retryInterval := min(s.cfg.PollInterval, 5*time.Second)
			if ((!fresh && now.Sub(lastSync) >= retryInterval) || now.Sub(lastSync) >= s.cfg.PollInterval) && !now.Before(cooldown) {
				if err := s.refresh(); err != nil {
					slog.Warn("subscription refresh failed", "error", err)
				}
				lastSync = now
				s.mu.Lock()
				fresh = time.Since(s.ledger.Current().At) <= s.cfg.SnapshotMaxAge && s.ready
				s.mu.Unlock()
			}
			if queued && fresh && !now.Before(cooldown) {
				for len(pending) < s.cfg.Concurrency {
					user, ok := nextQueued(queues, next)
					if !ok {
						break
					}
					j := queues[user][0]
					queues[user] = queues[user][1:]
					next = (user + 1) % len(queues)
					pending = append(pending, j)
				}
			}
		}
		if len(pending) > 0 && !now.Before(s.lastStart.Add(s.cfg.StartInterval)) && !now.Before(cooldown) {
			j := pending[0]
			pending = pending[1:]
			if j.r.Context().Err() != nil {
				close(j.done)
				continue
			}
			if now.Sub(j.queued) >= s.cfg.QueueTimeout {
				finish(j, 504, "queue wait timed out")
				continue
			}
			if err := s.prepare(j); err != nil {
				status := 400
				if errors.Is(err, ErrInsufficientFunds) || errors.Is(err, ErrFrozen) {
					status = 402
				}
				if errors.Is(err, errSubscriptionUnavailable) {
					status = 503
				}
				s.mu.Lock()
				if s.fatal != nil || time.Since(s.ledger.Current().At) > s.cfg.SnapshotMaxAge {
					status = 503
				}
				s.mu.Unlock()
				finish(j, status, err.Error())
				continue
			}
			s.lastStart = time.Now()
			active++
			go s.forward(j)
			if len(pending) > 0 && s.cfg.StartInterval == 0 {
				continue
			}
		}
		select {
		case j := <-s.incoming:
			count := len(queues[j.user])
			for _, p := range pending {
				if p.user == j.user {
					count++
				}
			}
			if count >= s.cfg.QueuePerUser {
				j.w.Header().Set("Retry-After", "1")
				finish(j, 429, "user queue is full")
			} else {
				queues[j.user] = append(queues[j.user], j)
			}
		case <-s.completed:
			active--
			if active == 0 && len(pending) == 0 {
				if err := s.refresh(); err != nil {
					slog.Warn("post-batch subscription refresh failed", "error", err)
				}
				lastSync = time.Now()
			}
		case <-ticker.C:
		case <-s.ctx.Done():
		}
	}
}

func nextQueued(queues [][]*queuedRequest, next int) (int, bool) {
	for i := 0; i < len(queues); i++ {
		user := (next + i) % len(queues)
		if len(queues[user]) > 0 {
			return user, true
		}
	}
	return 0, false
}

func (s *relayServer) prepare(j *queuedRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fatal != nil {
		return errors.New("ledger unavailable")
	}
	if !s.ready {
		return errSubscriptionUnavailable
	}
	if time.Since(s.ledger.Current().At) > s.cfg.SnapshotMaxAge {
		return errors.New("upstream subscription is stale")
	}
	if j.r.URL.Path == "/ai/generate-image/suggest-tags" {
		return nil
	}
	snap := s.ledger.Current()
	if !snap.Active || snap.ExpiresAt > 0 && !time.Now().Before(time.Unix(snap.ExpiresAt, 0)) {
		return errSubscriptionUnavailable
	}
	if err := s.ledger.Advance(time.Now()); err != nil {
		return err
	}
	q := Quote{}
	var err error
	if j.r.URL.Path != "/ai/generate-image/suggest-tags" {
		q, err = Price(j.r.URL.Path, j.info, s.ledger.Current(), s.cfg.UsageMultiplier)
	}
	if err != nil {
		return err
	}
	if err = s.ledger.Reserve(j.user, q, j.id); err != nil {
		return err
	}
	if err = s.persistLocked(); err != nil {
		return err
	}
	if err = s.ledger.MarkSent(j.id); err != nil {
		return err
	}
	if err = s.persistLocked(); err != nil {
		return err
	}
	slog.Info("request reserved", "user", j.user, "request", j.id, "path", j.r.URL.Path, "queue_wait", time.Since(j.queued), "opus", float64(q.Opus)/float64(micro), "anlas", float64(q.Anlas)/float64(micro))
	return nil
}

func (s *relayServer) persistLocked() error {
	if err := s.store.Save(s.ledger); err != nil {
		s.fatal = fmt.Errorf("persist ledger: %w", err)
		s.ready = false
		slog.Error("ledger persistence failed; dispatch stopped", "error", err)
		return s.fatal
	}
	return nil
}

func (s *relayServer) forward(j *queuedRequest) {
	started := time.Now()
	defer func() { close(j.done); s.completed <- struct{}{} }()
	ctx, cancel := context.WithTimeout(s.ctx, s.cfg.RequestTimeout)
	defer cancel()
	// A caller disconnect must stop transfer, but never refunds an already sent attempt.
	stop := context.AfterFunc(j.r.Context(), cancel)
	defer stop()
	reader, err := j.body.Reader()
	if err != nil {
		s.settle(j, "refunded")
		reject(j.w, 500, "cannot read request body")
		return
	}
	base, err := url.Parse(s.cfg.UpstreamURL)
	if err != nil {
		s.settle(j, "refunded")
		reject(j.w, 500, "invalid upstream URL")
		return
	}
	base.Path = strings.TrimSuffix(base.Path, "/") + j.r.URL.Path
	base.RawQuery = j.r.URL.RawQuery
	req, err := http.NewRequestWithContext(ctx, j.r.Method, base.String(), reader)
	if err != nil {
		s.settle(j, "refunded")
		reject(j.w, 500, "cannot create upstream request")
		return
	}
	copyHeaders(req.Header, j.r.Header)
	req.Header.Del("Cookie")
	req.Header.Del("Proxy-Authorization")
	req.Header.Set("Authorization", "Bearer "+s.cfg.UpstreamToken)
	req.ContentLength = j.body.size
	// NewRequest installs GetBody for in-memory readers. Clearing it prevents
	// transport replay of a billed POST even when callers send idempotency headers.
	req.GetBody = nil
	resp, err := s.client.Do(req)
	if err != nil {
		s.settle(j, "unknown")
		if j.r.Context().Err() == nil {
			reject(j.w, 502, "upstream request outcome is unknown")
		}
		slog.Warn("upstream transport failed", "user", j.user, "request", j.id, "duration", time.Since(started), "error", safeTransportError(err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 429 {
		s.mu.Lock()
		s.throttles++
		s.cooldown = time.Now().Add(retryDelay(resp.Header.Get("Retry-After"), s.throttles, time.Now()))
		s.mu.Unlock()
	} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		s.mu.Lock()
		s.throttles = 0
		s.mu.Unlock()
	}
	copyHeaders(j.w.Header(), resp.Header)
	// Upstream credentials and cookies never become part of a downstream response.
	j.w.Header().Del("Set-Cookie")
	j.w.Header().Del("Authorization")
	j.w.Header().Del("Proxy-Authenticate")
	j.w.WriteHeader(resp.StatusCode)
	out := &flushWriter{w: j.w}
	if j.r.URL.Path == "/ai/generate-image-stream" && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		err = ObserveStream(io.TeeReader(resp.Body, out), j.info.StreamMode, j.info.Samples)
		if err != nil {
			// Inspection may reject future protocol extensions. Bytes already read
			// were teed verbatim; continue transferring the unread response.
			_, copyErr := io.Copy(out, resp.Body)
			if copyErr != nil {
				err = errors.Join(err, copyErr)
			}
		}
	} else {
		_, err = io.Copy(out, resp.Body)
	}
	outcome := "unknown"
	if provenNotExecuted(resp.StatusCode) {
		outcome = "refunded"
	} else if resp.StatusCode >= 200 && resp.StatusCode < 300 && err == nil {
		outcome = "success"
	}
	s.settle(j, outcome)
	slog.Info("upstream request completed", "user", j.user, "request", j.id, "status", resp.StatusCode, "outcome", outcome, "duration", time.Since(started))
	if err != nil {
		slog.Warn("upstream response incomplete; reservation retained unless execution was rejected", "user", j.user, "request", j.id, "status", resp.StatusCode, "error", err)
	}
}

func (s *relayServer) settle(j *queuedRequest, outcome string) {
	if j.r.URL.Path == "/ai/generate-image/suggest-tags" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ledger.SettleOutcome(j.id, outcome); err != nil {
		s.fatal = fmt.Errorf("settle ledger: %w", err)
		s.ready = false
		slog.Error("ledger settlement failed", "request", j.id, "error", err)
		return
	}
	_ = s.persistLocked()
}

func provenNotExecuted(status int) bool {
	switch status {
	case 400, 401, 403, 404, 422, 429:
		return true
	}
	return false
}

func retryDelay(value string, attempt int, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil && seconds >= 0 {
		if seconds > 86400 {
			seconds = 86400
		}
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	if attempt <= 1 {
		return 30 * time.Second
	}
	if attempt == 2 {
		return 60 * time.Second
	}
	return 120 * time.Second
}

type flushWriter struct{ w http.ResponseWriter }

func (w *flushWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if f, ok := w.w.(http.Flusher); ok {
		f.Flush()
	}
	return n, err
}

func copyHeaders(dst, src http.Header) {
	blocked := map[string]bool{"Connection": true, "Proxy-Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true, "Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Host": true}
	for _, value := range src.Values("Connection") {
		for _, name := range strings.Split(value, ",") {
			blocked[http.CanonicalHeaderKey(strings.TrimSpace(name))] = true
		}
	}
	for name, values := range src {
		if !blocked[http.CanonicalHeaderKey(name)] {
			dst[name] = append([]string(nil), values...)
		}
	}
}

func requestID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}
func reject(w http.ResponseWriter, status int, message string) {
	codes := map[int]string{400: "invalid_request", 401: "invalid_key", 402: "quota_unavailable", 404: "not_found", 405: "method_not_allowed", 413: "body_too_large", 422: "price_unavailable", 429: "rate_limited", 500: "internal_error", 502: "upstream_error", 503: "service_unavailable", 504: "queue_timeout"}
	code := codes[status]
	if code == "" {
		code = "request_failed"
	}
	writeJSON(w, status, map[string]string{"error": message, "code": code})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (s *relayServer) refresh() (err error) {
	defer func() {
		if err != nil {
			s.mu.Lock()
			s.ready = false
			s.mu.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(s.ctx, s.cfg.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.SubscriptionURL, nil)
	if err != nil {
		return safeTransportError(err)
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.UpstreamToken)
	req.Header.Set("Accept", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return safeTransportError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("subscription returned HTTP %d", resp.StatusCode)
	}
	snapshot, err := decodeSnapshot(io.LimitReader(resp.Body, 2<<20), time.Now())
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fatal != nil {
		return s.fatal
	}
	if s.ledger.View(0).Initialized {
		err = s.ledger.Sync(snapshot)
	} else {
		err = s.ledger.Initialize(snapshot)
	}
	if err != nil {
		return err
	}
	if err = s.persistLocked(); err != nil {
		return err
	}
	s.ready = true
	slog.Debug("quota snapshot synchronized", "normal", float64(snapshot.Normal)/float64(micro), "purchased_opus", float64(snapshot.PurchasedOpus)/float64(micro), "fixed_anlas", float64(snapshot.Fixed)/float64(micro), "purchased_anlas", float64(snapshot.PurchasedAnlas)/float64(micro), "frozen", s.ledger.Frozen)
	return nil
}

func decodeSnapshot(r io.Reader, at time.Time) (Snapshot, error) {
	var raw map[string]json.RawMessage
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&raw); err != nil {
		return Snapshot{}, fmt.Errorf("decode subscription: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Snapshot{}, errors.New("subscription has trailing data")
	}
	var wire struct {
		Active    *bool  `json:"active"`
		Tier      *int   `json:"tier"`
		ExpiresAt *int64 `json:"expiresAt"`
		Usage     *struct {
			Percent              *float64 `json:"percent"`
			PurchasedPercent     *float64 `json:"purchasedPercent"`
			IsNegative           *bool    `json:"isNegative"`
			TimeUntilNextPercent *int64   `json:"timeUntilNextPercent"`
		} `json:"usage"`
		Training *struct {
			Fixed     *int64 `json:"fixedTrainingStepsLeft"`
			Purchased *int64 `json:"purchasedTrainingSteps"`
		} `json:"trainingStepsLeft"`
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return Snapshot{}, err
	}
	if err = json.Unmarshal(encoded, &wire); err != nil {
		return Snapshot{}, err
	}
	if wire.Active == nil || wire.Tier == nil || wire.ExpiresAt == nil {
		return Snapshot{}, errors.New("subscription missing active, tier, or expiresAt")
	}
	if wire.Training == nil || wire.Training.Fixed == nil || wire.Training.Purchased == nil {
		return Snapshot{}, errors.New("subscription missing Anlas balance fields")
	}
	if *wire.Active && *wire.Tier == 3 && wire.Usage == nil {
		return Snapshot{}, errors.New("active Opus subscription missing usage")
	}
	snap := Snapshot{At: at, Active: *wire.Active, Tier: *wire.Tier, ExpiresAt: *wire.ExpiresAt, Raw: make(map[string]json.RawMessage)}
	if *wire.Training.Fixed < 0 || *wire.Training.Purchased < 0 || *wire.Training.Fixed > math.MaxInt64/micro || *wire.Training.Purchased > math.MaxInt64/micro {
		return Snapshot{}, errors.New("invalid upstream Anlas balance")
	}
	snap.Fixed = *wire.Training.Fixed * micro
	snap.PurchasedAnlas = *wire.Training.Purchased * micro
	if wire.Usage != nil {
		usage := wire.Usage
		if usage.Percent == nil || usage.PurchasedPercent == nil || usage.IsNegative == nil || usage.TimeUntilNextPercent == nil {
			return Snapshot{}, errors.New("subscription missing Opus usage fields")
		}
		if math.IsNaN(*usage.Percent) || math.IsInf(*usage.Percent, 0) || *usage.Percent < 0 || *usage.Percent > float64(math.MaxInt64/micro) || *usage.PurchasedPercent < 0 || *usage.PurchasedPercent > 400 || *usage.TimeUntilNextPercent < 0 {
			return Snapshot{}, errors.New("invalid upstream Opus usage")
		}
		snap.Normal = int64(math.Round(*usage.Percent * float64(micro)))
		snap.UsageNegative = *usage.IsNegative
		snap.PurchasedOpus = int64(math.Round(*usage.PurchasedPercent * float64(micro)))
		snap.SecondsPerPercent = *usage.TimeUntilNextPercent
	}
	for _, name := range []string{"active", "tier", "expiresAt", "perks", "trainingStepsLeft", "usage"} {
		if value, ok := raw[name]; ok {
			snap.Raw[name] = value
		}
	}
	return snap, nil
}

func personalSubscription(view QuotaView) map[string]any {
	snap := view.Snapshot
	result := map[string]any{"active": snap.Active, "tier": snap.Tier, "expiresAt": snap.ExpiresAt}
	if raw, ok := snap.Raw["perks"]; ok {
		var perks map[string]json.RawMessage
		if json.Unmarshal(raw, &perks) == nil {
			safe := make(map[string]json.RawMessage)
			for _, key := range []string{"contextTokens", "maxPriorityActions", "moduleTrainingSteps", "startPriority", "unlimitedMaxPriority"} {
				if value, ok := perks[key]; ok {
					safe[key] = value
				}
			}
			result["perks"] = safe
		}
	}
	normal := normalizedPercent(view.Available.Normal, view.NormalCap, 100)
	purchased := normalizedPercent(view.Available.PurchasedOpus, view.NormalCap, 400)
	if purchased > 400 {
		purchased = 400
	}
	seconds := int64(0)
	if view.NormalRefillPerSecond > 0 && view.NormalCap > 0 {
		seconds = int64(math.Ceil(float64(view.NormalCap) / float64(micro) / 100 / view.NormalRefillPerSecond))
	}
	result["usage"] = map[string]any{"percent": normal, "purchasedPercent": purchased, "isNegative": view.Frozen || snap.UsageNegative || (!view.Spendable.Normal && !view.Spendable.PurchasedOpus), "timeUntilNextPercent": seconds}
	result["trainingStepsLeft"] = map[string]int64{"fixedTrainingStepsLeft": view.Available.Fixed / micro, "purchasedTrainingSteps": view.Available.PurchasedAnlas / micro}
	return result
}

func normalizedPercent(balance, cap int64, maximum float64) int64 {
	if cap <= 0 {
		return 0
	}
	return int64(math.Floor(math.Min(maximum, math.Max(0, float64(balance)/float64(cap)*100))))
}

func safeTransportError(err error) error {
	var wrapped *url.Error
	if errors.As(err, &wrapped) {
		return wrapped.Err
	}
	return err
}

// personalQuota excludes the global snapshot and reports native units, including fractional Anlas.
func personalQuota(view QuotaView, maxAge time.Duration) map[string]any {
	balances := func(p PoolBalances) map[string]float64 {
		return map[string]float64{"normal": float64(p.Normal) / float64(micro), "purchased_opus": float64(p.PurchasedOpus) / float64(micro), "fixed": float64(p.Fixed) / float64(micro), "purchased_anlas": float64(p.PurchasedAnlas) / float64(micro)}
	}
	fresh := time.Since(view.Snapshot.At) <= maxAge
	status := "ready"
	if !fresh {
		status = "snapshot_stale"
	} else if view.Frozen {
		status = "frozen"
	}
	return map[string]any{"user": view.User, "available": balances(view.Available), "reserved": balances(view.Reserved), "spendable": view.Spendable, "normal_cap": float64(view.NormalCap) / float64(micro), "fixed_cap": float64(view.FixedCap) / float64(micro), "frozen": view.Frozen, "snapshot_at": view.Snapshot.At, "snapshot_fresh": fresh, "refill_at": view.RefillAt, "normal_refill_per_second": view.NormalRefillPerSecond, "estimated": true, "status": status}
}
