package relay

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestAuthenticationUsesArrayPosition(t *testing.T) {
	keys := []string{"alice-secret", "bob-secret"}
	for _, tc := range []struct {
		header string
		want   int
	}{{"Bearer alice-secret", 0}, {"Bearer bob-secret", 1}, {"Bearer unknown", -1}, {"Basic alice-secret", -1}, {"Bearer bob-secret ", -1}} {
		if got := authenticate(tc.header, keys); got != tc.want {
			t.Errorf("authenticate(%q)=%d, want %d", tc.header, got, tc.want)
		}
	}
}

func TestRoundRobinSelection(t *testing.T) {
	queues := [][]*queuedRequest{{{id: "first"}, {id: "second"}}, nil, {{id: "third"}}}
	start := 0
	for _, want := range []string{"first", "third", "second"} {
		user, ok := nextQueued(queues, start)
		if !ok {
			t.Fatal("unexpected empty queues")
		}
		if got := queues[user][0].id; got != want {
			t.Fatalf("selected %q, want %q", got, want)
		}
		queues[user] = queues[user][1:]
		start = (user + 1) % len(queues)
	}
	if _, ok := nextQueued(queues, start); ok {
		t.Fatal("expected empty queues")
	}
}

func TestRetryDelayAndRefundEvidence(t *testing.T) {
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		value   string
		attempt int
		want    time.Duration
	}{{"12", 1, 12 * time.Second}, {now.Add(45 * time.Second).Format(http.TimeFormat), 1, 45 * time.Second}, {"bad", 1, 30 * time.Second}, {"", 2, time.Minute}, {"", 7, 2 * time.Minute}} {
		if got := retryDelay(tc.value, tc.attempt, now); got != tc.want {
			t.Errorf("retryDelay=%v, want %v", got, tc.want)
		}
	}
	for _, status := range []int{400, 401, 403, 404, 422, 429} {
		if !provenNotExecuted(status) {
			t.Errorf("status %d must refund", status)
		}
	}
	for _, status := range []int{200, 202, 301, 408, 409, 500, 502, 504} {
		if provenNotExecuted(status) {
			t.Errorf("status %d cannot prove no execution", status)
		}
	}
}

func TestRequestBodyPreservesBytesInMemory(t *testing.T) {
	input := "multipart\x00payload\r\nwith binary bytes\xff"
	for _, limit := range []int64{int64(len(input)), 1000} {
		body, err := spoolBody(strings.NewReader(input), limit, 1000, "")
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			reader, err := body.Reader()
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != input {
				t.Fatal("body bytes changed")
			}
		}
		body.Close()
	}
	if _, err := spoolBody(strings.NewReader(input), 1, 2, ""); !errors.Is(err, errBodyTooLarge) {
		t.Fatalf("expected size rejection, got %v", err)
	}
}

func TestDecodeSnapshotRequiresRealBalances(t *testing.T) {
	input := `{"active":true,"tier":3,"expiresAt":2000000000,"usage":{"percent":135.5,"purchasedPercent":25,"isNegative":true,"timeUntilNextPercent":30},"trainingStepsLeft":{"fixedTrainingStepsLeft":10,"purchasedTrainingSteps":20},"payment":{"secret":"hidden"}}`
	snap, err := decodeSnapshot(strings.NewReader(input), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Normal != 135500000 || snap.PurchasedOpus != 25000000 || snap.Fixed != 10000000 || !snap.UsageNegative {
		t.Fatalf("unexpected balances: %+v", snap)
	}
	if _, ok := snap.Raw["payment"]; ok {
		t.Fatal("payment metadata must be discarded")
	}
	for _, invalid := range []string{
		`{"active":true,"tier":3,"expiresAt":0,"trainingStepsLeft":{"fixedTrainingStepsLeft":0,"purchasedTrainingSteps":0}}`,
		`{"active":true,"tier":3,"expiresAt":0,"usage":{"percent":0,"purchasedPercent":0,"timeUntilNextPercent":30},"trainingStepsLeft":{"fixedTrainingStepsLeft":0,"purchasedTrainingSteps":0}}`,
		`{"active":true,"tier":1,"expiresAt":0,"trainingStepsLeft":{"fixedTrainingStepsLeft":0}}`,
		`{"active":null,"tier":1,"expiresAt":0,"trainingStepsLeft":{"fixedTrainingStepsLeft":0,"purchasedTrainingSteps":0}}`,
	} {
		if _, err := decodeSnapshot(strings.NewReader(invalid), time.Now()); err == nil {
			t.Errorf("accepted incomplete subscription: %s", invalid)
		}
	}
}

func TestPersonalSubscriptionAndQuotaExcludeGlobalData(t *testing.T) {
	view := QuotaView{User: 1, NormalCap: 50 * micro, FixedCap: 100 * micro, NormalRefillPerSecond: 1.0 / 20,
		Available: PoolBalances{Normal: 25 * micro, PurchasedOpus: 150 * micro, Fixed: 12500000, PurchasedAnlas: 7500000},
		Snapshot:  Snapshot{At: time.Now(), Active: true, Tier: 3, Normal: 90 * micro, PurchasedOpus: 400 * micro, Raw: map[string]json.RawMessage{"perks": json.RawMessage(`{"contextTokens":8192,"billingSecret":"hidden"}`), "payment": json.RawMessage(`"hidden"`)}}}
	personal := personalSubscription(view)
	usage := personal["usage"].(map[string]any)
	if usage["percent"] != int64(50) || usage["purchasedPercent"] != int64(300) || usage["timeUntilNextPercent"] != int64(10) {
		t.Fatalf("wrong personal normalized balances: %v", usage)
	}
	steps := personal["trainingStepsLeft"].(map[string]int64)
	if steps["fixedTrainingStepsLeft"] != 12 || steps["purchasedTrainingSteps"] != 7 {
		t.Fatalf("integer display must round down: %v", steps)
	}
	encoded, err := json.Marshal(personal)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "hidden") {
		t.Fatalf("personal response leaked metadata: %s", encoded)
	}
	quota := personalQuota(view, time.Minute)
	if _, ok := quota["snapshot"]; ok {
		t.Fatal("global snapshot exposed")
	}
	balances := quota["available"].(map[string]float64)
	if balances["fixed"] != 12.5 {
		t.Fatalf("quota must retain native fractional balance: %v", balances)
	}
}

func TestNormalizedPercentReturnsNativeIntegers(t *testing.T) {
	if got := normalizedPercent(12499999, 50*micro, 100); got != 24 {
		t.Fatalf("normalized balance=%d, want24", got)
	}
	if got := normalizedPercent(201*micro, 50*micro, 400); got != 400 {
		t.Fatalf("purchased cap=%d, want400", got)
	}
	if got := normalizedPercent(1, 0, 100); got != 0 {
		t.Fatalf("zero capacity=%d", got)
	}
}

func TestCopyHeadersRemovesConnectionExtensions(t *testing.T) {
	src := http.Header{"Content-Type": {"multipart/form-data; boundary=raw"}, "Accept": {"application/x-msgpack"}, "Connection": {"keep-alive, X-Private-Hop"}, "X-Private-Hop": {"secret"}, "Cache-Control": {"no-cache"}}
	dst := make(http.Header)
	copyHeaders(dst, src)
	if dst.Get("Connection") != "" || dst.Get("X-Private-Hop") != "" {
		t.Fatal("hop headers copied")
	}
	if dst.Get("Content-Type") != src.Get("Content-Type") || dst.Get("Accept") != src.Get("Accept") || dst.Get("Cache-Control") != "no-cache" {
		t.Fatal("end-to-end headers changed")
	}
}
