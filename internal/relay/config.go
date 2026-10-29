package relay

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

// Config contains startup-only settings. Keys identify wallets by array position.
type Config struct {
	Keys            []string      `toml:"keys"`
	UpstreamToken   string        `toml:"upstream_token"`
	UpstreamURL     string        `toml:"upstream_url"`
	SubscriptionURL string        `toml:"subscription_url"`
	Listen          string        `toml:"listen"`
	Database        string        `toml:"database"`
	Reserve         float64       `toml:"reserve"`
	DeficitPolicy   string        `toml:"deficit_policy"`
	Concurrency     int           `toml:"concurrency"`
	StartInterval   time.Duration `toml:"start_interval"`
	QueuePerUser    int           `toml:"queue_per_user"`
	QueueTimeout    time.Duration `toml:"queue_timeout"`
	RequestTimeout  time.Duration `toml:"request_timeout"`
	SnapshotMaxAge  time.Duration `toml:"snapshot_max_age"`
	PollInterval    time.Duration `toml:"poll_interval"`
	MemoryBodyBytes int64         `toml:"memory_body_bytes"`
	MaxBodyBytes    int64         `toml:"max_body_bytes"`
	TempDir         string        `toml:"temp_dir"`
	UsageMultiplier float64       `toml:"usage_multiplier"`
}

func DefaultConfig() Config {
	return Config{UpstreamURL: "https://image.novelai.net", SubscriptionURL: "https://api.novelai.net/user/subscription", Listen: "0.0.0.0:8080", Database: "data/nai-relay.db", Reserve: 1, DeficitPolicy: "proportional", Concurrency: 1, StartInterval: 10 * time.Second, QueuePerUser: 4, QueueTimeout: 5 * time.Minute, RequestTimeout: 5 * time.Minute, SnapshotMaxAge: 120 * time.Second, PollInterval: 60 * time.Second, MemoryBodyBytes: 8 << 20, MaxBodyBytes: 256 << 20, UsageMultiplier: 1}
}

// ParseConfig applies file fields then environment overrides without merging key arrays.
func ParseConfig(data []byte, lookup func(string) (string, bool)) (Config, error) {
	cfg := DefaultConfig()
	fields := map[string]any{}
	if len(data) > 0 {
		if err := toml.Unmarshal(data, &fields); err != nil {
			return cfg, fmt.Errorf("parse config: %w", err)
		}
	}
	value := reflect.ValueOf(&cfg).Elem()
	typ := value.Type()
	known := map[string]bool{}
	for i := 0; i < value.NumField(); i++ {
		name := typ.Field(i).Tag.Get("toml")
		known[name] = true
		raw, ok := fields[name]
		if env, present := lookup("NAI_RELAY_" + strings.ToUpper(name)); present {
			raw = env
			ok = true
		}
		if !ok {
			continue
		}
		field := value.Field(i)
		var err error
		switch {
		case field.Type() == reflect.TypeOf(time.Duration(0)):
			var duration time.Duration
			duration, err = time.ParseDuration(fmt.Sprint(raw))
			if err == nil {
				field.SetInt(int64(duration))
			}
		case field.Kind() == reflect.Slice:
			var keys []string
			if s, yes := raw.(string); yes {
				err = json.Unmarshal([]byte(s), &keys)
			} else {
				var encoded []byte
				encoded, err = json.Marshal(raw)
				if err == nil {
					err = json.Unmarshal(encoded, &keys)
				}
			}
			if err == nil {
				field.Set(reflect.ValueOf(keys))
			}
		case field.Kind() == reflect.String:
			var s string
			var yes bool
			s, yes = raw.(string)
			if !yes {
				err = fmt.Errorf("must be a string")
			} else {
				field.SetString(s)
			}
		case field.Kind() == reflect.Float64:
			var number float64
			number, err = strconv.ParseFloat(fmt.Sprint(raw), 64)
			if err == nil {
				field.SetFloat(number)
			}
		default:
			var number int64
			number, err = strconv.ParseInt(fmt.Sprint(raw), 10, 64)
			if err == nil {
				field.SetInt(number)
			}
		}
		if err != nil {
			return cfg, fmt.Errorf("invalid config field %s", name)
		}
	}
	for name := range fields {
		if !known[name] {
			return cfg, fmt.Errorf("unknown config field %q", name)
		}
	}
	return cfg, cfg.Validate()
}

func LoadConfig(path string) (Config, error) {
	var data []byte
	if path != "" {
		var err error
		data, err = os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config: %w", err)
		}
	}
	return ParseConfig(data, os.LookupEnv)
}

func (c Config) Validate() error {
	if len(c.Keys) == 0 {
		return fmt.Errorf("at least one user key is required")
	}
	seen := map[string]bool{}
	for _, key := range c.Keys {
		if strings.TrimSpace(key) == "" || seen[key] {
			return fmt.Errorf("user keys must be nonempty and unique")
		}
		seen[key] = true
	}
	if strings.TrimSpace(c.UpstreamToken) == "" {
		return fmt.Errorf("upstream_token is required")
	}
	for _, target := range []string{c.UpstreamURL, c.SubscriptionURL} {
		u, err := url.Parse(target)
		if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.Fragment != "" {
			return fmt.Errorf("upstream URLs must be absolute HTTP(S) URLs without credentials or fragments")
		}
	}
	if c.Listen == "" || c.Database == "" {
		return fmt.Errorf("listen and database must be nonempty")
	}
	if math.IsNaN(c.Reserve) || math.IsInf(c.Reserve, 0) || c.Reserve < 0 || c.Reserve >= 100 {
		return fmt.Errorf("reserve must be in [0,100)")
	}
	if c.DeficitPolicy != "proportional" && c.DeficitPolicy != "freeze" {
		return fmt.Errorf("deficit_policy must be proportional or freeze")
	}
	if c.Concurrency < 1 || c.QueuePerUser < 1 || c.StartInterval < 0 || c.QueueTimeout <= 0 || c.RequestTimeout <= 0 || c.SnapshotMaxAge <= 0 || c.PollInterval <= 0 {
		return fmt.Errorf("invalid scheduler durations or limits")
	}
	if c.MemoryBodyBytes < 0 || c.MaxBodyBytes <= 0 || c.MemoryBodyBytes > c.MaxBodyBytes {
		return fmt.Errorf("invalid request body limits")
	}
	if math.IsNaN(c.UsageMultiplier) || math.IsInf(c.UsageMultiplier, 0) || c.UsageMultiplier <= 0 {
		return fmt.Errorf("usage_multiplier must be finite and positive")
	}
	return nil
}
