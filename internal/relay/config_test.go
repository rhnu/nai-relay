package relay

import "testing"

func TestConfigEnvironmentReplacesKeys(t *testing.T) {
	env := map[string]string{"NAI_RELAY_KEYS": "[\"new\"]", "NAI_RELAY_UPSTREAM_TOKEN": "token"}
	c, err := ParseConfig([]byte("keys = ['old', 'older']\nstart_interval = '3s'"), func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Keys) != 1 || c.Keys[0] != "new" || c.StartInterval.Seconds() != 3 {
		t.Fatalf("unexpected configuration")
	}
}

func TestInvalidConfig(t *testing.T) {
	for _, input := range []string{"keys=['a','a']", "keys=['a']\nreserve=100", "keys=['a']\ndeficit_policy='ignore'", "keys=['a']\nunknown=true", "keys=['a']\nusage_multiplier=nan"} {
		_, err := ParseConfig([]byte(input), func(k string) (string, bool) { return "token", k == "NAI_RELAY_UPSTREAM_TOKEN" })
		if err == nil {
			t.Errorf("accepted invalid input: %s", input)
		}
	}
}
