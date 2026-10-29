package relay

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"mime/multipart"
	"strings"
	"testing"
)

func pricingRequest(t *testing.T, body string) RequestInfo {
	t.Helper()
	info, err := ExtractRequest("application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestExtractMultipartMetadataAfterBinary(t *testing.T) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, err := w.CreateFormFile("image", "blob")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write([]byte{0, 255, 13, 10, 42})
	part, err = w.CreateFormField("request")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(part, `{"model":"nai-diffusion-4-5-full","parameters":{"width":1024,"height":1024,"n_samples":2,"stream":"msgpack","director_reference_images_cached":[{"cache_secret_key":"a"}]},"extension":{"keep":true}}`)
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), body.Bytes()...)
	info, err := ExtractRequest(w.FormDataContentType(), bytes.NewReader(body.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if info.Samples != 2 || info.StreamMode != "msgpack" {
		t.Fatalf("metadata: %+v", info)
	}
	if !bytes.Equal(body.Bytes(), original) {
		t.Fatal("original body changed")
	}
	quote, err := Price("/ai/generate-image", info, Snapshot{Active: true, Tier: 3}, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Each image costs 17 Anlas at 23 steps and 5 for its character reference.
	if quote.Anlas != 44*Micro || quote.Opus != 0 {
		t.Fatalf("quote: %+v", quote)
	}
}

func TestGenerationPricingModesAndAllowances(t *testing.T) {
	cases := []struct {
		name, body string
		snap       Snapshot
		want       Quote
	}{
		{"ordinary", `{"model":"nai-diffusion-3","parameters":{"width":1024,"height":1024,"steps":28}}`, Snapshot{}, Quote{Anlas: 20 * Micro}},
		{"opus first only", `{"model":"nai-diffusion-4-full","parameters":{"width":1024,"height":1024,"steps":28,"n_samples":3}}`, Snapshot{Active: true, Tier: 3}, Quote{Anlas: 40 * Micro}},
		{"inactive", `{"model":"nai-diffusion-4-full","parameters":{"width":1024,"height":1024,"steps":28}}`, Snapshot{Tier: 3}, Quote{Anlas: 20 * Micro}},
		{"img2img", `{"model":"nai-diffusion-4-full","action":"img2img","parameters":{"width":1024,"height":1024,"steps":28,"strength":0.5}}`, Snapshot{}, Quote{Anlas: 10 * Micro}},
		{"pure infill ignores flat strength", `{"model":"nai-diffusion-4-full-inpainting","action":"infill","parameters":{"width":1024,"height":1024,"steps":28,"strength":0.1}}`, Snapshot{}, Quote{Anlas: 20 * Micro}},
		{"nested infill", `{"model":"nai-diffusion-4-5-full-inpainting","action":"infill","parameters":{"width":1024,"height":1024,"steps":28,"img2img":{"strength":0.4}}}`, Snapshot{}, Quote{Anlas: 8 * Micro}},
		{"dynamic smea", `{"model":"nai-diffusion-3","parameters":{"width":1024,"height":1024,"steps":28,"sm":true,"sm_dyn":true}}`, Snapshot{}, Quote{Anlas: 28 * Micro}},
		{"vibe overage", `{"model":"nai-diffusion-4-full","parameters":{"width":1024,"height":1024,"steps":28,"reference_image_multiple_cached":[{},{},{},{},{},{}]}}`, Snapshot{Active: true, Tier: 3}, Quote{Anlas: 4 * Micro}},
		{"v3 raw vibes have no encoded overage", `{"model":"nai-diffusion-3","parameters":{"width":1024,"height":1024,"steps":28,"reference_image_multiple":["a","b","c","d","e"]}}`, Snapshot{Active: true, Tier: 3}, Quote{}},
		{"character precedence", `{"model":"nai-diffusion-4-5-full","parameters":{"width":1024,"height":1024,"steps":28,"director_reference_images_cached":[{}],"reference_image_multiple_cached":[{},{},{},{},{}]}}`, Snapshot{Active: true, Tier: 3}, Quote{Anlas: 25 * Micro}},
		{"v5 anlas fallback", `{"model":"nai-diffusion-5-full","parameters":{"width":1024,"height":1024,"steps":28}}`, Snapshot{Active: true, Tier: 3, UsageNegative: true, Normal: 100 * Micro}, Quote{Anlas: 30 * Micro}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Price("/ai/generate-image", pricingRequest(t, tc.body), tc.snap, 1)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestV5ApproximationAndMixedBatch(t *testing.T) {
	info := pricingRequest(t, `{"model":"nai-diffusion-5-full","parameters":{"width":1024,"height":1024,"steps":23,"n_samples":3}}`)
	snap := Snapshot{Active: true, Tier: 3, Normal: 100 * Micro}
	quote, err := Price("/ai/generate-image-stream", info, snap, 1)
	if err != nil {
		t.Fatal(err)
	}
	// One baseline image uses 100/1730 percent; the other two use Anlas.
	if quote.Opus != 57804 || quote.Anlas != 52*Micro {
		t.Fatalf("mixed batch: %+v", quote)
	}
	quote2, err := Price("/ai/generate-image", info, snap, 2)
	if err != nil {
		t.Fatal(err)
	}
	if quote2.Opus != 115607 || quote2.Anlas != quote.Anlas {
		t.Fatalf("multiplier: %+v", quote2)
	}
	info.Payload["model"] = "nai-diffusion-5-full-medium"
	quote, err = Price("/ai/generate-image", info, snap, 1)
	if err != nil {
		t.Fatal(err)
	}
	if quote.Opus != 33527 || quote.Anlas != 36*Micro {
		t.Fatalf("medium: %+v", quote)
	}
}

func TestZeroStrengthUsesNativeMinimumAnlas(t *testing.T) {
	info := pricingRequest(t, `{"model":"nai-diffusion-5-full","action":"img2img","parameters":{"width":1024,"height":1024,"steps":23,"strength":0}}`)
	quote, err := Price("/ai/generate-image", info, Snapshot{}, 1)
	if err != nil || quote.Anlas != 2*Micro {
		t.Fatalf("zero strength minimum: %+v %v", quote, err)
	}
	quote, err = Price("/ai/generate-image", info, Snapshot{Active: true, Tier: 3, Normal: Micro}, 1)
	if err != nil || quote != (Quote{}) {
		t.Fatalf("zero strength Opus formula: %+v %v", quote, err)
	}
}

func TestMaxEnhancePricesTargetCanvas(t *testing.T) {
	info := pricingRequest(t, `{"model":"nai-diffusion-5-full","action":"img2img","parameters":{"width":512,"height":512,"steps":23,"strength":0.5,"upscaled_enhance":true}}`)
	quote, err := Price("/ai/generate-image", info, Snapshot{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if quote.Anlas != 13*Micro {
		t.Fatalf("target quote: %+v", quote)
	}
	// A 1024-square source prices on the capped, 32px-rounded target.
	w, h, err := maxEnhancePriceSize(1024, 1024)
	if err != nil || w != 1760 || h != 1760 {
		t.Fatalf("capped target %v x %v: %v", w, h, err)
	}
}

func TestAuxiliaryPrices(t *testing.T) {
	for _, tc := range []struct {
		path, body string
		snap       Snapshot
		want       int64
	}{
		{"/ai/encode-vibe", `{"model":"nai-diffusion-4-full"}`, Snapshot{}, 2},
		{"/ai/augment-image", `{"width":1024,"height":1024,"req_type":"bg-removal"}`, Snapshot{Active: true, Tier: 3}, 65},
		{"/ai/augment-image", `{"width":1024,"height":1024,"req_type":"lineart"}`, Snapshot{Active: true, Tier: 3}, 0},
		{"/ai/augment-image", `{"width":1024,"height":1024,"req_type":"colorize","defry":3}`, Snapshot{}, 20},
	} {
		quote, err := Price(tc.path, pricingRequest(t, tc.body), tc.snap, 1)
		if err != nil {
			t.Fatal(err)
		}
		if quote.Anlas != tc.want*Micro || quote.Opus != 0 {
			t.Fatalf("%s: %+v", tc.path, quote)
		}
	}
}

// A complete IHDR permits dimension inspection without any pixel payload.
func pricingPNGHeader(width, height uint32) []byte {
	data := append([]byte("\x89PNG\r\n\x1a\n"), 0, 0, 0, 13, 'I', 'H', 'D', 'R')
	data = binary.BigEndian.AppendUint32(data, width)
	data = binary.BigEndian.AppendUint32(data, height)
	data = append(data, 8, 6, 0, 0, 0)
	return binary.BigEndian.AppendUint32(data, crc32.ChecksumIEEE(data[12:]))
}

func TestUpscaleUsesSourceHeaderInBothTransports(t *testing.T) {
	for _, tc := range []struct {
		width, height uint32
		want          int64
	}{{1024, 1024, 1}, {1536, 1024, 2}, {2048, 1024, 3}, {2048, 1536, 4}} {
		image := pricingPNGHeader(tc.width, tc.height)
		data, _ := json.Marshal(map[string]any{"model": "nai-diffusion-5-full", "image": base64.StdEncoding.EncodeToString(image), "width": 1, "height": 1})
		info := pricingRequest(t, string(data))
		quote, err := Price("/ai/upscale", info, Snapshot{}, 1)
		if err != nil || quote.Anlas != tc.want*Micro {
			t.Fatalf("JSON source %dx%d: %+v %v", tc.width, tc.height, quote, err)
		}
		var body bytes.Buffer
		w := multipart.NewWriter(&body)
		part, err := w.CreateFormFile("source-part", "blob")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = part.Write(image)
		part, err = w.CreateFormField("request")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.WriteString(part, `{"image":"source-part","model":"nai-diffusion-5-full"}`)
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
		info, err = ExtractRequest(w.FormDataContentType(), &body)
		if err != nil {
			t.Fatal(err)
		}
		quote, err = Price("/ai/upscale", info, Snapshot{}, 1)
		if err != nil || quote.Anlas != tc.want*Micro {
			t.Fatalf("multipart source %dx%d: %+v %v", tc.width, tc.height, quote, err)
		}
	}
}

func TestUnknownPricesNeverBecomeFree(t *testing.T) {
	for _, body := range []string{
		`{"model":"future-model","parameters":{"width":1024,"height":1024}}`,
		`{"model":"nai-diffusion-5-full","parameters":{"width":0,"height":1024}}`,
		`{"model":"nai-diffusion-5-full","parameters":{"width":4096,"height":4096}}`,
		`{"model":"nai-diffusion-5-full","action":"future-mode","parameters":{"width":1024,"height":1024}}`,
		`{"model":"nai-diffusion-5-full","parameters":{"width":1024,"height":1024,"steps":"23"}}`,
	} {
		_, err := Price("/ai/generate-image", pricingRequest(t, body), Snapshot{}, 1)
		if !errors.Is(err, ErrUnknownPrice) {
			t.Fatalf("body %s: %v", body, err)
		}
	}
	for _, body := range []string{`null`, `{"parameters":{"n_samples":0}}`, `{"parameters":{"stream":"future"}}`, `{} {}`} {
		if _, err := ExtractRequest("application/json", strings.NewReader(body)); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
}

func TestUnrelatedParametersArePreserved(t *testing.T) {
	info := pricingRequest(t, `{"model":"nai-diffusion-3","input":"prompt","parameters":{"width":1024,"height":1024,"steps":28,"sampler":"extension_sampler","seed":-1,"scale":"native-extension"},"extra":{"x":12}}`)
	before, _ := json.Marshal(info.Payload)
	if _, err := Price("/ai/generate-image", info, Snapshot{}, 1); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(info.Payload)
	if !bytes.Equal(before, after) {
		t.Fatal("pricing mutated request metadata")
	}
}
