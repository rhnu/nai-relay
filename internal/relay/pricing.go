package relay

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"math"
	"mime"
	"mime/multipart"
	"strings"

	_ "golang.org/x/image/webp"
)

// RequestInfo is a read-only projection of native metadata. The relay forwards
// the separately retained original body, including multipart binaries.
type RequestInfo struct {
	Payload                   map[string]any
	Samples                   int
	StreamMode                string
	sourceWidth, sourceHeight int
}

// ExtractRequest reads native JSON or the request JSON part of a multipart body.
// Binary parts may precede the metadata and are discarded without image decoding.
func ExtractRequest(contentType string, body io.Reader) (RequestInfo, error) {
	media, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return RequestInfo{}, fmt.Errorf("request content type: %w", err)
	}
	var payload map[string]any
	partSizes := make(map[string][2]int)
	switch media {
	case "application/json":
		payload, err = readRequestJSON(body)
	case "multipart/form-data":
		if params["boundary"] == "" {
			return RequestInfo{}, errors.New("multipart request has no boundary")
		}
		parts := multipart.NewReader(body, params["boundary"])
		found := false
		for {
			part, partErr := parts.NextPart()
			if partErr == io.EOF {
				break
			}
			if partErr != nil {
				return RequestInfo{}, fmt.Errorf("multipart request: %w", partErr)
			}
			if part.FormName() == "request" {
				if found {
					_ = part.Close()
					return RequestInfo{}, errors.New("multipart request has duplicate metadata")
				}
				found = true
				payload, err = readRequestJSON(part)
			} else {
				// DecodeConfig reads only the raster header. Native upscale omits
				// dimensions from JSON, so its source header supplies the quote.
				if config, _, configErr := image.DecodeConfig(part); configErr == nil {
					partSizes[part.FormName()] = [2]int{config.Width, config.Height}
				}
				_, err = io.Copy(io.Discard, part)
			}
			closeErr := part.Close()
			if err != nil {
				return RequestInfo{}, err
			}
			if closeErr != nil {
				return RequestInfo{}, closeErr
			}
		}
		if !found {
			return RequestInfo{}, errors.New("multipart request has no request metadata part")
		}
	default:
		return RequestInfo{}, fmt.Errorf("unsupported request content type %q", media)
	}
	if err != nil {
		return RequestInfo{}, err
	}
	info := RequestInfo{Payload: payload, Samples: 1}
	if source, ok := payload["image"].(string); ok {
		if size, found := partSizes[source]; found {
			info.sourceWidth, info.sourceHeight = size[0], size[1]
		} else if media == "application/json" {
			if config, _, configErr := image.DecodeConfig(base64.NewDecoder(base64.StdEncoding, strings.NewReader(source))); configErr == nil {
				info.sourceWidth, info.sourceHeight = config.Width, config.Height
			}
		}
	}
	p, _ := payload["parameters"].(map[string]any)
	if value, exists := p["n_samples"]; exists {
		n, err := priceNumber(value)
		if err != nil || n != math.Trunc(n) || n < 1 || n > 8 {
			return RequestInfo{}, errors.New("cannot price n_samples outside 1..8")
		}
		info.Samples = int(n)
	}
	if mode, exists := p["stream"]; exists {
		var ok bool
		info.StreamMode, ok = mode.(string)
		if !ok || (info.StreamMode != "sse" && info.StreamMode != "msgpack") {
			return RequestInfo{}, errors.New("unsupported native stream mode")
		}
	}
	return info, nil
}

func readRequestJSON(r io.Reader) (map[string]any, error) {
	d := json.NewDecoder(r)
	d.UseNumber()
	var value map[string]any
	if err := d.Decode(&value); err != nil {
		return nil, fmt.Errorf("request metadata: %w", err)
	}
	if value == nil {
		return nil, errors.New("request metadata must be an object")
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, errors.New("request metadata contains multiple JSON values")
		}
		return nil, fmt.Errorf("request metadata trailing bytes: %w", err)
	}
	return value, nil
}

// ErrUnknownPrice distinguishes an unavailable quote from a known free operation.
var ErrUnknownPrice = errors.New("request price unavailable")

type priceModel struct{ v5, medium, encodedVibe, character, legacy bool }

func nativePriceModel(name string) (priceModel, bool) {
	name = strings.TrimSuffix(name, "-inpainting")
	switch name {
	case "nai-diffusion-3", "nai-diffusion-furry-3":
		return priceModel{legacy: true}, true
	case "nai-diffusion-4-full", "nai-diffusion-4-curated-preview", "nai-diffusion-4-curated":
		return priceModel{encodedVibe: true}, true
	case "nai-diffusion-4-5-full", "nai-diffusion-4-5-curated":
		return priceModel{encodedVibe: true, character: true}, true
	case "nai-diffusion-5-full", "nai-diffusion-5-curated":
		return priceModel{v5: true}, true
	case "nai-diffusion-5-full-medium":
		return priceModel{v5: true, medium: true}, true
	default:
		return priceModel{}, false
	}
}

// Price quotes native operations in millionths of an Anlas or Opus percent.
// The coefficients follow the owned novelai-bridge pricing reference. V5 usage
// drawdown is an explicitly approximate area/steps/strength model.
func Price(path string, info RequestInfo, snap Snapshot, multiplier float64) (Quote, error) {
	if !finite(multiplier) || multiplier <= 0 {
		return Quote{}, fmt.Errorf("%w: invalid V5 multiplier", ErrUnknownPrice)
	}
	switch path {
	case "/ai/generate-image", "/ai/generate-image-stream":
		return priceGeneration(info, snap, multiplier)
	case "/ai/encode-vibe":
		model, ok := nativePriceModel(stringField(info.Payload, "model"))
		if !ok || !model.encodedVibe {
			return Quote{}, fmt.Errorf("%w: model cannot encode vibes", ErrUnknownPrice)
		}
		return Quote{Anlas: 2_000_000}, nil
	case "/ai/upscale":
		w, h := float64(info.sourceWidth), float64(info.sourceHeight)
		if w <= 0 || h <= 0 {
			return Quote{}, fmt.Errorf("%w: unavailable upscale source header dimensions", ErrUnknownPrice)
		}
		_, _, err := pricingSize(map[string]any{"width": w, "height": h})
		if err != nil {
			return Quote{}, err
		}
		pixels := w * h
		for ix, limit := range []float64{1048576, 1747627, 2446678, 3145728} {
			if pixels <= limit {
				return Quote{Anlas: int64(ix+1) * 1_000_000}, nil
			}
		}
		return Quote{}, fmt.Errorf("%w: upscale area exceeds known table", ErrUnknownPrice)
	case "/ai/augment-image":
		w, h, err := pricingSize(info.Payload)
		if _, hasWidth := info.Payload["width"]; !hasWidth {
			if _, hasHeight := info.Payload["height"]; !hasHeight && info.sourceWidth > 0 && info.sourceHeight > 0 {
				w, h = float64(info.sourceWidth), float64(info.sourceHeight)
				err = nil
			}
		}
		if err != nil {
			return Quote{}, err
		}
		if w*h > 3145728 {
			return Quote{}, fmt.Errorf("%w: director area exceeds pricing limit", ErrUnknownPrice)
		}
		tool := stringField(info.Payload, "req_type")
		switch tool {
		case "lineart", "sketch", "emotion", "declutter", "declutter-keep-bubbles", "colorize", "bg-removal":
		default:
			return Quote{}, fmt.Errorf("%w: unknown director tool", ErrUnknownPrice)
		}
		// Wire width and height already describe Director's prepared processing canvas.
		cost := generationAnlas(w*h, 28, 1, false, false, false, false)
		if tool == "bg-removal" {
			cost = cost*3 + 5
		} else if snap.Active && snap.Tier >= 3 && w*h <= 1048576 {
			cost = 0
		}
		return Quote{Anlas: int64(cost) * 1_000_000}, nil
	default:
		return Quote{}, fmt.Errorf("%w: unknown endpoint", ErrUnknownPrice)
	}
}

func priceGeneration(info RequestInfo, snap Snapshot, multiplier float64) (Quote, error) {
	model, ok := nativePriceModel(stringField(info.Payload, "model"))
	if !ok {
		return Quote{}, fmt.Errorf("%w: unknown image model", ErrUnknownPrice)
	}
	p, ok := info.Payload["parameters"].(map[string]any)
	if !ok {
		return Quote{}, fmt.Errorf("%w: missing generation parameters", ErrUnknownPrice)
	}
	if extra, exists := p["upscale"]; exists && extra != nil {
		return Quote{}, fmt.Errorf("%w: inline upscale pricing is not established", ErrUnknownPrice)
	}
	w, h, err := pricingSize(p)
	if err != nil {
		return Quote{}, err
	}
	steps, err := numberField(p, "steps", 23)
	if err != nil || steps < 1 || steps > 50 || steps != math.Trunc(steps) {
		return Quote{}, fmt.Errorf("%w: unknown steps", ErrUnknownPrice)
	}
	if model.medium {
		steps = 14
	}
	action := stringField(info.Payload, "action")
	strength := 1.0
	switch action {
	case "", "generate":
	case "img2img":
		strength, err = numberField(p, "strength", 1)
		if err == nil && boolField(p, "upscaled_enhance") {
			if !model.v5 {
				return Quote{}, fmt.Errorf("%w: unsupported Max Enhance model", ErrUnknownPrice)
			}
			w, h, err = maxEnhancePriceSize(w, h)
		}
	case "infill":
		if nested, exists := p["img2img"]; exists {
			fields, valid := nested.(map[string]any)
			if !valid {
				return Quote{}, fmt.Errorf("%w: invalid infill strength object", ErrUnknownPrice)
			}
			strength, err = numberField(fields, "strength", 1)
		}
	default:
		return Quote{}, fmt.Errorf("%w: unknown image action", ErrUnknownPrice)
	}
	if err != nil {
		return Quote{}, err
	}
	if !finite(strength) || strength < 0 || strength > 1 {
		return Quote{}, fmt.Errorf("%w: unknown image strength", ErrUnknownPrice)
	}
	pixels := w * h
	if pixels > 3145728 {
		return Quote{}, fmt.Errorf("%w: image area exceeds pricing limit", ErrUnknownPrice)
	}
	samples := info.Samples
	if samples == 0 {
		samples = 1
	}
	limit := 4
	if pixels <= 360448 {
		limit = 8
	} else if pixels <= 409600 {
		limit = 6
	}
	if samples < 1 || samples > limit {
		return Quote{}, fmt.Errorf("%w: samples exceed known canvas limit", ErrUnknownPrice)
	}
	characters, err := referenceCount(p, "director_reference_images")
	if err != nil {
		return Quote{}, err
	}
	vibes, err := referenceCount(p, "reference_image_multiple")
	if err != nil {
		return Quote{}, err
	}
	if !model.character {
		characters = 0
	}
	if !model.encodedVibe || action == "infill" || characters > 0 {
		vibes = 0
	}
	perImage := generationAnlas(pixels, steps, strength, model.v5, model.medium, model.legacy && boolField(p, "sm"), model.legacy && boolField(p, "sm_dyn"))
	if perImage > 140 {
		return Quote{}, fmt.Errorf("%w: per-image price exceeds known limit", ErrUnknownPrice)
	}
	free := snap.Active && snap.Tier >= 3 && characters == 0 && pixels <= 1048576 && steps <= 28
	quote := Quote{}
	billable := samples
	if free && model.v5 {
		free = !snap.UsageNegative && (snap.Normal > 0 || snap.PurchasedOpus > 0)
		if free {
			factor := (2.951823174884865e-6 + 5.753298233447344e-7*steps) / (2.951823174884865e-6 + 5.753298233447344e-7*23)
			if model.medium {
				factor = .58
			}
			usage := 100.0 / 1730 * (pixels / 1048576) * factor * strength * multiplier
			if !finite(usage) || usage < 0 || usage > float64(math.MaxInt64)/1_000_000 {
				return Quote{}, fmt.Errorf("%w: V5 usage overflow", ErrUnknownPrice)
			}
			quote.Opus = int64(math.Ceil(usage * 1_000_000))
		}
	}
	if free {
		billable--
	}
	surcharge := characters * 5 * samples
	if vibes > 4 {
		surcharge += (vibes - 4) * 2
	}
	quote.Anlas = int64(perImage*float64(billable)+float64(surcharge)) * 1_000_000
	return quote, nil
}

func generationAnlas(pixels, steps, strength float64, v5, medium, smea, dynamic bool) float64 {
	stepFactor := 1.0
	if medium {
		stepFactor = 1 / 1.06521739
	}
	base := math.Ceil(2.951823174884865e-6*pixels + 5.753298233447344e-7*pixels*steps*stepFactor)
	if smea {
		if dynamic {
			base *= 1.4
		} else {
			base *= 1.2
		}
	}
	if v5 {
		base *= 1.5
	}
	return math.Max(math.Ceil(base*strength), 2)
}

func maxEnhancePriceSize(w, h float64) (float64, float64, error) {
	bw, bh := math.Floor(w/16)*32, math.Floor(h/16)*32
	if bw == 0 || bh == 0 {
		return 0, 0, fmt.Errorf("%w: Max Enhance dimensions", ErrUnknownPrice)
	}
	scale := math.Min(math.Sqrt(3145728/(bw*bh)), 1)
	sx, sy := bw*scale/32, bh*scale/32
	w, h = math.Round(sx)*32, math.Round(sy)*32
	if w*h > 3145728 {
		w, h = math.Floor(sx)*32, math.Floor(sy)*32
	}
	if w < 32 || h < 32 {
		return 0, 0, fmt.Errorf("%w: Max Enhance target dimensions", ErrUnknownPrice)
	}
	return w, h, nil
}

func referenceCount(p map[string]any, name string) (int, error) {
	count := 0
	for _, field := range []string{name, name + "_cached"} {
		value, exists := p[field]
		if !exists {
			continue
		}
		refs, ok := value.([]any)
		if !ok {
			return 0, fmt.Errorf("%w: invalid %s", ErrUnknownPrice, field)
		}
		count += len(refs)
	}
	return count, nil
}

func pricingSize(p map[string]any) (float64, float64, error) {
	w, err := numberField(p, "width", 0)
	if err != nil {
		return 0, 0, err
	}
	h, err := numberField(p, "height", 0)
	if err != nil {
		return 0, 0, err
	}
	if w <= 0 || h <= 0 || w > 16384 || h > 16384 || w != math.Trunc(w) || h != math.Trunc(h) {
		return 0, 0, fmt.Errorf("%w: missing or invalid dimensions", ErrUnknownPrice)
	}
	return w, h, nil
}
func stringField(p map[string]any, name string) string { value, _ := p[name].(string); return value }
func boolField(p map[string]any, name string) bool     { value, _ := p[name].(bool); return value }
func numberField(p map[string]any, name string, def float64) (float64, error) {
	value, ok := p[name]
	if !ok {
		return def, nil
	}
	n, err := priceNumber(value)
	if err != nil {
		return 0, fmt.Errorf("%w: %s: %w", ErrUnknownPrice, name, err)
	}
	return n, nil
}
func priceNumber(value any) (float64, error) {
	var n float64
	var err error
	switch value := value.(type) {
	case json.Number:
		n, err = value.Float64()
	case float64:
		n = value
	case int:
		n = float64(value)
	case int64:
		n = float64(value)
	default:
		return 0, errors.New("expected number")
	}
	if err != nil {
		return 0, err
	}
	if !finite(n) {
		return 0, errors.New("expected finite number")
	}
	return n, nil
}
func finite(n float64) bool { return !math.IsNaN(n) && !math.IsInf(n, 0) }
