package main

import (
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
)

func TestBuildPriceRegexForBigEraStore(t *testing.T) {
	pattern := buildPriceRegex("大時代攝影器材", "624567")
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("compile generated price regex: %v", err)
	}

	page := "merchant_id=9146 product_id=624567 tr_pw=9999 tr_so=w " +
		"merchant_id=2868 product_id=624567 tr_pw=8888 tr_so=w " +
		"merchant_id=2868 product_id=123456 tr_pw=7777 tr_so=w"
	match := re.FindStringSubmatch(page)
	if len(match) != 2 || match[1] != "8888" {
		t.Fatalf("expected only target merchant/product water price 8888, got %v", match)
	}
}

func TestBuildAutoDisplayNameUsesSelectedStore(t *testing.T) {
	got := buildAutoDisplayName(" Olympus E-P7 (Galaxy 水貨)", "大時代攝影器材")
	want := "Olympus E-P7 (大時代水貨)"
	if got != want {
		t.Fatalf("buildAutoDisplayName() = %q, want %q", got, want)
	}
}

func TestFormatFeishuPriceChangeShowsCNYAndSourceCurrency(t *testing.T) {
	change := priceChange{
		Product: Product{ID: 1, Name: "Olympus PEN (Galaxy 水貨)", StoreName: "Galaxy", Currency: "HKD"},
		Old:     1000,
		New:     950,
	}
	got, err := formatFeishuPriceChange(change, 0.92)
	if err != nil {
		t.Fatalf("formatFeishuPriceChange() error = %v", err)
	}
	want := "- Olympus PEN（Galaxy）：↓ 下降｜CNY 920.00（原价 HKD 1000.00）→ CNY 874.00（原价 HKD 950.00）"
	if got != want {
		t.Fatalf("formatFeishuPriceChange() = %q, want %q", got, want)
	}
}

func TestFormatFeishuPriceChangeDirection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		currency  string
		oldPrice  float64
		newPrice  float64
		direction string
	}{
		{"CNY increase", "CNY", 950, 1000, "↑ 上涨"},
		{"CNY decrease", "CNY", 1000, 950, "↓ 下降"},
		{"HKD increase", "HKD", 950, 1000, "↑ 上涨"},
		{"HKD decrease", "HKD", 1000, 950, "↓ 下降"},
		{"unchanged", "CNY", 1000, 1000, "→ 持平"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			change := priceChange{
				Product: Product{Name: "Camera", StoreName: "Store", Currency: tc.currency},
				Old:     tc.oldPrice,
				New:     tc.newPrice,
			}
			got, err := formatFeishuPriceChange(change, 0.92)
			if err != nil {
				t.Fatalf("formatFeishuPriceChange() error = %v", err)
			}
			if !strings.Contains(got, "："+tc.direction+"｜") {
				t.Fatalf("formatFeishuPriceChange() = %q, want direction %q", got, tc.direction)
			}
		})
	}
}

func TestConvertPriceToCNYKeepsCNYUnchanged(t *testing.T) {
	got, err := convertPriceToCNY(123.45, "CNY", 0.92)
	if err != nil || got != 123.45 {
		t.Fatalf("convertPriceToCNY() = %.2f, %v; want 123.45, nil", got, err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestSendFeishuText(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q", got)
		}
		var payload struct {
			MsgType string `json:"msg_type"`
			Content struct {
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
		}
		if payload.MsgType != "text" || payload.Content.Text != "每日更新完成" {
			t.Errorf("unexpected payload: %+v", payload)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"code":0,"msg":"success"}`)),
			Header:     make(http.Header),
			Request:    r,
		}, nil
	})}

	if err := sendFeishuTextWithClient(client, "https://example.test/hook", "每日更新完成"); err != nil {
		t.Fatalf("sendFeishuText() error = %v", err)
	}
}

func TestSendFeishuTextRejectsAPIError(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"code":19001,"msg":"invalid token"}`)),
			Header:     make(http.Header),
			Request:    r,
		}, nil
	})}

	if err := sendFeishuTextWithClient(client, "https://example.test/hook", "每日更新完成"); err == nil {
		t.Fatal("sendFeishuText() expected API error")
	}
}
