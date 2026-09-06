package edgecache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	baseURL string
	http    *http.Client
}

type Options struct {
	BaseURL   string
	TimeoutMS int
	HTTP      *http.Client
}

type GetResult struct {
	Hit        bool            `json:"hit"`
	Key        string          `json:"key"`
	Value      json.RawMessage `json:"value"`
	TTLSeconds int64           `json:"ttlSeconds"`
}

type MGetResult struct {
	Results []GetResult `json:"results"`
}

type SetEntry struct {
	Key        string `json:"key"`
	Value      any    `json:"value"`
	TTLSeconds int64  `json:"ttlSeconds"`
}

type VersionResult struct {
	Namespace string `json:"namespace"`
	Version   int64  `json:"version"`
}

func New(options Options) *Client {
	baseURL := strings.TrimRight(strings.TrimSpace(options.BaseURL), "/")
	if baseURL == "" {
		baseURL = "http://127.0.0.1:4410"
	}
	httpClient := options.HTTP
	if httpClient == nil {
		timeout := 3 * time.Second
		if options.TimeoutMS > 0 {
			timeout = time.Duration(options.TimeoutMS) * time.Millisecond
		}
		httpClient = &http.Client{Timeout: timeout}
	}
	return &Client{baseURL: baseURL, http: httpClient}
}

func (c *Client) Get(ctx context.Context, key string) (GetResult, error) {
	var result GetResult
	err := c.doJSON(ctx, http.MethodGet, "/v1/cache/get?key="+url.QueryEscape(key), nil, &result)
	if len(result.Value) == 0 {
		result.Value = json.RawMessage("null")
	}
	return result, err
}

func (c *Client) Set(ctx context.Context, key string, value any, ttlSeconds int64) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/cache/set", map[string]any{
		"key":        key,
		"value":      value,
		"ttlSeconds": ttlSeconds,
	}, nil)
}

func (c *Client) MGet(ctx context.Context, keys []string) (MGetResult, error) {
	var result MGetResult
	err := c.doJSON(ctx, http.MethodPost, "/v1/cache/mget", map[string]any{"keys": keys}, &result)
	for i := range result.Results {
		if len(result.Results[i].Value) == 0 {
			result.Results[i].Value = json.RawMessage("null")
		}
	}
	return result, err
}

func (c *Client) MSet(ctx context.Context, entries []SetEntry) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/cache/mset", map[string]any{"entries": entries}, nil)
}

func (c *Client) Delete(ctx context.Context, key string) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/cache/del?key="+url.QueryEscape(key), nil, nil)
}

func (c *Client) Clear(ctx context.Context, pattern string) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/cache/clear", map[string]any{"pattern": pattern}, nil)
}

func (c *Client) Stats(ctx context.Context, out any) error {
	return c.doJSON(ctx, http.MethodGet, "/v1/cache/stats", nil, out)
}

func (c *Client) GetVersion(ctx context.Context, namespace string) (VersionResult, error) {
	var result VersionResult
	err := c.doJSON(ctx, http.MethodGet, "/v1/forum/version?namespace="+url.QueryEscape(namespace), nil, &result)
	return result, err
}

func (c *Client) BumpVersion(ctx context.Context, namespace string) (VersionResult, error) {
	var result VersionResult
	err := c.doJSON(ctx, http.MethodPost, "/v1/forum/version/bump", map[string]any{"namespace": namespace}, &result)
	return result, err
}

func (c *Client) doJSON(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("edgecache request failed: %s %s: %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
