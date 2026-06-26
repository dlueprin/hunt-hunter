package xhunt

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"xhunt-hunter/internal/model"
)

const userInfoURL = "https://kol.xhunt.ai/api/twitter/user-info"

type Client struct {
	httpClient *http.Client
	domain     string
}

func NewClient(domain string) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	return &Client{
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
		domain: domain,
	}
}

func (c *Client) FetchUserInfo(ctx context.Context, username string) (*model.UserInfoResponse, error) {
	reqURL := fmt.Sprintf("%s?username=%s&domain=%s",
		userInfoURL,
		url.QueryEscape(strings.TrimSpace(username)),
		url.QueryEscape(c.domain),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("User-Agent", "xhunt-hunter/0.1")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var parsed model.UserInfoResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("decode response: %w, body=%s", err, string(body))
	}
	return &parsed, nil
}
