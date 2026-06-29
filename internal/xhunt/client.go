package xhunt

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"xhunt-hunter/internal/model"
)

const userInfoURL = "https://kol.xhunt.ai/api/twitter/user-info"
const exitIPURL = "https://api.ipify.org"

type Client struct {
	httpClient *http.Client
	domain     string
	ipTimeout  time.Duration
}

func NewClient(domain string, proxyPort int, requestTimeout time.Duration) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if proxyPort > 0 {
		transport.Proxy = http.ProxyURL(&url.URL{
			Scheme: "http",
			Host:   "127.0.0.1:" + strconv.Itoa(proxyPort),
		})
	}
	if requestTimeout <= 0 {
		requestTimeout = 6 * time.Second
	}
	ipTimeout := 3 * time.Second
	if requestTimeout < ipTimeout {
		ipTimeout = requestTimeout
	}
	return &Client{
		httpClient: &http.Client{
			Timeout:   requestTimeout,
			Transport: transport,
		},
		domain:    domain,
		ipTimeout: ipTimeout,
	}
}

func (c *Client) FetchExitIP(ctx context.Context) (string, error) {
	ipCtx, cancel := context.WithTimeout(ctx, c.ipTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ipCtx, http.MethodGet, exitIPURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return strings.TrimSpace(string(body)), nil
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
