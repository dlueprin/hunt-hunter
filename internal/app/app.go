package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"xhunt-hunter/internal/model"
	"xhunt-hunter/internal/store"
	"xhunt-hunter/internal/xhunt"
)

type Config struct {
	DSN             string
	Domain          string
	SeedsRaw        string
	MaxDepth        int
	RequestInterval time.Duration
	RateLimitSleep  time.Duration
	LogDir          string
	ImportJSON      string
	MigrateOnly     bool
}

func Run(ctx context.Context, cfg Config) error {
	logger, closeLog, err := newLogger(cfg.LogDir)
	if err != nil {
		return err
	}
	defer closeLog()

	logger.Printf("xhunt-hunter starting max_depth=%d request_interval=%s rate_limit_sleep=%s", cfg.MaxDepth, cfg.RequestInterval, cfg.RateLimitSleep)

	st, err := store.Open(cfg.DSN)
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.Ping(ctx); err != nil {
		return err
	}
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	if cfg.MigrateOnly {
		logger.Printf("schema migrated, exiting because migrate-only=true")
		return nil
	}

	if err := importJSONIfNeeded(ctx, st, logger, cfg.ImportJSON); err != nil {
		return err
	}

	seeds := parseSeeds(cfg.SeedsRaw)
	if len(seeds) > 0 {
		if err := st.SeedAccounts(ctx, seeds); err != nil {
			return err
		}
		logger.Printf("seeded %d accounts: %s", len(seeds), strings.Join(seeds, ","))
	}

	client := xhunt.NewClient(cfg.Domain)
	requestsSinceLastRateLimit := 0
	requestCount := 0

	for {
		select {
		case <-ctx.Done():
			logger.Printf("context canceled, stopping crawl")
			return ctx.Err()
		default:
		}

		item, err := st.NextPendingAccount(ctx, cfg.MaxDepth, time.Now())
		if err != nil {
			return err
		}
		if item == nil {
			logger.Printf("no pending accounts left, crawl finished")
			return nil
		}

		if requestCount > 0 && cfg.RequestInterval > 0 {
			logger.Printf("sleeping between requests duration=%s", cfg.RequestInterval)
			if err := sleepWithContext(ctx, cfg.RequestInterval); err != nil {
				return err
			}
		}

		now := time.Now()
		requestCount++
		requestsSinceLastRateLimit++
		if err := st.MarkAttempt(ctx, item.Username, now); err != nil {
			return err
		}

		logger.Printf("fetch start username=%s depth=%d request_no=%d", item.Username, item.DiscoveryDepth, requestCount)
		resp, err := client.FetchUserInfo(ctx, item.Username)
		if err != nil {
			nextRetry := time.Now().Add(cfg.RateLimitSleep)
			logger.Printf("fetch failed username=%s depth=%d err=%v", item.Username, item.DiscoveryDepth, err)
			if markErr := st.MarkFailed(ctx, item.Username, time.Now(), nextRetry, err.Error()); markErr != nil {
				return markErr
			}
			continue
		}

		if strings.EqualFold(strings.TrimSpace(resp.Err), "rate_limit") {
			now = time.Now()
			nextRetry := now.Add(cfg.RateLimitSleep)
			logger.Printf("rate_limit username=%s depth=%d at=%s requests_since_last_rate_limit=%d next_retry_at=%s", item.Username, item.DiscoveryDepth, now.Format(time.RFC3339), requestsSinceLastRateLimit, nextRetry.Format(time.RFC3339))
			if err := st.MarkRateLimited(ctx, item.Username, now, nextRetry, "rate_limit"); err != nil {
				return err
			}
			requestsSinceLastRateLimit = 0
			if err := sleepWithContext(ctx, cfg.RateLimitSleep); err != nil {
				return err
			}
			continue
		}

		if resp.Data == nil {
			nextRetry := time.Now().Add(cfg.RateLimitSleep)
			logger.Printf("empty data username=%s depth=%d err=%s", item.Username, item.DiscoveryDepth, resp.Err)
			if err := st.MarkFailed(ctx, item.Username, time.Now(), nextRetry, fmt.Sprintf("empty data err=%s", resp.Err)); err != nil {
				return err
			}
			continue
		}

		now = time.Now()
		if err := st.SaveFetchedAccount(ctx, item.Username, item.DiscoveryDepth, resp.Data, now); err != nil {
			return err
		}
		if err := st.SaveFollowers(ctx, item.Username, item.DiscoveryDepth, resp.Data.FollowersByBucket(), now); err != nil {
			return err
		}
		if err := st.MarkFetchedSuccess(ctx, item.Username, now); err != nil {
			return err
		}
		logger.Printf(
			"fetch success username=%s depth=%d rank=%v followers(global=%d cn=%d top100=%d)",
			item.Username,
			item.DiscoveryDepth,
			resp.Data.Feature.Rank.KOLRank,
			resp.Data.Feature.KOLFollowers.GlobalKOLFollowersCount,
			resp.Data.Feature.KOLFollowers.CNKOLFollowersCount,
			resp.Data.Feature.KOLFollowers.TopKOLFollowersCount,
		)
	}
}

func importJSONIfNeeded(ctx context.Context, st *store.Store, logger *log.Logger, raw string) error {
	paths := expandImportPaths(raw)
	if len(paths) == 0 {
		return nil
	}
	logger.Printf("starting json import files=%d", len(paths))
	imported := 0
	for _, path := range paths {
		rows, err := loadTopRankingRows(path)
		if err != nil {
			return fmt.Errorf("load import file %s: %w", path, err)
		}
		for _, row := range rows {
			if err := st.SaveImportedSeed(ctx, row, time.Now()); err != nil {
				return fmt.Errorf("import row from %s: %w", path, err)
			}
			imported++
		}
		logger.Printf("imported file path=%s rows=%d", path, len(rows))
	}
	logger.Printf("json import finished total_rows=%d", imported)
	return nil
}

func expandImportPaths(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{})
	var result []string
	for _, part := range parts {
		path := strings.TrimSpace(part)
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		if info.IsDir() {
			entries, err := os.ReadDir(path)
			if err != nil {
				continue
			}
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
					continue
				}
				full := filepath.Join(path, entry.Name())
				if _, ok := seen[full]; ok {
					continue
				}
				seen[full] = struct{}{}
				result = append(result, full)
			}
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		result = append(result, path)
	}
	return result
}

func loadTopRankingRows(path string) ([]model.TopRankingRow, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var list model.TopRankingList
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, err
	}
	return list.Data, nil
}

func parseSeeds(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(parts))
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		username := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(part, "@")))
		if username == "" {
			continue
		}
		if _, ok := seen[username]; ok {
			continue
		}
		seen[username] = struct{}{}
		result = append(result, username)
	}
	return result
}

func newLogger(logDir string) (*log.Logger, func(), error) {
	if strings.TrimSpace(logDir) == "" {
		logDir = "logs"
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, nil, err
	}
	filename := fmt.Sprintf("xhunt-hunter-%s.txt", time.Now().Format("20060102-150405"))
	filePath := filepath.Join(logDir, filename)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	writer := io.MultiWriter(os.Stdout, file)
	logger := log.New(writer, "[xhunt-hunter] ", log.LstdFlags)
	return logger, func() {
		_ = file.Close()
	}, nil
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
