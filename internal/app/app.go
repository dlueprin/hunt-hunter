package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"xhunt-hunter/internal/model"
	"xhunt-hunter/internal/store"
	"xhunt-hunter/internal/wifi"
	"xhunt-hunter/internal/xhunt"
)

type Config struct {
	DSN                      string
	Domain                   string
	ProxyPort                int
	RequestTimeout           time.Duration
	SeedsRaw                 string
	MaxDepth                 int
	ExpandRankLimit          int
	RequestInterval          time.Duration
	RateLimitSleep           time.Duration
	FailureBackoffMultiplier float64
	WiFiRecoverAfterFailures int
	WiFiRecoverMode          string
	WiFiRecoverService       string
	WiFiRecoverDevice        string
	WiFiRecoverSSID          string
	WiFiRecoverPassword      string
	WiFiRecoverFromSSID      string
	WiFiRecoverFromPassword  string
	WiFiRecoverToSSID        string
	WiFiRecoverToPassword    string
	WiFiRecoverWait          time.Duration
	WiFiRecoverPostWait      time.Duration
	ReplayOnStart            bool
	ReplaySuccessDepths      []int
	ReplaySuccessLimit       int
	SuccessCooldownEvery     int
	SuccessCooldownSleep     time.Duration
	SuccessCountEvery        int
	SuccessCooldownAllSleep  time.Duration
	LogDir                   string
	ImportJSON               string
	MigrateOnly              bool
}

const transientDBRetrySleep = 2 * time.Second

func handleTransientDBError(ctx context.Context, logger *log.Logger, stage, username string, depth int, err error) error {
	if !store.IsRetryableDBError(err) {
		return err
	}
	logger.Printf("transient db error stage=%s username=%s depth=%d err=%v retry_in=%s", stage, username, depth, err, transientDBRetrySleep)
	if sleepErr := sleepWithContext(ctx, transientDBRetrySleep); sleepErr != nil {
		return sleepErr
	}
	return nil
}

func Run(ctx context.Context, cfg Config) error {
	logger, closeLog, err := newLogger(cfg.LogDir)
	if err != nil {
		return err
	}
	defer closeLog()

	logger.Printf(
		"xhunt-hunter starting max_depth=%d expand_rank_limit=%d proxy_port=%d request_timeout=%s request_interval=%s rate_limit_sleep=%s failure_backoff_multiplier=%.2f wifi_recover_after_failures=%d wifi_recover_mode=%s wifi_recover_post_wait=%s replay_on_start=%t replay_success_depths=%v replay_success_limit=%d success_cooldown_every=%d success_cooldown_sleep=%s success_count_every=%d success_cooldownall_sleep=%s",
		cfg.MaxDepth,
		cfg.ExpandRankLimit,
		cfg.ProxyPort,
		cfg.RequestTimeout,
		cfg.RequestInterval,
		cfg.RateLimitSleep,
		cfg.FailureBackoffMultiplier,
		cfg.WiFiRecoverAfterFailures,
		cfg.WiFiRecoverMode,
		cfg.WiFiRecoverPostWait,
		cfg.ReplayOnStart,
		cfg.ReplaySuccessDepths,
		cfg.ReplaySuccessLimit,
		cfg.SuccessCooldownEvery,
		cfg.SuccessCooldownSleep,
		cfg.SuccessCountEvery,
		cfg.SuccessCooldownAllSleep,
	)

	st, err := store.Open(cfg.DSN)
	if err != nil {
		return err
	}
	defer st.Close()

	for {
		if err := st.Ping(ctx); err != nil {
			if retryErr := handleTransientDBError(ctx, logger, "ping", "", 0, err); retryErr == nil {
				continue
			}
			return err
		}
		break
	}
	if cfg.MigrateOnly {
		if err := st.Migrate(ctx); err != nil {
			return err
		}
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
	if cfg.ReplayOnStart && len(cfg.ReplaySuccessDepths) > 0 {
		requeued, err := st.RequeueSuccessfulAccountsForReplay(ctx, cfg.ReplaySuccessDepths, cfg.ReplaySuccessLimit, time.Now())
		if err != nil {
			return err
		}
		logger.Printf("replay requeued successful accounts count=%d depths=%v limit=%d", requeued, cfg.ReplaySuccessDepths, cfg.ReplaySuccessLimit)
	}

	client := xhunt.NewClient(cfg.Domain, cfg.ProxyPort, cfg.RequestTimeout)
	requestsSinceLastRateLimit := 0
	requestCount := 0
	successCount := 0
	successCount1 := 0
	consecutiveFailures := 0

	for {
		select {
		case <-ctx.Done():
			logger.Printf("context canceled, stopping crawl")
			return ctx.Err()
		default:
		}

		item, err := st.NextPendingAccount(ctx, cfg.MaxDepth, time.Now())
		if err != nil {
			if retryErr := handleTransientDBError(ctx, logger, "next_pending_account", "", 0, err); retryErr == nil {
				continue
			}
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
			if retryErr := handleTransientDBError(ctx, logger, "mark_attempt", item.Username, item.DiscoveryDepth, err); retryErr == nil {
				continue
			}
			return err
		}

		exitIP, exitIPErr := client.FetchExitIP(ctx)
		if exitIPErr != nil {
			logger.Printf("fetch start username=%s depth=%d request_no=%d exit_ip_lookup_err=%v", item.Username, item.DiscoveryDepth, requestCount, exitIPErr)
		} else {
			logger.Printf("fetch start username=%s depth=%d request_no=%d exit_ip=%s", item.Username, item.DiscoveryDepth, requestCount, exitIP)
		}
		resp, err := client.FetchUserInfo(ctx, item.Username)
		if err != nil {
			consecutiveFailures++
			retrySleep := nextFailureSleep(cfg.RateLimitSleep, cfg.FailureBackoffMultiplier, consecutiveFailures)
			nextRetry := time.Now().Add(retrySleep)
			logger.Printf(
				"fetch failed username=%s depth=%d err=%v consecutive_failures=%d retry_sleep=%s next_retry_at=%s",
				item.Username,
				item.DiscoveryDepth,
				err,
				consecutiveFailures,
				retrySleep,
				nextRetry.Format(time.RFC3339),
			)
			if markErr := st.MarkFailed(ctx, item.Username, time.Now(), nextRetry, err.Error()); markErr != nil {
				if retryErr := handleTransientDBError(ctx, logger, "mark_failed_after_fetch_error", item.Username, item.DiscoveryDepth, markErr); retryErr == nil {
					continue
				}
				return markErr
			}
			if err := sleepWithContext(ctx, retrySleep); err != nil {
				return err
			}
			continue
		}

		if strings.EqualFold(strings.TrimSpace(resp.Err), "rate_limit") {
			consecutiveFailures++
			eventAt := time.Now()
			accountRetrySleep := time.Duration(0)
			loopSleep := cfg.RateLimitSleep
			if loopSleep < 0 {
				loopSleep = 0
			}
			if shouldTriggerWiFiRecovery(cfg, consecutiveFailures) {
				if recovered := tryWiFiRecovery(logger, cfg, consecutiveFailures); recovered {
					consecutiveFailures = 0
					loopSleep = cfg.WiFiRecoverPostWait
					client = xhunt.NewClient(cfg.Domain, cfg.ProxyPort, cfg.RequestTimeout)
					logger.Printf("xhunt http client reset after wifi recovery")
				}
			}
			nextRetry := eventAt
			logger.Printf(
				"rate_limit username=%s depth=%d at=%s requests_since_last_rate_limit=%d consecutive_failures=%d account_retry_sleep=%s loop_sleep=%s next_retry_at=%s",
				item.Username,
				item.DiscoveryDepth,
				eventAt.Format(time.RFC3339),
				requestsSinceLastRateLimit,
				consecutiveFailures,
				accountRetrySleep,
				loopSleep,
				nextRetry.Format(time.RFC3339),
			)
			if err := st.MarkRateLimited(ctx, item.Username, eventAt, nextRetry, "rate_limit"); err != nil {
				if retryErr := handleTransientDBError(ctx, logger, "mark_rate_limited", item.Username, item.DiscoveryDepth, err); retryErr == nil {
					continue
				}
				return err
			}
			requestsSinceLastRateLimit = 0
			if err := sleepWithContext(ctx, loopSleep); err != nil {
				return err
			}
			continue
		}

		if resp.Data == nil {
			errKind := strings.TrimSpace(resp.Err)
			if strings.EqualFold(errKind, "not_found") {
				logger.Printf("skip not_found username=%s depth=%d", item.Username, item.DiscoveryDepth)
				if err := st.MarkTerminalSkip(ctx, item.Username, "not_found", "empty data err=not_found", time.Now()); err != nil {
					if retryErr := handleTransientDBError(ctx, logger, "mark_terminal_skip", item.Username, item.DiscoveryDepth, err); retryErr == nil {
						continue
					}
					return err
				}
				consecutiveFailures = 0
				continue
			}

			consecutiveFailures++
			retrySleep := nextFailureSleep(cfg.RateLimitSleep, cfg.FailureBackoffMultiplier, consecutiveFailures)
			nextRetry := time.Now().Add(retrySleep)
			logger.Printf(
				"empty data username=%s depth=%d err=%s consecutive_failures=%d retry_sleep=%s next_retry_at=%s",
				item.Username,
				item.DiscoveryDepth,
				resp.Err,
				consecutiveFailures,
				retrySleep,
				nextRetry.Format(time.RFC3339),
			)
			if err := st.MarkFailed(ctx, item.Username, time.Now(), nextRetry, fmt.Sprintf("empty data err=%s", resp.Err)); err != nil {
				if retryErr := handleTransientDBError(ctx, logger, "mark_failed_after_empty_data", item.Username, item.DiscoveryDepth, err); retryErr == nil {
					continue
				}
				return err
			}
			if err := sleepWithContext(ctx, retrySleep); err != nil {
				return err
			}
			continue
		}

		now = time.Now()
		if err := st.SaveFetchedAccount(ctx, item.Username, item.DiscoveryDepth, resp.Data, now); err != nil {
			if retryErr := handleTransientDBError(ctx, logger, "save_fetched_account", item.Username, item.DiscoveryDepth, err); retryErr == nil {
				continue
			}
			return err
		}
		rank := 0
		if resp.Data.Feature.Rank.KOLRank != nil {
			rank = *resp.Data.Feature.Rank.KOLRank
		}
		shouldExpandFollowers := rank == 0 || cfg.ExpandRankLimit <= 0 || rank <= cfg.ExpandRankLimit
		followersSaved := 0
		if shouldExpandFollowers {
			followersByBucket := resp.Data.FollowersByBucket()
			followersSaved = countFollowers(followersByBucket)
			if err := st.SaveFollowers(ctx, item.Username, item.DiscoveryDepth, followersByBucket, now); err != nil {
				if retryErr := handleTransientDBError(ctx, logger, "save_followers", item.Username, item.DiscoveryDepth, err); retryErr == nil {
					continue
				}
				return err
			}
		}
		if err := st.MarkFetchedSuccess(ctx, item.Username, now); err != nil {
			if retryErr := handleTransientDBError(ctx, logger, "mark_fetched_success", item.Username, item.DiscoveryDepth, err); retryErr == nil {
				continue
			}
			return err
		}
		consecutiveFailures = 0
		successCount++
		successCount1++
		var start time.Time
		var end time.Duration
		if successCount1 == 1 && successCount1 < cfg.SuccessCountEvery {
			start = time.Now()
		}
		if successCount1 == cfg.SuccessCountEvery {
			end = time.Since(start)
			if err := sleepWithContext(ctx, cfg.SuccessCooldownAllSleep-end); err != nil {
				return err
			}
		}

		logger.Printf(
			"fetch success username=%s depth=%d rank=%d followers_saved=%d followers(global=%d cn=%d top100=%d)",
			item.Username,
			item.DiscoveryDepth,
			rank,
			followersSaved,
			resp.Data.Feature.KOLFollowers.GlobalKOLFollowersCount,
			resp.Data.Feature.KOLFollowers.CNKOLFollowersCount,
			resp.Data.Feature.KOLFollowers.TopKOLFollowersCount,
		)
		if !shouldExpandFollowers {
			logger.Printf(
				"skip follower expansion username=%s depth=%d rank=%d expand_rank_limit=%d",
				item.Username,
				item.DiscoveryDepth,
				rank,
				cfg.ExpandRankLimit,
			)
		}

		if cfg.SuccessCooldownEvery > 0 && cfg.SuccessCooldownSleep > 0 && successCount%cfg.SuccessCooldownEvery == 0 {
			logger.Printf(
				"sleeping after success streak success_count=%d every=%d duration=%s",
				successCount,
				cfg.SuccessCooldownEvery,
				cfg.SuccessCooldownSleep,
			)
			if err := sleepWithContext(ctx, cfg.SuccessCooldownSleep); err != nil {
				return err
			}
		}
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

func countFollowers(buckets map[string][]model.Follower) int {
	total := 0
	for _, followers := range buckets {
		total += len(followers)
	}
	return total
}

func nextFailureSleep(base time.Duration, multiplier float64, consecutiveFailures int) time.Duration {
	if base <= 0 {
		return 0
	}
	if multiplier <= 1 || consecutiveFailures <= 1 {
		return base
	}

	wait := float64(base)
	maxDuration := float64(time.Duration(math.MaxInt64))
	for i := 1; i < consecutiveFailures; i++ {
		wait *= multiplier
		if wait >= maxDuration {
			return time.Duration(math.MaxInt64)
		}
	}

	return time.Duration(wait)
}

func shouldTriggerWiFiRecovery(cfg Config, consecutiveFailures int) bool {
	if cfg.WiFiRecoverAfterFailures <= 0 {
		return false
	}
	if consecutiveFailures < cfg.WiFiRecoverAfterFailures {
		return false
	}
	return strings.TrimSpace(cfg.WiFiRecoverMode) != ""
}

func tryWiFiRecovery(logger *log.Logger, cfg Config, consecutiveFailures int) bool {
	wifiCfg := wifi.Config{
		Service:      cfg.WiFiRecoverService,
		Mode:         cfg.WiFiRecoverMode,
		Device:       cfg.WiFiRecoverDevice,
		SSID:         cfg.WiFiRecoverSSID,
		Password:     cfg.WiFiRecoverPassword,
		FromSSID:     cfg.WiFiRecoverFromSSID,
		FromPassword: cfg.WiFiRecoverFromPassword,
		ToSSID:       cfg.WiFiRecoverToSSID,
		ToPassword:   cfg.WiFiRecoverToPassword,
		Wait:         cfg.WiFiRecoverWait,
	}

	steps, err := wifi.BuildSteps(wifiCfg)
	if err != nil {
		logger.Printf("wifi recovery config invalid consecutive_failures=%d err=%v", consecutiveFailures, err)
		return false
	}

	logger.Printf(
		"wifi recovery triggered consecutive_failures=%d mode=%s device=%s wait=%s steps=%d",
		consecutiveFailures,
		wifiCfg.Mode,
		wifiCfg.Device,
		wifiCfg.Wait,
		len(steps),
	)
	for idx, step := range steps {
		logger.Printf("wifi recovery step %d/%d %s", idx+1, len(steps), strings.Join(step, " "))
	}

	if err := wifi.Run(wifiCfg, os.Stdout, os.Stderr); err != nil {
		logger.Printf("wifi recovery failed consecutive_failures=%d err=%v", consecutiveFailures, err)
		return false
	}

	logger.Printf("wifi recovery completed consecutive_failures=%d", consecutiveFailures)
	return true
}
