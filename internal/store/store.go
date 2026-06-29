package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"xhunt-hunter/internal/model"
)

type Store struct {
	db *sql.DB
}

const (
	dbRetryMaxAttempts  = 3
	dbRetryInitialDelay = 500 * time.Millisecond
	dbRetryMaxDelay     = 2 * time.Second
)

func Open(dsn string) (*Store, error) {
	parsedDSN, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if parsedDSN.Timeout <= 0 {
		parsedDSN.Timeout = 5 * time.Second
	}
	if parsedDSN.ReadTimeout <= 0 {
		parsedDSN.ReadTimeout = 10 * time.Second
	}
	if parsedDSN.WriteTimeout <= 0 {
		parsedDSN.WriteTimeout = 10 * time.Second
	}

	dsn = parsedDSN.FormatDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return withDBRetry(ctx, func() error {
		return s.db.PingContext(ctx)
	})
}

func (s *Store) Migrate(ctx context.Context) error {
	stmts := strings.Split(schemaSQL, ";\n")
	for _, stmt := range stmts {
		sqlText := strings.TrimSpace(stmt)
		if sqlText == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, sqlText); err != nil {
			if shouldIgnoreMigrationError(sqlText, err) {
				continue
			}
			return fmt.Errorf("migrate failed: %w, sql=%s", err, sqlText)
		}
	}
	return nil
}

func shouldIgnoreMigrationError(sqlText string, err error) bool {
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	if mysqlErr.Number == 1060 && strings.Contains(sqlText, "ADD COLUMN user_id") {
		return true
	}

	return false
}

func (s *Store) SeedAccounts(ctx context.Context, seeds []string) error {
	now := time.Now()
	for _, raw := range seeds {
		username := normalizeUsername(raw)
		if username == "" {
			continue
		}
		if err := s.upsertDiscoveredAccount(ctx, username, username, "", 0, now); err != nil {
			return err
		}
		if err := s.upsertSeen(ctx, username, 0, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) NextPendingAccount(ctx context.Context, maxDepth int, now time.Time) (*model.PendingAccount, error) {
	var item model.PendingAccount
	err := withDBRetry(ctx, func() error {
		row := s.db.QueryRowContext(ctx, `
SELECT username, discovery_depth
FROM crawl_seen
WHERE is_fetched = 0
  AND discovery_depth < ?
  AND fetch_status <> 'rate_limited'
  AND (next_retry_at = 0 OR next_retry_at <= ?)
ORDER BY discovery_depth ASC, last_enqueued_at ASC, username ASC
LIMIT 1
`, maxDepth, now.Unix())
		return row.Scan(&item.Username, &item.DiscoveryDepth)
	})
	if err == sql.ErrNoRows {
		err = withDBRetry(ctx, func() error {
			row := s.db.QueryRowContext(ctx, `
SELECT username, discovery_depth
FROM crawl_seen
WHERE is_fetched = 0
  AND discovery_depth < ?
  AND fetch_status = 'rate_limited'
ORDER BY last_attempt_at ASC, rate_limit_count ASC, username ASC
LIMIT 1
`, maxDepth)
			return row.Scan(&item.Username, &item.DiscoveryDepth)
		})
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &item, nil
}

func (s *Store) RequeueSuccessfulAccountsForReplay(ctx context.Context, depths []int, limit int, now time.Time) (int64, error) {
	depths = normalizeDepths(depths)
	if len(depths) == 0 {
		return 0, nil
	}

	args := make([]any, 0, len(depths)+2)
	args = append(args, now.Unix())
	placeholders := make([]string, 0, len(depths))
	for _, depth := range depths {
		placeholders = append(placeholders, "?")
		args = append(args, depth)
	}

	query := fmt.Sprintf(`
UPDATE crawl_seen
SET is_fetched = 0,
    fetch_status = 'pending',
    next_retry_at = 0,
    last_error = '',
    last_enqueued_at = ?
WHERE is_fetched = 1
  AND fetch_status = 'success'
  AND discovery_depth IN (%s)
ORDER BY discovery_depth ASC, last_success_at ASC, username ASC
`, strings.Join(placeholders, ","))
	if limit > 0 {
		query += "LIMIT ?"
		args = append(args, limit)
	}

	result, err := execContextWithRetry(ctx, s.db, query, args...)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

func (s *Store) MarkAttempt(ctx context.Context, username string, now time.Time) error {
	_, err := execContextWithRetry(ctx, s.db, `
UPDATE crawl_seen
SET attempt_count = attempt_count + 1,
    last_attempt_at = ?,
    fetch_status = 'fetching'
WHERE username = ?
`, now.Unix(), username)
	return err
}

func (s *Store) MarkFetchedSuccess(ctx context.Context, username string, now time.Time) error {
	_, err := execContextWithRetry(ctx, s.db, `
UPDATE crawl_seen
SET is_fetched = 1,
    fetch_status = 'success',
    last_success_at = ?,
    next_retry_at = 0,
    last_error = ''
WHERE username = ?
`, now.Unix(), username)
	return err
}

func (s *Store) MarkRateLimited(ctx context.Context, username string, now, nextRetry time.Time, lastError string) error {
	_, err := execContextWithRetry(ctx, s.db, `
UPDATE crawl_seen
SET fetch_status = 'rate_limited',
    rate_limit_count = rate_limit_count + 1,
    next_retry_at = ?,
    last_error = ?
WHERE username = ?
`, nextRetry.Unix(), lastError, username)
	return err
}

func (s *Store) MarkFailed(ctx context.Context, username string, now, nextRetry time.Time, lastError string) error {
	_, err := execContextWithRetry(ctx, s.db, `
UPDATE crawl_seen
SET fetch_status = 'failed',
    next_retry_at = ?,
    last_error = ?
WHERE username = ?
`, nextRetry.Unix(), lastError, username)
	return err
}

func (s *Store) MarkTerminalSkip(ctx context.Context, username, status, lastError string, now time.Time) error {
	_, err := execContextWithRetry(ctx, s.db, `
UPDATE crawl_seen
SET is_fetched = 1,
    fetch_status = ?,
    last_success_at = ?,
    next_retry_at = 0,
    last_error = ?
WHERE username = ?
`, status, now.Unix(), lastError, username)
	return err
}

func (s *Store) SaveFetchedAccount(ctx context.Context, username string, depth int, info *model.UserInfo, now time.Time) error {
	avatar := firstNonEmpty(strings.TrimSpace(info.Avatar), strings.TrimSpace(info.Profile.Avatar), strings.TrimSpace(info.Profile.ProfileImageURL))

	accountCreatedAt := unixSec(info.CreateTime)
	globalRank := 0
	if info.Feature.Rank.KOLRank != nil {
		globalRank = *info.Feature.Rank.KOLRank
	}

	_, err := execContextWithRetry(ctx, s.db, `
INSERT INTO kol_rankings (
  username, user_id, display_name, profile_url, avatar_url, bio, location, account_created_at,
  global_rank, classification, is_cn, followers_count, following_count, listed_count, tweets_count,
  global_kol_followers_count, cn_kol_followers_count, top_kol_followers_count,
  discovery_depth, discovered_by_count, first_discovered_at, last_discovered_at, last_fetched_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  user_id = CASE
    WHEN VALUES(user_id) <> '' THEN VALUES(user_id)
    ELSE user_id
  END,
  display_name = VALUES(display_name),
  profile_url = VALUES(profile_url),
  avatar_url = VALUES(avatar_url),
  bio = VALUES(bio),
  location = VALUES(location),
  account_created_at = IF(VALUES(account_created_at) > 0, VALUES(account_created_at), account_created_at),
  global_rank = IF(VALUES(global_rank) > 0, VALUES(global_rank), global_rank),
  classification = VALUES(classification),
  is_cn = VALUES(is_cn),
  followers_count = VALUES(followers_count),
  following_count = VALUES(following_count),
  listed_count = VALUES(listed_count),
  tweets_count = VALUES(tweets_count),
  global_kol_followers_count = VALUES(global_kol_followers_count),
  cn_kol_followers_count = VALUES(cn_kol_followers_count),
  top_kol_followers_count = VALUES(top_kol_followers_count),
  discovery_depth = LEAST(discovery_depth, VALUES(discovery_depth)),
  last_discovered_at = GREATEST(last_discovered_at, VALUES(last_discovered_at)),
  last_fetched_at = VALUES(last_fetched_at)
	`, username, strings.TrimSpace(info.ID), info.Name, fmt.Sprintf("https://x.com/%s", username), avatar, firstNonEmpty(info.Desc, info.Profile.Description),
		info.Profile.Location, accountCreatedAt, globalRank, info.AI.Classification, info.AI.IsCN,
		info.Profile.FollowersCount, info.Profile.FollowingCount, info.Profile.ListedCount, info.Profile.TweetsCount,
		info.Feature.KOLFollowers.GlobalKOLFollowersCount, info.Feature.KOLFollowers.CNKOLFollowersCount,
		info.Feature.KOLFollowers.TopKOLFollowersCount, depth, now.Unix(), now.Unix(), now.Unix())
	return err
}

func (s *Store) SaveImportedSeed(ctx context.Context, row model.TopRankingRow, now time.Time) error {
	username := normalizeUsername(firstNonEmpty(row.Username, row.Profile.Username))
	if username == "" {
		return nil
	}
	_, err := execContextWithRetry(ctx, s.db, `
INSERT INTO crawl_seen (
  username, discovery_depth, is_enqueued, is_fetched, fetch_status,
  attempt_count, rate_limit_count, first_enqueued_at, last_enqueued_at
) VALUES (?, 0, 1, 0, 'pending', 0, 0, ?, ?)
ON DUPLICATE KEY UPDATE
  discovery_depth = LEAST(discovery_depth, VALUES(discovery_depth)),
  is_enqueued = 1,
  last_enqueued_at = VALUES(last_enqueued_at)
`, username, now.Unix(), now.Unix())
	return err
}

func (s *Store) SaveFollowers(ctx context.Context, sourceUsername string, sourceDepth int, buckets map[string][]model.Follower, now time.Time) error {
	targets := collectFollowerTargets(sourceUsername, buckets)
	if len(targets) == 0 {
		return nil
	}

	return withDBRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() {
			_ = tx.Rollback()
		}()

		newEdgeTargets, err := findNewEdgeTargets(ctx, tx, normalizeUsername(sourceUsername), targets)
		if err != nil {
			return err
		}
		if err := batchUpsertDiscoveredAccounts(ctx, tx, targets, sourceDepth+1, now); err != nil {
			return err
		}
		if err := batchInsertEdges(ctx, tx, normalizeUsername(sourceUsername), targets, sourceDepth+1, now); err != nil {
			return err
		}
		if err := batchIncrementDiscoveredByCount(ctx, tx, newEdgeTargets, sourceDepth+1, now); err != nil {
			return err
		}
		if err := batchUpsertSeen(ctx, tx, targets, sourceDepth+1, now); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	})
}

type followerTarget struct {
	Username    string
	DisplayName string
	AvatarURL   string
	Bucket      string
}

func collectFollowerTargets(sourceUsername string, buckets map[string][]model.Follower) []followerTarget {
	sourceUsername = normalizeUsername(sourceUsername)
	seen := make(map[string]struct{})
	result := make([]followerTarget, 0)

	for _, bucket := range []string{"global", "cn", "top100"} {
		followers := buckets[bucket]
		for _, follower := range followers {
			username := normalizeUsername(follower.Username)
			if username == "" || username == sourceUsername {
				continue
			}
			if _, ok := seen[username]; ok {
				continue
			}
			seen[username] = struct{}{}
			result = append(result, followerTarget{
				Username:    username,
				DisplayName: strings.TrimSpace(follower.Name),
				AvatarURL:   strings.TrimSpace(follower.Avatar),
				Bucket:      bucket,
			})
		}
	}

	return result
}

func findNewEdgeTargets(ctx context.Context, tx *sql.Tx, sourceUsername string, targets []followerTarget) (map[string]struct{}, error) {
	args := make([]any, 0, len(targets)+1)
	args = append(args, sourceUsername)
	placeholders := make([]string, 0, len(targets))
	for _, target := range targets {
		placeholders = append(placeholders, "?")
		args = append(args, target.Username)
	}

	query := fmt.Sprintf(`
SELECT target_username
FROM crawl_edges
WHERE source_username = ?
  AND target_username IN (%s)
`, strings.Join(placeholders, ","))
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	existing := make(map[string]struct{}, len(targets))
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			return nil, err
		}
		existing[normalizeUsername(username)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if _, ok := existing[target.Username]; ok {
			continue
		}
		result[target.Username] = struct{}{}
	}
	return result, nil
}

func batchUpsertDiscoveredAccounts(ctx context.Context, tx *sql.Tx, targets []followerTarget, depth int, now time.Time) error {
	args := make([]any, 0, len(targets)*7)
	values := make([]string, 0, len(targets))
	for _, target := range targets {
		values = append(values, "(?, ?, ?, ?, ?, 0, ?, ?)")
		args = append(args,
			target.Username,
			target.DisplayName,
			fmt.Sprintf("https://x.com/%s", target.Username),
			target.AvatarURL,
			depth,
			now.Unix(),
			now.Unix(),
		)
	}

	query := fmt.Sprintf(`
INSERT INTO kol_rankings (
  username, display_name, profile_url, avatar_url, discovery_depth, discovered_by_count,
  first_discovered_at, last_discovered_at
) VALUES %s
ON DUPLICATE KEY UPDATE
  display_name = CASE
    WHEN (display_name = '' OR display_name IS NULL) AND VALUES(display_name) <> '' THEN VALUES(display_name)
    ELSE display_name
  END,
  profile_url = CASE
    WHEN (profile_url = '' OR profile_url IS NULL) AND VALUES(profile_url) <> '' THEN VALUES(profile_url)
    ELSE profile_url
  END,
  avatar_url = CASE
    WHEN (avatar_url = '' OR avatar_url IS NULL) AND VALUES(avatar_url) <> '' THEN VALUES(avatar_url)
    ELSE avatar_url
  END,
  discovery_depth = LEAST(discovery_depth, VALUES(discovery_depth)),
  last_discovered_at = GREATEST(last_discovered_at, VALUES(last_discovered_at))
`, strings.Join(values, ","))

	_, err := txExecContextWithRetry(ctx, tx, query, args...)
	return err
}

func batchInsertEdges(ctx context.Context, tx *sql.Tx, sourceUsername string, targets []followerTarget, depth int, now time.Time) error {
	args := make([]any, 0, len(targets)*5)
	values := make([]string, 0, len(targets))
	for _, target := range targets {
		values = append(values, "(?, ?, ?, ?, ?)")
		args = append(args, sourceUsername, target.Username, target.Bucket, depth, now.Unix())
	}

	query := fmt.Sprintf(`
INSERT IGNORE INTO crawl_edges (
  source_username, target_username, source_bucket, discovery_depth, discovered_at
) VALUES %s
`, strings.Join(values, ","))

	_, err := txExecContextWithRetry(ctx, tx, query, args...)
	return err
}

func batchIncrementDiscoveredByCount(ctx context.Context, tx *sql.Tx, targets map[string]struct{}, depth int, now time.Time) error {
	if len(targets) == 0 {
		return nil
	}

	args := make([]any, 0, len(targets)+2)
	args = append(args, depth, now.Unix())
	placeholders := make([]string, 0, len(targets))
	for username := range targets {
		placeholders = append(placeholders, "?")
		args = append(args, username)
	}

	query := fmt.Sprintf(`
UPDATE kol_rankings
SET discovered_by_count = discovered_by_count + 1,
    discovery_depth = LEAST(discovery_depth, ?),
    last_discovered_at = ?
WHERE username IN (%s)
`, strings.Join(placeholders, ","))

	_, err := txExecContextWithRetry(ctx, tx, query, args...)
	return err
}

func batchUpsertSeen(ctx context.Context, tx *sql.Tx, targets []followerTarget, depth int, now time.Time) error {
	args := make([]any, 0, len(targets)*4)
	values := make([]string, 0, len(targets))
	for _, target := range targets {
		values = append(values, "(?, ?, 1, 0, 'pending', 0, 0, ?, ?)")
		args = append(args, target.Username, depth, now.Unix(), now.Unix())
	}

	query := fmt.Sprintf(`
INSERT INTO crawl_seen (
  username, discovery_depth, is_enqueued, is_fetched, fetch_status,
  attempt_count, rate_limit_count, first_enqueued_at, last_enqueued_at
) VALUES %s
ON DUPLICATE KEY UPDATE
  discovery_depth = LEAST(discovery_depth, VALUES(discovery_depth)),
  is_enqueued = 1,
  last_enqueued_at = VALUES(last_enqueued_at)
`, strings.Join(values, ","))

	_, err := txExecContextWithRetry(ctx, tx, query, args...)
	return err
}

func (s *Store) upsertDiscoveredAccount(ctx context.Context, username, displayName, avatarURL string, depth int, now time.Time) error {
	_, err := execContextWithRetry(ctx, s.db, `
INSERT INTO kol_rankings (
  username, display_name, profile_url, avatar_url, discovery_depth, discovered_by_count,
  first_discovered_at, last_discovered_at
) VALUES (?, ?, ?, ?, ?, 0, ?, ?)
ON DUPLICATE KEY UPDATE
  display_name = CASE
    WHEN (display_name = '' OR display_name IS NULL) AND VALUES(display_name) <> '' THEN VALUES(display_name)
    ELSE display_name
  END,
  profile_url = CASE
    WHEN (profile_url = '' OR profile_url IS NULL) AND VALUES(profile_url) <> '' THEN VALUES(profile_url)
    ELSE profile_url
  END,
  avatar_url = CASE
    WHEN (avatar_url = '' OR avatar_url IS NULL) AND VALUES(avatar_url) <> '' THEN VALUES(avatar_url)
    ELSE avatar_url
  END,
  discovery_depth = LEAST(discovery_depth, VALUES(discovery_depth)),
  last_discovered_at = GREATEST(last_discovered_at, VALUES(last_discovered_at))
`, username, displayName, fmt.Sprintf("https://x.com/%s", username), avatarURL, depth, now.Unix(), now.Unix())
	return err
}

func (s *Store) upsertSeen(ctx context.Context, username string, depth int, now time.Time) error {
	_, err := execContextWithRetry(ctx, s.db, `
INSERT INTO crawl_seen (
  username, discovery_depth, is_enqueued, is_fetched, fetch_status,
  attempt_count, rate_limit_count, first_enqueued_at, last_enqueued_at
) VALUES (?, ?, 1, 0, 'pending', 0, 0, ?, ?)
ON DUPLICATE KEY UPDATE
  discovery_depth = LEAST(discovery_depth, VALUES(discovery_depth)),
  is_enqueued = 1,
  last_enqueued_at = VALUES(last_enqueued_at)
`, username, depth, now.Unix(), now.Unix())
	return err
}

func (s *Store) insertEdge(ctx context.Context, sourceUsername, targetUsername, bucket string, depth int, now time.Time) (bool, error) {
	result, err := execContextWithRetry(ctx, s.db, `
INSERT IGNORE INTO crawl_edges (
  source_username, target_username, source_bucket, discovery_depth, discovered_at
) VALUES (?, ?, ?, ?, ?)
`, normalizeUsername(sourceUsername), normalizeUsername(targetUsername), bucket, depth, now.Unix())
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *Store) incrementDiscoveredByCount(ctx context.Context, username string, depth int, now time.Time) error {
	_, err := execContextWithRetry(ctx, s.db, `
UPDATE kol_rankings
SET discovered_by_count = discovered_by_count + 1,
    discovery_depth = LEAST(discovery_depth, ?),
    last_discovered_at = ?
WHERE username = ?
`, depth, now.Unix(), username)
	return err
}

func normalizeUsername(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "@")
	return strings.ToLower(v)
}

func execContextWithRetry(ctx context.Context, execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, query string, args ...any) (sql.Result, error) {
	var result sql.Result
	err := withDBRetry(ctx, func() error {
		var err error
		result, err = execer.ExecContext(ctx, query, args...)
		return err
	})
	return result, err
}

func txExecContextWithRetry(ctx context.Context, tx *sql.Tx, query string, args ...any) (sql.Result, error) {
	return execContextWithRetry(ctx, tx, query, args...)
}

func withDBRetry(ctx context.Context, fn func() error) error {
	delay := dbRetryInitialDelay
	var lastErr error
	for attempt := 1; attempt <= dbRetryMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryableDBError(err) || attempt == dbRetryMaxAttempts {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		delay *= 2
		if delay > dbRetryMaxDelay {
			delay = dbRetryMaxDelay
		}
	}
	return lastErr
}

func isRetryableDBError(err error) bool {
	if err == nil {
		return false
	}
	if err == driver.ErrBadConn || err == sql.ErrConnDone {
		return true
	}
	var netErr net.Error
	if ok := errors.As(err, &netErr); ok {
		return true
	}
	var mysqlErr *mysql.MySQLError
	if ok := errors.As(err, &mysqlErr); ok {
		switch mysqlErr.Number {
		case 1040, 1042, 1047, 1158, 1159, 1160, 1161, 1184, 1205, 1213, 1317, 2002, 2003, 2006, 2013:
			return true
		}
	}

	message := strings.ToLower(err.Error())
	for _, keyword := range []string{
		"server has gone away",
		"bad connection",
		"connection refused",
		"connection reset by peer",
		"broken pipe",
		"invalid connection",
		"unexpected eof",
		"i/o timeout",
		"timeout",
		"context deadline exceeded",
	} {
		if strings.Contains(message, keyword) {
			return true
		}
	}

	return false
}

func IsRetryableDBError(err error) bool {
	return isRetryableDBError(err)
}

func normalizeDepths(depths []int) []int {
	if len(depths) == 0 {
		return nil
	}

	seen := make(map[int]struct{}, len(depths))
	result := make([]int, 0, len(depths))
	for _, depth := range depths {
		if depth < 0 {
			continue
		}
		if _, ok := seen[depth]; ok {
			continue
		}
		seen[depth] = struct{}{}
		result = append(result, depth)
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func unixSec(value *time.Time) int64 {
	if value == nil {
		return 0
	}
	return value.Unix()
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS kol_rankings (
  id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键' PRIMARY KEY,
  username VARCHAR(64) NOT NULL DEFAULT '' COMMENT '账号唯一标识，统一转成小写 username',
  user_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'Twitter用户ID',
  display_name VARCHAR(255) NOT NULL DEFAULT '' COMMENT '账号显示名',
  profile_url VARCHAR(1024) NOT NULL DEFAULT '' COMMENT 'X 个人主页 URL',
  avatar_url VARCHAR(1024) NOT NULL DEFAULT '' COMMENT '头像 URL',
  bio TEXT NULL COMMENT '账号简介',
  location VARCHAR(255) NOT NULL DEFAULT '' COMMENT '账号 location',
  account_created_at BIGINT NOT NULL DEFAULT 0 COMMENT 'X 账号创建时间，Unix 秒',
  global_rank INT NOT NULL DEFAULT 0 COMMENT 'XHunt 全局真实排名，仅全局榜单或主动请求详情时补全，0 表示未知',
  classification VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'XHunt AI 分类，如 person/project/media',
  is_cn TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否为中文区账号',
  followers_count BIGINT NOT NULL DEFAULT 0 COMMENT 'X 粉丝数',
  following_count BIGINT NOT NULL DEFAULT 0 COMMENT 'X 关注数',
  listed_count BIGINT NOT NULL DEFAULT 0 COMMENT 'X 被加入列表数',
  tweets_count BIGINT NOT NULL DEFAULT 0 COMMENT 'X 发帖数',
  global_kol_followers_count INT NOT NULL DEFAULT 0 COMMENT '全球 KOL 粉丝数量',
  cn_kol_followers_count INT NOT NULL DEFAULT 0 COMMENT '华语区 KOL 粉丝数量',
  top_kol_followers_count INT NOT NULL DEFAULT 0 COMMENT 'Top100 KOL 粉丝数量',
  discovery_depth INT NOT NULL DEFAULT 0 COMMENT '首次被发现的最小层数，seed 为 0',
  discovered_by_count INT NOT NULL DEFAULT 0 COMMENT '被多少个上游账号发现过',
  first_discovered_at BIGINT NOT NULL DEFAULT 0 COMMENT '首次被发现时间，Unix 秒',
  last_discovered_at BIGINT NOT NULL DEFAULT 0 COMMENT '最近一次被发现时间，Unix 秒',
  last_fetched_at BIGINT NOT NULL DEFAULT 0 COMMENT '最近一次成功主动请求该账号详情的时间，Unix 秒',
  ctime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  mtime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  UNIQUE KEY uniq_username (username),
  KEY idx_global_rank (global_rank),
  KEY idx_last_fetched_at (last_fetched_at),
  KEY idx_discovery_depth (discovery_depth)
) COMMENT='给其他项目读取的 KOL 主表，一账号一行' DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS crawl_seen (
  id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键' PRIMARY KEY,
  username VARCHAR(64) NOT NULL DEFAULT '' COMMENT '账号唯一标识，统一转成小写 username',
  discovery_depth INT NOT NULL DEFAULT 0 COMMENT '当前待抓取状态对应的最小发现层数',
  is_enqueued TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否已进入采集队列',
  is_fetched TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否已经主动请求过该账号详情',
  fetch_status VARCHAR(32) NOT NULL DEFAULT 'pending' COMMENT '采集状态，如 pending/fetching/success/rate_limited/failed',
  attempt_count INT NOT NULL DEFAULT 0 COMMENT '主动请求该账号的总尝试次数',
  rate_limit_count INT NOT NULL DEFAULT 0 COMMENT '该账号触发 rate_limit 的次数',
  last_attempt_at BIGINT NOT NULL DEFAULT 0 COMMENT '最近一次请求时间，Unix 秒',
  last_success_at BIGINT NOT NULL DEFAULT 0 COMMENT '最近一次成功抓取时间，Unix 秒',
  next_retry_at BIGINT NOT NULL DEFAULT 0 COMMENT '下次允许重试的时间，Unix 秒',
  last_error TEXT NULL COMMENT '最近一次错误信息',
  first_enqueued_at BIGINT NOT NULL DEFAULT 0 COMMENT '首次进入采集队列时间，Unix 秒',
  last_enqueued_at BIGINT NOT NULL DEFAULT 0 COMMENT '最近一次进入采集队列时间，Unix 秒',
  ctime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  mtime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  UNIQUE KEY uniq_username (username),
  KEY idx_pending (is_fetched, discovery_depth, next_retry_at),
  KEY idx_status (fetch_status)
) COMMENT='采集器内部状态表，用于断点续跑和限流恢复' DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS crawl_edges (
  id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键' PRIMARY KEY,
  source_username VARCHAR(64) NOT NULL DEFAULT '' COMMENT '发现者账号 username',
  target_username VARCHAR(64) NOT NULL DEFAULT '' COMMENT '被发现账号 username',
  source_bucket VARCHAR(32) NOT NULL DEFAULT '' COMMENT '来源桶，global/cn/top100',
  discovery_depth INT NOT NULL DEFAULT 0 COMMENT 'target 被发现时所在层数',
  discovered_at BIGINT NOT NULL DEFAULT 0 COMMENT '这条发现关系首次建立时间，Unix 秒',
  ctime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  mtime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  UNIQUE KEY uniq_source_target (source_username, target_username),
  KEY idx_target (target_username),
  KEY idx_source (source_username)
) COMMENT='账号发现关系表，记录谁把谁带进 BFS' DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE kol_rankings
  ADD COLUMN user_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'X/Twitter 用户 UID，来自详情接口 data.id' AFTER username;

ALTER TABLE kol_rankings CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE crawl_seen CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE crawl_edges CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

`
