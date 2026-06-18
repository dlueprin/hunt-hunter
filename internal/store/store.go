package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"xhunt-hunter/internal/model"
)

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
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
	return s.db.PingContext(ctx)
}

func (s *Store) Migrate(ctx context.Context) error {
	stmts := strings.Split(schemaSQL, ";\n")
	for _, stmt := range stmts {
		sqlText := strings.TrimSpace(stmt)
		if sqlText == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, sqlText); err != nil {
			return fmt.Errorf("migrate failed: %w, sql=%s", err, sqlText)
		}
	}
	return nil
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
	row := s.db.QueryRowContext(ctx, `
SELECT username, discovery_depth
FROM crawl_seen
WHERE is_fetched = 0
  AND discovery_depth < ?
  AND (next_retry_at IS NULL OR next_retry_at <= ?)
ORDER BY discovery_depth ASC, last_enqueued_at ASC, username ASC
LIMIT 1
`, maxDepth, now)

	var item model.PendingAccount
	if err := row.Scan(&item.Username, &item.DiscoveryDepth); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &item, nil
}

func (s *Store) MarkAttempt(ctx context.Context, username string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE crawl_seen
SET attempt_count = attempt_count + 1,
    last_attempt_at = ?,
    fetch_status = 'fetching',
    updated_at = ?
WHERE username = ?
`, now, now, username)
	return err
}

func (s *Store) MarkFetchedSuccess(ctx context.Context, username string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE crawl_seen
SET is_fetched = 1,
    fetch_status = 'success',
    last_success_at = ?,
    next_retry_at = NULL,
    last_error = '',
    updated_at = ?
WHERE username = ?
`, now, now, username)
	return err
}

func (s *Store) MarkRateLimited(ctx context.Context, username string, now, nextRetry time.Time, lastError string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE crawl_seen
SET fetch_status = 'rate_limited',
    rate_limit_count = rate_limit_count + 1,
    next_retry_at = ?,
    last_error = ?,
    updated_at = ?
WHERE username = ?
`, nextRetry, lastError, now, username)
	return err
}

func (s *Store) MarkFailed(ctx context.Context, username string, now, nextRetry time.Time, lastError string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE crawl_seen
SET fetch_status = 'failed',
    next_retry_at = ?,
    last_error = ?,
    updated_at = ?
WHERE username = ?
`, nextRetry, lastError, now, username)
	return err
}

func (s *Store) SaveFetchedAccount(ctx context.Context, username string, depth int, info *model.UserInfo, now time.Time) error {
	avatar := firstNonEmpty(strings.TrimSpace(info.Avatar), strings.TrimSpace(info.Profile.Avatar), strings.TrimSpace(info.Profile.ProfileImageURL))

	var createTime any
	if info.CreateTime != nil {
		createTime = *info.CreateTime
	}
	var globalRank any
	if info.Feature.Rank.KOLRank != nil {
		globalRank = *info.Feature.Rank.KOLRank
	}

	_, err := s.db.ExecContext(ctx, `
INSERT INTO kol_rankings (
  username, display_name, profile_url, avatar_url, bio, location, create_time,
  global_rank, classification, is_cn, followers_count, following_count, listed_count, tweets_count,
  global_kol_followers_count, cn_kol_followers_count, top_kol_followers_count,
  discovery_depth, discovered_by_count, first_discovered_at, last_discovered_at,
  last_fetched_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  display_name = VALUES(display_name),
  profile_url = VALUES(profile_url),
  avatar_url = VALUES(avatar_url),
  bio = VALUES(bio),
  location = VALUES(location),
  create_time = COALESCE(VALUES(create_time), create_time),
  global_rank = COALESCE(VALUES(global_rank), global_rank),
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
  last_fetched_at = VALUES(last_fetched_at),
  updated_at = VALUES(updated_at)
`, username, info.Name, fmt.Sprintf("https://x.com/%s", username), avatar, firstNonEmpty(info.Desc, info.Profile.Description),
		info.Profile.Location, createTime, globalRank, info.AI.Classification, info.AI.IsCN,
		info.Profile.FollowersCount, info.Profile.FollowingCount, info.Profile.ListedCount, info.Profile.TweetsCount,
		info.Feature.KOLFollowers.GlobalKOLFollowersCount, info.Feature.KOLFollowers.CNKOLFollowersCount,
		info.Feature.KOLFollowers.TopKOLFollowersCount, depth, now, now, now, now, now)
	return err
}

func (s *Store) SaveImportedTopRanking(ctx context.Context, row model.TopRankingRow, rankKind model.ImportedRankKind, now time.Time) error {
	username := normalizeUsername(firstNonEmpty(row.Username, row.Profile.Username))
	if username == "" {
		return nil
	}

	var createTime any
	if row.CreateTime != nil {
		createTime = *row.CreateTime
	}

	var globalRank any
	var cnRank any
	var enRank any
	if row.Rank != nil {
		switch rankKind {
		case model.ImportedRankKindGlobal:
			globalRank = *row.Rank
		case model.ImportedRankKindCN:
			cnRank = *row.Rank
		case model.ImportedRankKindEN:
			enRank = *row.Rank
		}
	}

	_, err := s.db.ExecContext(ctx, `
INSERT INTO kol_rankings (
  username, display_name, profile_url, avatar_url, bio, location, create_time,
  global_rank, cn_rank, en_rank, classification, is_cn, followers_count, following_count, listed_count, tweets_count,
  global_kol_followers_count, cn_kol_followers_count, top_kol_followers_count,
  discovery_depth, discovered_by_count, first_discovered_at, last_discovered_at,
  last_fetched_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, ?, ?, NULL, ?, ?)
ON DUPLICATE KEY UPDATE
  display_name = VALUES(display_name),
  profile_url = VALUES(profile_url),
  avatar_url = VALUES(avatar_url),
  bio = VALUES(bio),
  location = VALUES(location),
  create_time = COALESCE(VALUES(create_time), create_time),
  global_rank = COALESCE(VALUES(global_rank), global_rank),
  cn_rank = COALESCE(VALUES(cn_rank), cn_rank),
  en_rank = COALESCE(VALUES(en_rank), en_rank),
  classification = VALUES(classification),
  is_cn = VALUES(is_cn),
  followers_count = VALUES(followers_count),
  following_count = VALUES(following_count),
  listed_count = VALUES(listed_count),
  tweets_count = VALUES(tweets_count),
  updated_at = VALUES(updated_at)
`, username, firstNonEmpty(row.Name, row.Profile.Name), fmt.Sprintf("https://x.com/%s", username),
		row.Profile.ProfileImageURL, row.Profile.Description, row.Profile.Location, createTime,
		globalRank, cnRank, enRank, row.AI.Classification, row.AI.IsCN,
		row.Profile.FollowersCount, row.Profile.FollowingCount, row.Profile.ListedCount, row.Profile.TweetsCount,
		now, now, now, now)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `
INSERT INTO crawl_seen (
  username, discovery_depth, is_enqueued, is_fetched, fetch_status,
  attempt_count, rate_limit_count, first_enqueued_at, last_enqueued_at, created_at, updated_at
) VALUES (?, 0, 1, 0, 'imported', 0, 0, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  is_enqueued = 1,
  updated_at = VALUES(updated_at),
  fetch_status = CASE
    WHEN is_fetched = 1 THEN fetch_status
    ELSE 'imported'
  END
`, username, now, now, now, now)
	return err
}

func (s *Store) SaveFollowers(ctx context.Context, sourceUsername string, sourceDepth int, buckets map[string][]model.Follower, now time.Time) error {
	for bucket, followers := range buckets {
		for _, follower := range followers {
			username := normalizeUsername(follower.Username)
			if username == "" || username == normalizeUsername(sourceUsername) {
				continue
			}
			if err := s.upsertDiscoveredAccount(ctx, username, follower.Name, follower.Avatar, sourceDepth+1, now); err != nil {
				return err
			}
			inserted, err := s.insertEdge(ctx, sourceUsername, username, bucket, sourceDepth+1, now)
			if err != nil {
				return err
			}
			if inserted {
				if err := s.incrementDiscoveredByCount(ctx, username, sourceDepth+1, now); err != nil {
					return err
				}
			}
			if err := s.upsertSeen(ctx, username, sourceDepth+1, now); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) upsertDiscoveredAccount(ctx context.Context, username, displayName, avatarURL string, depth int, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO kol_rankings (
  username, display_name, profile_url, avatar_url, discovery_depth, discovered_by_count,
  first_discovered_at, last_discovered_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
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
  last_discovered_at = GREATEST(last_discovered_at, VALUES(last_discovered_at)),
  updated_at = VALUES(updated_at)
`, username, displayName, fmt.Sprintf("https://x.com/%s", username), avatarURL, depth, now, now, now, now)
	return err
}

func (s *Store) upsertSeen(ctx context.Context, username string, depth int, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO crawl_seen (
  username, discovery_depth, is_enqueued, is_fetched, fetch_status,
  attempt_count, rate_limit_count, first_enqueued_at, last_enqueued_at, created_at, updated_at
) VALUES (?, ?, 1, 0, 'pending', 0, 0, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  discovery_depth = LEAST(discovery_depth, VALUES(discovery_depth)),
  is_enqueued = 1,
  last_enqueued_at = VALUES(last_enqueued_at),
  updated_at = VALUES(updated_at)
`, username, depth, now, now, now, now)
	return err
}

func (s *Store) insertEdge(ctx context.Context, sourceUsername, targetUsername, bucket string, depth int, now time.Time) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
INSERT IGNORE INTO crawl_edges (
  source_username, target_username, source_bucket, discovery_depth, discovered_at, created_at
) VALUES (?, ?, ?, ?, ?, ?)
`, normalizeUsername(sourceUsername), normalizeUsername(targetUsername), bucket, depth, now, now)
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
	_, err := s.db.ExecContext(ctx, `
UPDATE kol_rankings
SET discovered_by_count = discovered_by_count + 1,
    discovery_depth = LEAST(discovery_depth, ?),
    last_discovered_at = ?,
    updated_at = ?
WHERE username = ?
`, depth, now, now, username)
	return err
}

func normalizeUsername(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "@")
	return strings.ToLower(v)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS kol_rankings (
  username VARCHAR(64) NOT NULL COMMENT '账号唯一标识，统一转成小写 username' PRIMARY KEY,
  display_name VARCHAR(255) NOT NULL DEFAULT '' COMMENT '账号显示名',
  profile_url VARCHAR(1024) NOT NULL DEFAULT '' COMMENT 'X 个人主页 URL',
  avatar_url VARCHAR(1024) NOT NULL DEFAULT '' COMMENT '头像 URL',
  bio TEXT NULL COMMENT '账号简介',
  location VARCHAR(255) NOT NULL DEFAULT '' COMMENT '账号 location',
  create_time DATETIME NULL COMMENT 'X 账号创建时间',
  global_rank INT NULL COMMENT 'XHunt 全局真实排名，仅全局榜单或主动请求详情时补全',
  cn_rank INT NULL COMMENT 'XHunt 华语区榜单排名',
  en_rank INT NULL COMMENT 'XHunt 英文区榜单排名',
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
  first_discovered_at DATETIME NOT NULL COMMENT '首次被发现时间',
  last_discovered_at DATETIME NOT NULL COMMENT '最近一次被发现时间',
  last_fetched_at DATETIME NULL COMMENT '最近一次成功主动请求该账号详情的时间',
  created_at DATETIME NOT NULL COMMENT '记录创建时间',
  updated_at DATETIME NOT NULL COMMENT '记录更新时间',
  KEY idx_global_rank (global_rank),
  KEY idx_last_fetched_at (last_fetched_at),
  KEY idx_discovery_depth (discovery_depth)
) COMMENT='给其他项目读取的 KOL 主表，一账号一行';

CREATE TABLE IF NOT EXISTS crawl_seen (
  username VARCHAR(64) NOT NULL COMMENT '账号唯一标识，统一转成小写 username' PRIMARY KEY,
  discovery_depth INT NOT NULL DEFAULT 0 COMMENT '当前待抓取状态对应的最小发现层数',
  is_enqueued TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否已进入采集队列',
  is_fetched TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否已经主动请求过该账号详情',
  fetch_status VARCHAR(32) NOT NULL DEFAULT 'pending' COMMENT '采集状态，如 pending/imported/fetching/success/rate_limited/failed',
  attempt_count INT NOT NULL DEFAULT 0 COMMENT '主动请求该账号的总尝试次数',
  rate_limit_count INT NOT NULL DEFAULT 0 COMMENT '该账号触发 rate_limit 的次数',
  last_attempt_at DATETIME NULL COMMENT '最近一次请求时间',
  last_success_at DATETIME NULL COMMENT '最近一次成功抓取时间',
  next_retry_at DATETIME NULL COMMENT '下次允许重试的时间，用于断点续跑和限流等待',
  last_error TEXT NULL COMMENT '最近一次错误信息',
  first_enqueued_at DATETIME NOT NULL COMMENT '首次进入采集队列时间',
  last_enqueued_at DATETIME NOT NULL COMMENT '最近一次进入采集队列时间',
  created_at DATETIME NOT NULL COMMENT '记录创建时间',
  updated_at DATETIME NOT NULL COMMENT '记录更新时间',
  KEY idx_pending (is_fetched, discovery_depth, next_retry_at),
  KEY idx_status (fetch_status)
) COMMENT='采集器内部状态表，用于断点续跑和限流恢复';

CREATE TABLE IF NOT EXISTS crawl_edges (
  id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键' PRIMARY KEY,
  source_username VARCHAR(64) NOT NULL COMMENT '发现者账号 username',
  target_username VARCHAR(64) NOT NULL COMMENT '被发现账号 username',
  source_bucket VARCHAR(32) NOT NULL COMMENT '来源桶，global/cn/top100',
  discovery_depth INT NOT NULL COMMENT 'target 被发现时所在层数',
  discovered_at DATETIME NOT NULL COMMENT '这条发现关系首次建立时间',
  created_at DATETIME NOT NULL COMMENT '记录创建时间',
  UNIQUE KEY uniq_source_target (source_username, target_username),
  KEY idx_target (target_username),
  KEY idx_source (source_username)
) COMMENT='账号发现关系表，记录谁把谁带进 BFS';
`
