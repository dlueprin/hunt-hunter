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
  ADD COLUMN user_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'Twitter用户ID' AFTER username;

ALTER TABLE kol_rankings CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE crawl_seen CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE crawl_edges CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
