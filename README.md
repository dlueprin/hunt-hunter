# xhunt-hunter

`xhunt-hunter` 是一个本地运行的 Go 采集工具，用来直接请求 XHunt 的 `user-info` 接口，把 KOL 主表写入 MySQL，并支持断点续跑。

这不是服务，也不是前端脚本，而是一个命令行工具。

## 为什么做成 CLI

这里的 `CLI` 可以直接理解成“命令行启动的小程序”。

做成 CLI 是因为它最适合当前需求：

- 你手动给几个 seed 开跑
- 需要限制最大层数
- 跑到一半停掉后，下次可以继续
- 需要反复试请求间隔，观察限流
- 主要目标是离线采集，不是给别人实时调接口

所以它不是为了复杂，而是为了更轻、更稳。

## 这版做了什么

- 直接请求 `https://kol.xhunt.ai/api/twitter/user-info`
- 从返回里拿当前账号自己的 rank 和账号信息
- 从 `global / cn / top100` 三组关注列表里拿下一层账号
- `username` 唯一去重
- 用数据库里的 `crawl_seen` 表维护采集状态
- 支持中断后继续，不会从头重复跑
- 日志直接输出到本地 `logs/*.txt`
- 遇到 `{"data":null,"err":"rate_limit"}` 会记录：
  - 限流时间
  - 当前账号
  - 当前层数
  - 距离上次限流之间的请求次数

## 表说明

### `kol_rankings`

这是给其他项目读的主表，1 个账号 1 行。

主要字段：

- `username`
- `display_name`
- `profile_url`
- `avatar_url`
- `bio`
- `location`
- `create_time`
- `global_rank`
- `cn_rank`
- `en_rank`
- `classification`
- `is_cn`
- `followers_count`
- `following_count`
- `listed_count`
- `tweets_count`
- `global_kol_followers_count`
- `cn_kol_followers_count`
- `top_kol_followers_count`
- `discovery_depth`
- `discovered_by_count`
- `first_discovered_at`
- `last_discovered_at`
- `last_fetched_at`

### `crawl_seen`

这是采集器内部状态表，不给业务方直接读。

它用来记录：

- 某个账号有没有被真正请求过
- 上次请求时间
- 上次成功时间
- 上次错误
- 是否被限流
- 下一次允许重试时间

### `crawl_edges`

记录“谁发现了谁”，方便回溯来源关系。

## 断点续跑

断点续跑依赖 `crawl_seen`，不是靠本地内存。

程序每次都会优先找：

- `is_fetched = 0`
- `discovery_depth < max_depth`
- `next_retry_at <= now`

的账号继续抓。

所以你哪怕中途停掉，再次执行同一条命令，也会从库里未完成的账号接着跑。

## 使用方式

先建表：

```bash
go run . \
  -dsn 'root:password@tcp(host:3306)/hot?parseTime=true&loc=Local&charset=utf8mb4' \
  -migrate-only
```

再开始采集：

```bash
go run . \
  -dsn 'root:password@tcp(host:3306)/hot?parseTime=true&loc=Local&charset=utf8mb4' \
  -seeds realDonaldTrump,elonmusk \
  -max-depth 2 \
  -request-interval 15s \
  -rate-limit-sleep 65s
```

如果你已经手工拿到了 top100 JSON，也可以先直接导入数据库：

```bash
go run . \
  -dsn 'root:password@tcp(host:3306)/hot?parseTime=true&loc=Local&charset=utf8mb4' \
  -import-json './我手动弄来的top数据'
```

`-import-json` 支持：

- 单个 json 文件
- 多个文件，逗号分隔
- 一个目录，程序会把目录下的 `.json` 全部导入

## 参数

- `-dsn`：MySQL 连接串，必填
- `-seeds`：逗号分隔的 seed 用户名，首次启动时使用
- `-max-depth`：最大 BFS 层数，root seed 是 `0`
- `-request-interval`：成功请求之间的固定等待
- `-rate-limit-sleep`：遇到限流后的休眠时间
- `-domain`：默认 `web3`
- `-log-dir`：日志目录，默认 `logs`
- `-import-json`：导入手工准备的 top100 json 文件或目录
- `-migrate-only`：只建表，不抓数据

## 限制说明

- 当前默认把 `rate_limit` 当成“整体限流”处理
- 关注列表只有 `username/name/avatar`，没有对方自己的 rank
- 所以只有当某个账号“轮到它自己被请求”时，主表里的 `global_rank` 才能被可靠补全
- 手工导入的全球榜单会写 `global_rank`；华语榜单写 `cn_rank`；英文榜单写 `en_rank`
- 手工导入榜单不会把账号标记成“已抓过详情”，后续 BFS 仍会继续请求它自己的详情页

## 建议跑法

先用少量 seed 和较慢间隔测试：

```bash
go run . \
  -dsn '...' \
  -seeds realDonaldTrump \
  -max-depth 2 \
  -request-interval 20s \
  -rate-limit-sleep 70s
```

等你确认限流规律后，再逐步扩大 seed 数量。
