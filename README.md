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
- `top json` 只做一次性 seed 导入源，导入后统一进入 `crawl_seen`
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
- `account_created_at`
- `global_rank`
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
- `ctime`
- `mtime`

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

先按 `resource/config_example.json5` 准备本地配置：

```bash
config.json5
```

`config.json5` 已经在 `.gitignore` 里，可以放真实 DB DSN、Wi-Fi 名和密码。

先建表：

```bash
go run . -migrate-only
```

再开始采集：

```bash
go run .
```

如果你已经手工拿到了 top100 JSON，也可以先直接导入数据库：

```bash
把 config.json5 里的 xhunt.import_json 改成 './我手动弄来的top数据'，然后执行 go run .
```

`-import-json` 的行为是：

- 只抽取其中的 `username`
- 写入 `crawl_seen`
- 初始状态统一为 `pending`
- 不直接给 `kol_rankings` 写业务字段

`-import-json` 支持：

- 单个 json 文件
- 多个文件，逗号分隔
- 一个目录，程序会把目录下的 `.json` 全部导入

## 配置项

- `mysql.addr`：MySQL 连接串，必填
- `xhunt.seeds`：逗号分隔的 seed 用户名，首次启动时使用
- `xhunt.import_json`：导入手工准备的 top100 json 文件或目录，只把 username 放进 `crawl_seen`
- `xhunt.max_depth`：最大 BFS 层数，root seed 是 `0`
- `xhunt.expand_rank_limit`：只有已知 `global_rank <= N` 的账号才会继续展开 followers，默认 `10000`，`<= 0` 表示关闭
- `service.request_interval`：成功请求之间的固定等待
- `service.rate_limit_sleep`：遇到限流后的休眠时间
- `service.failure_backoff_multiplier`：全局连续失败退避倍数，基于 `rate_limit_sleep` 递增，`<= 1` 表示关闭
- `service.success_cooldown_every`：每成功 N 次后额外等待一次，`0` 表示关闭
- `service.success_cooldown_sleep`：命中成功阈值后的额外等待时长
- `wifi_recover.after_failures`：连续失败达到 N 次后触发一次 Wi-Fi 恢复，`0` 表示关闭
- `wifi_recover.mode`：Wi-Fi 恢复模式，支持 `reconnect` 和 `double-hop`
- `wifi_recover.device`：Wi-Fi 设备名，比如 `en1`
- `wifi_recover.from_ssid` / `wifi_recover.from_password`：`double-hop` 模式下回切的原 Wi-Fi
- `wifi_recover.to_ssid` / `wifi_recover.to_password`：`double-hop` 模式下临时切换的目标 Wi-Fi
- `wifi_recover.ssid` / `wifi_recover.password`：`reconnect` 模式下直接切换的目标 Wi-Fi
- `wifi_recover.wait`：Wi-Fi 恢复步骤之间的等待时间
- `wifi_recover.post_wait`：Wi-Fi 恢复成功后的额外等待时间，默认 `5s`
- `replay.on_start`：启动时是否把指定浅层 `success` 账号重新放回 `pending`
- `replay.success_depths`：要补跑的成功账号层数，例如 `[1, 2]`
- `replay.success_limit`：本次补跑最多重置多少个成功账号，`0` 表示不限制
- `log.dir`：日志目录，默认 `logs`
- `-config`：指定配置文件路径，默认 `config.json5`
- `-migrate-only`：只建表，不抓数据

## 限制说明

- 当前默认把 `rate_limit` 当成“整体限流”处理
- 当连续失败时，等待时间会按 `rate-limit-sleep * failure-backoff-multiplier^(连续失败次数-1)` 递增，成功一次后清零
- 如果配置了 Wi-Fi 恢复，当前只会在 `rate_limit` 时触发；每次命中阈值都会尝试恢复。恢复成功后，会先额外等待 `wifi-recover-post-wait`，并把当前被限流账号按 `rate_limit_sleep` 延后再重试，避免立刻重复请求同一个用户
- 如果配置了 `replay.on_start=true`，程序启动时会把指定浅层里已经 `success` 的账号重新丢回待跑队列，用于补跑 1、2 层这类早期测试阶段可能漏掉的扩展
- 关注列表只有 `username/name/avatar`，没有对方自己的 rank
- 所以只有当某个账号“轮到它自己被请求”时，主表里的 `global_rank` 才能被可靠补全
- 当前默认会先请求账号详情；如果确认 `global_rank > 10000`，该账号仍然会入主表，但不会继续展开下一层 followers
- 手工导入的 top JSON 只负责提供起步 `username`
- 主表里的业务字段全部以真实 `user-info` 请求结果为准

## 建议跑法

先用少量 seed 和较慢间隔测试，确认限流规律后，再在 `config.json5` 里逐步扩大 seed 数量、层数和请求速度。

## Wi-Fi 切换小测试

如果你想单独验证 macOS 上的 Wi-Fi 切换命令能不能跑，可以用这个独立小工具：

```bash
go run ./cmd/wifi-toggle-test -dry-run=true -mode=power-cycle -service 'Wi-Fi'
```

默认是 `dry-run=true`，只打印将要执行的命令，不会真的断网。

确认命令没问题后，再手动改成真实执行：

```bash
go run ./cmd/wifi-toggle-test -dry-run=false -mode=power-cycle -service 'Wi-Fi' -wait 3s
```

如果你只想直接切到另一个热点，可以用：

```bash
go run ./cmd/wifi-toggle-test \
  -dry-run=false \
  -mode=reconnect \
  -service 'Wi-Fi' \
  -device en0 \
  -ssid '你的WiFi名' \
  -password '你的密码'
```

如果你要做双跳，也就是先切到另一个热点，再切回当前热点，可以用：

```bash
go run ./cmd/wifi-toggle-test \
  -dry-run=false \
  -mode=double-hop \
  -device en0 \
  -from-ssid '当前WiFi' \
  -from-password '当前密码' \
  -to-ssid '另一个WiFi' \
  -to-password '另一个密码' \
  -wait 3s
```
