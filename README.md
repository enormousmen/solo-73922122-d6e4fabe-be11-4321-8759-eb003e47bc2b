# pkg-cache：单进程 HTTP 包缓存代理 + 可控本地源站

面向构建工程的依赖包缓存代理：固定本地源站、按路径 GET 完整包，减少重复
回源流量；源包更新后按路径清除缓存。代理由 **FastAPI + aiohttp** 实现，
单进程单 worker 运行。

## 快速开始

```bash
docker compose up --build
```

* 代理：http://localhost:8080 （下载 API + `/docs`：校验下载、清除）
* 源站：http://localhost:8081 （控制 API + `/docs`，自带 32MiB 包 `pkg-32m.bin` 与 1MiB 包 `pkg-1m.bin`）

## 使用

```bash
# 下载（首次 MISS 回源，之后 HIT 命中磁盘缓存）
curl -OJ http://localhost:8080/packages/pkg-32m.bin

# 按路径清除（也可打开 http://localhost:8080/docs 在 Swagger UI 里执行）
curl -X POST http://localhost:8080/purge/pkg-32m.bin

# 查看代理内部状态
curl http://localhost:8080/cache/status
```

### 校验下载（按锁文件 SHA-256）

构建工程师只信任锁文件里的 SHA-256。把期望摘要放在请求头
`X-Expected-Sha256`（64 位十六进制，可带 `sha256:` 前缀）里请求
`/verified/{path}`，代理会先按块重算整包摘要，只有完整包与期望一致
才开始发送字节；不一致返回 **412**，不发送任何包字节。

```bash
# 1. 取出行锁文件里的摘要
SHA=$(curl -s http://localhost:8081/control/stats \
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["packages"]["pkg-1m.bin"]["sha256"])')

# 2. 校验下载：成功时整包字节与摘要一致
curl -sS -D - -o pkg-1m.bin \
  -H "X-Expected-Sha256: $SHA" \
  http://localhost:8080/verified/pkg-1m.bin
sha256sum pkg-1m.bin

# 3. 错误的摘要 -> 412，响应体只有诊断文本，没有包字节
curl -sS -i -H "X-Expected-Sha256: $(printf '0%.0s' {1..64})" \
  http://localhost:8080/verified/pkg-1m.bin
```

响应头 `X-Checksum-Verified: sha256` 表示该响应已按请求头摘要核验；
`X-Package-Sha256` 是代理实际算出的完整包摘要（不是源站声明）。

演示“缓存中已换新版本，但已受理的校验下载仍只发旧版”：

```bash
# 先让代理缓存旧版
curl -s -o /dev/null http://localhost:8080/packages/pkg-32m.bin
OLD=$(curl -s http://localhost:8081/control/stats \
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["packages"]["pkg-32m.bin"]["sha256"])')

# 后台慢速消费一次校验下载（先用源站延迟把传输拉长）
curl -s -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' -d '{"chunk_delay_ms":50}'
(curl -s -H "X-Expected-Sha256: $OLD" \
   http://localhost:8080/verified/pkg-32m.bin -o old.bin) &
sleep 1
# 期间换源站内容 + 清除代理缓存，再让新下载填充新版
curl -s -X POST http://localhost:8081/control/content/pkg-32m.bin
curl -s -X POST http://localhost:8080/purge/pkg-32m.bin
curl -s -o new.bin http://localhost:8080/packages/pkg-32m.bin
wait
sha256sum old.bin new.bin   # old.bin == $OLD，new.bin == 新摘要
```

源站控制（换内容 / 延迟 / 断流 / 回源次数）：

```bash
# 查看回源次数与当前包 sha256
curl http://localhost:8081/control/stats

# 更换包内容（重新生成随机字节，sha256 改变；代理在清除前仍发旧包）
curl -X POST http://localhost:8081/control/content/pkg-32m.bin

# 每个 256KiB 块延迟 100ms（模拟慢源站）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":100,"truncate_after_bytes":null,"chunk_size":262144}'

# 发送 200000 字节后断流（模拟上游故障）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":0,"truncate_after_bytes":200000,"chunk_size":65536}'

# 清零回源计数
curl -X POST http://localhost:8081/control/reset
```

## 行为要点

* **单飞回源**：同路径并发未命中只回源一次；所有下载（包括中途加入的）
  都从字节 0 开始逐块收到原字节，首块在源站首块落盘后即可读出，不等整包。
* **慢客户端隔离**：填充只写磁盘，各读者按自己的节奏尾随 `.part-<代>` 文件；
  单客户端内存为一个读缓冲，不随包长增长（实测 32MiB 下载代理 RSS 增量约 4MiB）。
* **普通下载 `GET /packages/{path}`**：保持原行为——逐块流式转发，首块就绪
  即开始响应，不做整包预校验。
* **校验下载 `GET /verified/{path}`**：与普通下载共用同一代单飞填充；整包
  落盘后按块重算 SHA-256（不信任源站的 `X-Package-Sha256`），完整包与请求头
  `X-Expected-Sha256` 一致才返回 200 并从头发送；不一致返回 412 且不发送包
  字节（响应体只是诊断文本）；缺少/非法摘要头返回 400。已缓存包同样每次重新
  按块计算，不复用任何历史摘要。
* **校验与发送同一份文件**：校验用私有 fd 计算并记录 inode，发送前再打开一次
  并要求 inode/size 相同，否则返回 409；响应把该 fd 固定到传输结束。因此期间
  即便清除缓存并填充新版，已受理的校验下载仍只发送它所校验的旧版字节（清除
  时有读者的完整旧代会被改名为 `.retired-<代>`，与新代彻底隔离）。客户端断开
  或失败时在 `finally` 中同步释放读者占用并关闭 fd，文件随即回收。
* **按路径清除**：清除返回后首个新下载重新回源，后续共用新填充/缓存；
  已开始的旧下载继续读完旧代文件；旧填充完成也不会重新登记；
  旧文件在最后一个读者结束后回收。
* **失败传播**：上游断流/非 200 使该代所有下载失败（客户端读到不完整
  响应），半包立即删除、绝不入缓存；下一个请求自动重试回源。校验下载此时
  返回源站状态或 502，同样不缓存、不发送包字节。
* **重启**：`proxy-cache` 卷保留完整包；启动时清理 `.part-*`、`.retired-*`
  残件与孤儿 meta。404 不做负缓存。

## 本地自测（无 Docker 时）

```bash
pip install -r proxy/requirements.txt -r origin/requirements.txt
pip install httpx pytest
python3 -m pytest tests/ -v
```

测试以真实子进程启动源站与代理，覆盖：单飞、首块延迟、慢客户端隔离、
清除语义（含填充中清除）、断流不缓存半包、重启保留/清理、内存有界、
404 与路径穿越；校验下载覆盖命中/未命中、摘要不符 412（含已缓存包被篡改）、
缺头/非法头 400、并发未命中共用单飞、源站失败 502、404、传输中清除并换版
仍发旧版字节、客户端断开释放读者占用。`tests/run_server.py` 是测试专用
启动器（本沙箱禁止进程间信号，用控制文件代替 SIGTERM 停服），交付应用本身
不含测试钩子。
