# mod_asr 压力测试方案

## 1. 测试目标

验证 mod_asr 模块在高并发场景下的**线程安全性**和**长时间运行稳定性**，重点关注：

- 阿里云 ASR 接口最大并发仅 2 路，需测试**超出并发限制**时的行为
- 多线程并发访问时的**线程冲突**和潜在 BUG
- 平台**长时间稳定运行**能力（内存泄漏、资源耗尽、连接堆积等）
- 异常场景下的**容错与恢复**能力

---

## 2. 已识别的线程安全风险点

通过代码审查，发现以下潜在线程安全问题，是压测重点验证目标：

### 2.1 CRITICAL — 全局静态变量竞争

| 位置 | 变量 | 风险 |
|------|------|------|
| `provider_aliyun.c:904-905` | `static uint32_t ws_feed_count` | 所有 WebSocket 会话共享，多 worker 线程并发递增无锁保护，可能导致计数不准或 UB |
| `provider_aliyun.c:1013` | `static uint32_t poll_count` | 所有会话共享的轮询计数器，同样无锁保护 |
| `asr_session.c:497` | `static uint32_t global_feed_count` | 所有 session 共享的 feed 计数器，多线程写入无同步 |

### 2.2 HIGH — result_text/result_xml 双重释放

| 位置 | 描述 |
|------|------|
| `mod_asr.c:480-481` + `asr_session.c:676-678` | `mod_asr_asr_get_results()` 和 `asr_session_get_result()` 都对 `result_text/result_xml` 执行 `switch_safe_free + 置NULL`，如果两者被并发调用，可能导致双重释放 |
| `mod_asr.c:464` | `session->result_xml` 检查未加锁，worker 线程可能同时通过 `asr_session_set_result()` 修改此字段 |

### 2.3 HIGH — WebSocket 连接状态竞争

| 位置 | 描述 |
|------|------|
| `provider_aliyun.c:914-920` | `aliyun_asr_feed()` 检查 `asr_ws_is_connected()` 和 `ctx->transcription_started` 时无锁，worker 线程可能同时在接收 CLOSE 帧设置 `conn->connected=FALSE` |
| `asr_ws_client.c:934-937` | `asr_ws_disconnect()` 设置 `conn->connected=FALSE` 与 `asr_ws_send_binary()` 的连接检查无同步 |

### 2.4 MEDIUM — 会话状态无原子检查

| 位置 | 描述 |
|------|------|
| `mod_asr.c:356-362` | `check_results()` 检查 flags 是无锁的（`switch_test_flag`），flags 可能被 worker 线程同时修改 |
| `asr_session.c:509` | `asr_session_feed()` 中 `switch_test_flag(session, ASR_SESSION_FLAG_CLOSED)` 无锁检查 |

### 2.5 MEDIUM — shutdown 持锁 join 死锁

| 位置 | 描述 |
|------|------|
| `mod_asr.c:939-949` | `mod_asr_shutdown()` 持全局锁遍历 sessions 哈希表，对每个 session 调用 `asr_session_stop_worker()`，而 `stop_worker()` 内部会 `switch_thread_join()` 阻塞等待线程退出。持全局锁期间阻塞会导致其他需要全局锁的操作（如新建/销毁 session）死锁 |

### 2.6 LOW — 伪随机数生成可预测性

| 位置 | 描述 |
|------|------|
| `provider_aliyun.c:163-177` | `aliyun_generate_id()` 使用时间戳+栈地址+LCG 生成 task_id/message_id，并发时多个线程可能生成相同 ID |
| `asr_ws_client.c:505-507` | WebSocket 帧掩码密钥使用时间戳生成，并发连接时掩码可能重复 |

---

## 3. 测试环境

### 3.1 环境要求

```
OS: Linux (WSL2)
FreeSWITCH: 1.10.6 (本机源码编译)
mod_asr: 当前 dev 分支
阿里云 ASR: NLS SpeechTranscriber (WebSocket) / 一句话识别 (REST)
并发限制: 阿里云 ASR 最大 2 路并发
```

### 3.2 测试音频资源

| 来源 | 路径 | 格式 | 用途 |
|------|------|------|------|
| FreeSWITCH 内置音效 | `/home/xyz/freeswitch/share/freeswitch/sounds/music/8000/*.wav` | 8kHz PCM | 模拟电话语音 |
| FreeSWITCH 内置提示音 | `/home/xyz/freeswitch/share/freeswitch/sounds/en/us/callie/**/*.wav` | 8kHz PCM | 模拟短语音 |
| 项目测试音频 | `src/mod/formats/mod_sndfile/test/sounds/*.wav` | 多种 | 功能验证 |
| 连续 ASR 脚本 | `conf/script/continuous_asr.lua` | Lua | 连续识别集成测试 |

### 3.3 工具链

| 工具 | 用途 |
|------|------|
| `fs_cli` | FreeSWITCH 命令行管理 |
| `sipp` | SIP 压力测试（模拟并发呼叫） |
| `originate` 命令 | 通过 ESL/Lua 脚本发起呼叫 |
| `valgrind` | 内存泄漏和非法访问检测 |
| `AddressSanitizer` | 编译期内存错误检测（`--enable-address-sanitizer`） |
| `gdb` | 运行时线程状态检查和崩溃分析 |
| `strace` | 系统调用追踪（文件描述符泄漏等） |
| `lsof` | 监控文件描述符数量 |

---

## 4. 测试方案

### Phase 1: 单会话功能验证（冒烟测试）

**目的**: 确保基本功能正常，作为压测基线

| 编号 | 测试项 | 方法 | 预期结果 |
|------|--------|------|----------|
| S1.1 | WebSocket 模式单次识别 | 使用 `continuous_asr.lua` 播放 wav 文件 | 正确返回识别文本 |
| S1.2 | REST 模式单次识别 | `play_and_detect_speech` REST 模式 | 正确返回识别文本 |
| S1.3 | 连续识别（多轮） | 连续播放多个 wav 文件，验证多轮 SentenceEnd | 每轮都有结果，不混淆 |
| S1.4 | 会话正常关闭 | 挂机后检查 worker 线程退出 | 线程退出、资源释放、无泄漏 |

**执行脚本**:
```lua
-- test_smoke.lua
session:answer()
session:setVariable("fire_asr_events", "true")
session:execute("detect_speech", "mod_asr {provider=aliyun,mode=websocket}default default")
session:streamFile("/home/xyz/freeswitch/share/freeswitch/sounds/music/8000/ponce-preludio-in-e-major.wav")
session:execute("detect_speech", "stop")
```

### Phase 2: 并发会话压力测试（核心）

**目的**: 超出阿里云 2 路并发限制，验证线程安全和资源管理

#### 2.1 并发梯度测试

| 编号 | 并发数 | 超出限制 | 测试重点 |
|------|--------|----------|----------|
| C2.1 | 2 | 0 | 基线：恰好等于并发限制 |
| C2.2 | 4 | 2 | 轻度超限：连接排队/失败处理 |
| C2.3 | 8 | 6 | 中度超限：大量连接失败场景 |
| C2.4 | 16 | 14 | 高压：验证不会崩溃 |
| C2.5 | 50 | 48 | 极端压力：资源耗尽测试 |

**执行方式**: 使用 FreeSWITCH `originate` 命令并发发起 SIP 呼叫

```bash
#!/bin/bash
# concurrent_test.sh
# 并发发起 N 路 ASR 识别呼叫

CONCURRENT=$1  # 并发数
SCRIPT="continuous_asr.lua"

for i in $(seq 1 $CONCURRENT); do
    fs_cli -x "originate sofia/gateway/gw1/1000${i} &lua(${SCRIPT})" &
    echo "Started call #$i"
done

wait
echo "All $CONCURRENT calls completed"
```

**监控指标**:
```bash
# 终端1: 监控 ASR 会话状态
watch -n 1 'fs_cli -x "asr status" && fs_cli -x "asr list"'

# 终端2: 监控文件描述符
watch -n 1 'lsof -p $(pidof freeswitch) | wc -l'

# 终端3: 监控线程数
watch -n 1 'ls /proc/$(pidof freeswitch)/task | wc -l'

# 终端4: 监控内存
watch -n 1 'ps -o rss,vsz -p $(pidof freeswitch)'
```

#### 2.2 快速创建/销毁循环

**目的**: 验证会话生命周期管理的线程安全，重点检测：
- `asr_session_create()` / `asr_session_destroy()` 的并发安全
- `asr_globals.active_sessions` 计数准确性
- `asr_globals.sessions` 哈希表并发访问
- worker 线程的创建和 join 安全性

```lua
-- test_rapid_cycle.lua
-- 快速创建和销毁 ASR 会话，每秒循环 2-3 次
session:answer()
for i = 1, 30 do
    session:execute("detect_speech", "mod_asr aliyun:websocket default")
    session:streamFile("silence_stream://500")
    session:execute("detect_speech", "stop")
    session:consoleLog("INFO", string.format("Cycle %d done\n", i))
end
```

**验证点**:
- [ ] `asr status` 显示的 `active_sessions` 最终归零
- [ ] 无 `double-free` 或 `use-after-free` 错误（需 AddressSanitizer）
- [ ] 线程数不持续增长
- [ ] 文件描述符不泄漏

### Phase 3: 线程冲突专项测试

**目的**: 针对已识别的线程安全风险点进行定向压力测试

#### 3.1 静态变量竞争测试

**针对风险 2.1** — `ws_feed_count`, `poll_count`, `global_feed_count`

```bash
# 同时发起 10 路 WebSocket 识别，观察静态计数器是否异常
# 日志中搜索异常的计数器值（如跳跃、回退）
for i in $(seq 1 10); do
    fs_cli -x "originate sofia/internal/100${i}@localhost &lua(continuous_asr.lua)" &
done
wait

# 分析日志
grep -E "(WS feed:|poll_results:|global_feed_count)" /var/log/freeswitch/freeswitch.log | \
  awk '{print $NF}' | sort -n | uniq -c | sort -rn | head -20
```

**验证方法**:
- 在代码中临时添加 `__sync_fetch_and_add` 原子操作替换 `++`，对比测试结果
- 如果使用原子操作后行为不同，说明确实存在竞争

#### 3.2 result_text/result_xml 双重释放测试

**针对风险 2.2** — get_results 并发访问

```lua
-- test_result_race.lua
-- 在多个回调中同时读取结果，模拟 check_results/get_results 竞争
session:answer()
session:setVariable("fire_asr_events", "true")

-- 启动一个定时器线程频繁调用 check_results
session:execute("detect_speech", "mod_asr aliyun:websocket default default")

-- 播放长音频确保持续产生结果
for i = 1, 20 do
    session:streamFile("/home/xyz/freeswitch/share/freeswitch/sounds/music/8000/suite-espanola-op-47-leyenda.wav")
end

session:execute("detect_speech", "stop")
```

**编译时启用 AddressSanitizer 检测**:
```bash
./configure --enable-address-sanitizer && make
# 运行压测后检查 ASan 日志
```

#### 3.3 WebSocket 连接断开竞态测试

**针对风险 2.3** — feed 时连接断开

```lua
-- test_ws_disconnect_race.lua
-- 模拟在 feed 音频过程中强制断开 WS 连接
session:answer()
session:execute("detect_speech", "mod_asr aliyun:websocket default default")

-- 播放音频 2 秒后挂机（触发 WS 断开，此时 worker 可能正在 feed）
session:streamFile("silence_stream://2000")
-- 挂机会触发 asr_close -> asr_ws_disconnect
-- 而 worker 线程可能同时在 asr_ws_send_binary
```

**重点验证**: 在高并发下反复执行上述操作，观察是否出现：
- SSL_write 在 SSL_free 后被调用（use-after-free）
- send 在 close(sockfd) 后被调用
- 崩溃或 ASan 报错

#### 3.4 shutdown 持锁 join 死锁测试

**针对风险 2.5**

```bash
# 终端1: 发起 50 个并发 ASR 会话
for i in $(seq 1 50); do
    fs_cli -x "originate sofia/internal/100${i}@localhost &lua(continuous_asr.lua)" &
done

# 终端2: 在会话运行期间执行模块卸载
sleep 5
fs_cli -x "unload mod_asr"

# 观察是否死锁（fs_cli 无响应超过 30 秒）
```

### Phase 4: 长时间稳定性测试（Soak Test）

**目的**: 验证 24 小时连续运行无崩溃、无内存泄漏、无资源耗尽

#### 4.1 循环拨测方案

```bash
#!/bin/bash
# soak_test.sh - 24小时持续压力测试

DURATION_HOURS=24
INTERVAL=30          # 每次呼叫间隔 30 秒
CONCURRENT=2         # 每次并发 2 路（等于阿里云限制）
ITERATIONS=$((DURATION_HOURS * 3600 / INTERVAL / CONCURRENT))

echo "Starting soak test: ${DURATION_HOURS}h, ${CONCURRENT} concurrent, interval ${INTERVAL}s"

for iter in $(seq 1 $ITERATIONS); do
    for c in $(seq 1 $CONCURRENT); do
        fs_cli -x "originate sofia/internal/test${c}@localhost &lua(continuous_asr.lua)" &
    done

    # 每 10 分钟记录一次状态
    if (( iter % 20 == 0 )); then
        TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
        ACTIVE=$(fs_cli -x "asr status" | grep "Active Sessions" | awk '{print $NF}')
        FD_COUNT=$(lsof -p $(pidof freeswitch) 2>/dev/null | wc -l)
        THREAD_COUNT=$(ls /proc/$(pidof freeswitch)/task 2>/dev/null | wc -l)
        MEM_KB=$(ps -o rss= -p $(pidof freeswitch) 2>/dev/null)
        echo "${TIMESTAMP} iter=${iter} active=${ACTIVE} fd=${FD_COUNT} threads=${THREAD_COUNT} mem_kb=${MEM_KB}" >> soak_results.log
    fi

    wait
    sleep $INTERVAL
done

echo "Soak test completed. Results in soak_results.log"
```

#### 4.2 长时间运行监控项

| 监控项 | 工具 | 告警阈值 |
|--------|------|----------|
| 内存使用（RSS） | `ps -o rss` | 持续增长超过基线 50% |
| 文件描述符数 | `lsof` | 超过 1024 或持续增长 |
| 线程数 | `/proc/pid/task` | 超过 200 或持续增长 |
| 活跃 ASR 会话 | `asr status` | 非零（应归零） |
| CPU 使用率 | `top` | 持续超过 80% |
| WebSocket 连接泄漏 | `netstat -anp` | CLOSE_WAIT 状态连接累积 |
| FreeSWITCH 日志错误 | `grep ERROR` | 出现 crash/segfault/assert |

#### 4.3 内存泄漏检测

**方法1: Valgrind 长时间运行**
```bash
# 启动 FreeSWITCH under valgrind（会显著降低性能，仅用于短时测试）
valgrind --leak-check=full --show-leak-kinds=all --track-fds=yes \
    --log-file=valgrind_asr.log freeswitch -nc -nonat

# 运行 1 小时循环测试后停止
# 分析 valgrind_asr.log
```

**方法2: 定期对比内存映射**
```bash
# 每 30 分钟保存一次内存映射快照
while true; do
    cat /proc/$(pidof freeswitch)/smaps > smaps_$(date +%H%M%S).log
    sleep 1800
done

# 测试结束后对比
diff smaps_first.log smaps_last.log
```

### Phase 5: 异常场景测试

**目的**: 验证各种异常条件下的容错能力

#### 5.1 网络异常

| 编号 | 场景 | 模拟方法 | 预期行为 |
|------|------|----------|----------|
| E5.1 | WS 连接中途断网 | `iptables -A OUTPUT -d aliyun_ip -j DROP` | feed 返回失败，worker 线程安全退出，不崩溃 |
| E5.2 | DNS 解析失败 | 临时修改 `/etc/resolv.conf` | open 返回失败，不崩溃 |
| E5.3 | SSL 握手超时 | `iptables` 延迟 aliyun 出站包 | 5 秒超时后返回失败 |
| E5.4 | REST 请求超时 | 设置极短超时 + 网络延迟 | 返回 SWITCH_STATUS_FALSE，不阻塞 worker |

#### 5.2 服务端异常

| 编号 | 场景 | 模拟方法 | 预期行为 |
|------|------|----------|----------|
| E5.5 | Token 获取失败 | 配置错误的 access_key | open 返回失败，不崩溃 |
| E5.6 | TaskFailed 事件 | 发送非法格式音频 | 日志记录错误，继续运行 |
| E5.7 | 服务端主动关闭 WS | 等待 Token 过期后继续 feed | feed 检测断开并返回失败 |
| E5.8 | 并发超限（429） | 同时发起 >2 路识别 | 超出限制的连接失败，不影响已有会话 |

#### 5.3 资源耗尽

| 编号 | 场景 | 模拟方法 | 预期行为 |
|------|------|----------|----------|
| E5.9 | 音频缓冲区无限增长 | REST 模式下持续 feed 不触发提交 | `rest_max_audio_len=1MB` 上限生效 |
| E5.10 | 大量并发 malloc | 50+ 路并发 WS feed | malloc 失败时安全退出，不崩溃 |
| E5.11 | 文件描述符耗尽 | 不关闭的 WS 连接累积 | `ulimit -n` 限制触发后安全失败 |

---

## 5. 测试自动化框架

### 5.1 主控脚本

```bash
#!/bin/bash
# run_all_tests.sh - 自动化压测执行框架

RESULTS_DIR="stress_test_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

# 编译 ASan 版本
build_asan() {
    echo "=== Building with AddressSanitizer ==="
    cd /home/xyz/freeswitch-1.10.6
    make clean && ./configure --enable-address-sanitizer && make -j$(nproc)
}

# 运行单个测试用例
run_test() {
    local test_id=$1
    local test_name=$2
    local test_cmd=$3
    local log_file="${RESULTS_DIR}/${test_id}.log"

    echo "=== Running ${test_id}: ${test_name} ===" | tee -a "$RESULTS_DIR/summary.log"
    eval "$test_cmd" > "$log_file" 2>&1
    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        echo "  PASS" | tee -a "$RESULTS_DIR/summary.log"
    else
        echo "  FAIL (exit=$exit_code)" | tee -a "$RESULTS_DIR/summary.log"
    fi

    # 收集 FreeSWITCH 日志
    cp /var/log/freeswitch/freeswitch.log "${RESULTS_DIR}/${test_id}_fs.log" 2>/dev/null

    # 重启 FreeSWITCH 确保干净状态
    fs_cli -x "reload mod_asr" 2>/dev/null
    sleep 2
}

# 基线状态记录
record_baseline() {
    echo "=== Recording baseline ==="
    fs_cli -x "asr status" > "${RESULTS_DIR}/baseline_asr_status.txt"
    ps -o rss,vsz,thcount -p $(pidof freeswitch) > "${RESULTS_DIR}/baseline_memory.txt"
    lsof -p $(pidof freeswitch) | wc -l > "${RESULTS_DIR}/baseline_fd_count.txt"
}

# ===== 执行测试 =====

# Phase 1: 冒烟测试
run_test "S1.1" "WebSocket 单次识别" \
    "fs_cli -x 'originate sofia/internal/test@localhost &lua(test_smoke.lua)'"

run_test "S1.2" "REST 单次识别" \
    "fs_cli -x 'originate sofia/internal/test@localhost &lua(test_smoke_rest.lua)'"

# Phase 2: 并发测试
for n in 2 4 8 16 50; do
    run_test "C2.${n}" "并发 ${n} 路 WebSocket" \
        "bash concurrent_test.sh ${n} websocket"
done

# Phase 3: 线程冲突
run_test "T3.1" "静态变量竞争" "bash static_var_race_test.sh"
run_test "T3.2" "result 双重释放" "bash result_race_test.sh"
run_test "T3.3" "WS 断开竞态" "bash ws_disconnect_race_test.sh"
run_test "T3.4" "shutdown 死锁" "bash shutdown_deadlock_test.sh"

# Phase 5: 异常场景
run_test "E5.1" "WS 中途断网" "bash network_disconnect_test.sh"
run_test "E5.5" "Token 获取失败" "bash token_failure_test.sh"
run_test "E5.8" "并发超限" "bash concurrent_limit_test.sh"

echo "=== All tests completed. Results in ${RESULTS_DIR}/ ==="
```

### 5.2 ESL Python 压测脚本

```python
#!/usr/bin/env python3
"""
asr_stress_test.py - 基于 FreeSWITCH ESL 的 ASR 压测脚本
支持并发发起 SIP 呼叫并监控 ASR 会话状态
"""

import threading
import time
import sys

ESL_HOST = "127.0.0.1"
ESL_PORT = 8021
ESL_PASSWORD = "ClueCon"

CONCURRENT_LEVELS = [2, 4, 8, 16, 50]
SCRIPT = "continuous_asr.lua"

results_lock = threading.Lock()
results = {
    "success": 0,
    "failed": 0,
    "timeout": 0,
    "errors": [],
}


def make_call(call_id, mode="websocket"):
    """发起一路 ASR 识别呼叫"""
    try:
        from ESL import ESLconnection
        con = ESLconnection(ESL_HOST, ESL_PORT, ESL_PASSWORD)
        if not con.connected():
            with results_lock:
                results["failed"] += 1
                results["errors"].append(f"call#{call_id}: ESL connect failed")
            return

        dest = f"test{call_id:04d}"
        args = f"mod_asr {{provider=aliyun,mode={mode}}}default default"

        con.bgapi(f"originate sofia/internal/{dest}@localhost &detect_speech({args})")

        with results_lock:
            results["success"] += 1

    except Exception as e:
        with results_lock:
            results["failed"] += 1
            results["errors"].append(f"call#{call_id}: {str(e)}")
    finally:
        try:
            con.disconnect()
        except:
            pass


def monitor_asr_status(duration_sec, interval_sec=5):
    """监控 ASR 模块状态"""
    import subprocess
    start = time.time()
    snapshots = []

    while time.time() - start < duration_sec:
        try:
            from ESL import ESLconnection
            con = ESLconnection(ESL_HOST, ESL_PORT, ESL_PASSWORD)
            if con.connected():
                status = con.api("asr status").getBody()
                active = int(status.split("Active Sessions:")[-1].strip())
                fd_count = int(
                    subprocess.check_output(
                        f"lsof -p $(pidof freeswitch) 2>/dev/null | wc -l", shell=True
                    ).strip()
                )
                snapshots.append({
                    "time": time.time() - start,
                    "active_sessions": active,
                    "fd_count": fd_count,
                })
                con.disconnect()
        except Exception:
            pass
        time.sleep(interval_sec)

    return snapshots


def run_concurrent_test(concurrent, duration_sec=120):
    """运行指定并发级别的测试"""
    print(f"\n{'='*60}")
    print(f"Concurrent Test: {concurrent} sessions")
    print(f"{'='*60}")

    # 启动监控线程
    monitor_thread = threading.Thread(
        target=monitor_asr_status,
        args=(duration_sec,),
        daemon=True,
    )
    monitor_thread.start()

    # 并发发起呼叫
    threads = []
    for i in range(concurrent):
        t = threading.Thread(target=make_call, args=(i, "websocket"))
        threads.append(t)
        t.start()
        time.sleep(0.5)  # 每 500ms 发起一路，避免瞬间冲击

    for t in threads:
        t.join(timeout=duration_sec)

    monitor_thread.join(timeout=5)

    # 输出结果
    print(f"\nResults for {concurrent} concurrent:")
    print(f"  Success: {results['success']}")
    print(f"  Failed:  {results['failed']}")
    if results["errors"]:
        print(f"  Errors ({len(results['errors'])}):")
        for err in results["errors"][:10]:
            print(f"    - {err}")


if __name__ == "__main__":
    levels = [int(x) for x in sys.argv[1:]] if len(sys.argv) > 1 else CONCURRENT_LEVELS

    for level in levels:
        results = {"success": 0, "failed": 0, "timeout": 0, "errors": []}
        run_concurrent_test(level)
        time.sleep(10)  # 级别间冷却
```

---

## 6. 结果判定标准

### 6.1 通过标准

| 级别 | 标准 |
|------|------|
| **必须通过** | 无 segfault/崩溃；无 double-free/use-after-free（ASan）；active_sessions 归零 |
| **应该通过** | 静态计数器值合理（允许少量不一致）；无内存泄漏（RSS 增长 <10%/24h）；无 FD 泄漏 |
| **建议通过** | 并发 >2 路时失败的会话能正确返回错误；不成功的会话不影响成功会话 |

### 6.2 失败分类

| 类别 | 表现 | 对应风险 |
|------|------|----------|
| **崩溃类** | segfault, abort, SIGBUS | 风险 2.2(双重释放), 2.3(连接断开后访问) |
| **死锁类** | FreeSWITCH 无响应 | 风险 2.5(shutdown 持锁 join) |
| **泄漏类** | RSS/FD/线程持续增长 | 会话销毁不完整、WS 连接未关闭 |
| **数据类** | 识别结果混乱、丢失 | 风险 2.1(静态变量竞争), 2.4(状态无原子检查) |

---

## 7. 代码修复建议（压测前可选实施）

针对已识别的风险点，建议在压测前修复以下高优先级问题：

### 7.1 修复静态变量竞争（风险 2.1）

```c
// 将 static uint32_t 改为原子操作或 session 级变量
// 方案1: 使用原子操作
static volatile uint32_t ws_feed_count = 0;
// 递增时使用:
__sync_fetch_and_add(&ws_feed_count, 1);

// 方案2（推荐）: 改为 session 级变量，放入 aliyun_asr_ctx_t
typedef struct {
    // ... 现有字段 ...
    uint32_t ws_feed_count;     // 替代 static 变量
    uint32_t ws_total_bytes;    // 替代 static 变量
} aliyun_asr_ctx_t;
```

### 7.2 修复 result 双重释放（风险 2.2）

```c
// mod_asr.c mod_asr_asr_get_results() 中，在访问 result_xml 前加锁
switch_mutex_lock(session->mutex);
if (session->result_xml) {
    // ... 现有的取结果+重置逻辑 ...
}
switch_mutex_unlock(session->mutex);
```

### 7.3 修复 shutdown 死锁（风险 2.5）

```c
// mod_asr_shutdown() 中，先收集所有 session，释放锁后再 join
switch_mutex_lock(asr_globals.mutex);
int count = 0;
asr_session_t *sessions[1024];
for (hi = switch_core_hash_first(asr_globals.sessions); hi; hi = switch_core_hash_next(&hi)) {
    switch_core_hash_this(hi, NULL, NULL, (void **) &sessions[count]);
    if (sessions[count]) count++;
}
switch_mutex_unlock(asr_globals.mutex);

// 不持锁地逐个停止 worker
for (int i = 0; i < count; i++) {
    asr_session_stop_worker(sessions[i]);
}
```

---

## 8. 测试执行计划

| 阶段 | 预计耗时 | 前置条件 |
|------|----------|----------|
| Phase 1: 冒烟测试 | 1 小时 | FreeSWITCH 运行 + 阿里云 ASR 可用 |
| Phase 2: 并发梯度 | 4 小时 | Phase 1 通过 |
| Phase 3: 线程冲突专项 | 4 小时 | ASan 编译版本 |
| Phase 4: 长时间稳定性 | 24 小时 | Phase 2+3 通过 |
| Phase 5: 异常场景 | 4 小时 | 网络控制权限 |

**总计约 37 小时**（可并行缩减），建议按顺序执行：Phase 1 → Phase 2 → Phase 3 → Phase 5 → Phase 4。
