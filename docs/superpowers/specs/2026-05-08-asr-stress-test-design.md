# mod_asr 混合压测方案设计

## 概述

采用混合方案（方案 C）：用对的工具做对的事 — Lua/ESL 管代码正确性，SIPp 管系统容量。

原方案中 Phase 2（originate 并发）和 Phase 4（originate soak）的核心问题是 `originate` 命令从 FreeSWITCH 内部发起呼叫，完全绕过 SIP 信令栈，无法测量真实 SIP 并发承载能力。SIPp 作为行业标准 SIP 压测工具，可以精确控制 CPS、模拟真实 SIP 信令和 RTP 媒体流，更适合系统容量测试。

## 整体架构

```
┌─────────────────────────────────────────────────┐
│                 压测执行框架                       │
├─────────────────────┬───────────────────────────┤
│  Track A: 代码正确性  │  Track B: 系统容量          │
│  (Lua + ESL + ASan) │  (SIPp + ESL 监控)         │
├─────────────────────┼───────────────────────────┤
│  Phase 1: 冒烟测试   │  Phase 6: SIP并发容量       │
│  Phase 3: 线程安全   │  Phase 7: SIPp长时间稳定性   │
│  Phase 5: 异常场景   │                            │
├─────────────────────┴───────────────────────────┤
│          共享层: ESL 监控 + 指标采集               │
│  (持久连接, 时序CSV, 自动化 pass/fail 判定)        │
└─────────────────────────────────────────────────┘
```

**分工原则**：

| 测试目标 | 工具 | 原因 |
|----------|------|------|
| ASR 功能验证 | Lua 脚本 | 需验证识别结果正确性 |
| 线程安全 / 内存安全 | Lua + ASan + 断言脚本 | 需精确控制 mod_asr 内部行为 |
| 异常场景容错 | Lua + 网络工具 | 需精细控制异常触发时机 |
| 系统并发容量 | SIPp | 需真实 SIP 负载测容量 |
| 长时间稳定性 | SIPp + ESL monitor | SIPp 产压 + ESL 采集内部指标 |

## Track B: SIPp 场景设计

### Phase 6：SIP 并发容量测试

**SIPp UAC 场景流程**：

```
INVITE (with SDP: PCMU 8000Hz)
  ← 100 Trying
  ← 180 Ringing
  ← 200 OK
ACK
  → RTP audio (pcap: 中文语音 8kHz PCMU, 持续 10s)
  ← RTP audio (FreeSWITCH 回放)
  [FreeSWITCH dialplan: answer → detect_speech → mod_asr 处理]
  → BYE (音频播放完毕)
  ← 200 OK
```

**FreeSWITCH 侧 dialplan**：

```xml
<extension name="asr_stress_test">
  <condition field="destination_number" expression="^8xxx$">
    <action application="answer"/>
    <action application="detect_speech" data="mod_asr aliyun:websocket default default"/>
    <action application="playback" data="/opt/test_audio/chinese_speech_8k.wav"/>
    <action application="detect_speech" data="stop"/>
    <action application="hangup"/>
  </condition>
</extension>
```

**SIPp 并发梯度**：

| 编号 | 并发数 | CPS | 时长 | 测试重点 |
|------|--------|-----|------|----------|
| P6.1 | 2 | 1 | 5min | 基线：等于阿里云限制 |
| P6.2 | 5 | 1 | 5min | 轻度超限 |
| P6.3 | 10 | 2 | 5min | 中度超限 |
| P6.4 | 20 | 3 | 10min | 高压 |
| P6.5 | 50 | 5 | 10min | 极端压力 |

**SIPp 命令示例**：

```bash
sipp 127.0.0.1:5060 \
  -sf asr_stress_uac.xml \
  -inf asr_test_users.csv \
  -m 50 \
  -r 5 \
  -l 20 \
  -d 10000 \
  -s 8001 \
  -i 127.0.0.1 \
  -p 6080 \
  -trace_stat \
  -stf stats_p6_4.csv
```

### Phase 7：SIPp 长时间稳定性测试

```bash
sipp 127.0.0.1:5060 \
  -sf asr_stress_uac.xml \
  -inf asr_test_users.csv \
  -r 1 \
  -l 2 \
  -d 30000 \
  -t mi \
  -trace_stat \
  -stf soak_stats.csv
```

持续 24 小时，SIPp 自带统计输出 + ESL 监控采集内部指标。

### 准备工作

1. **安装 SIPp**：`apt install sipp`（或源码编译带 pcap 支持的版本）
2. **准备 pcap 音频**：将 8kHz 中文语音 wav 转为 SIPp 可用的 pcap 文件
3. **Sofia profile 配置**：确保 internal profile 允许来自 127.0.0.1 的呼叫、PCMU 编解码
4. **用户 CSV**：生成 SIPp 用的被叫号码列表

## 共享层：ESL 监控 + 指标采集

### 持久连接监控守护进程

替代当前方案中 4 个终端手动 `watch` 和 ESL Python 脚本每次新建连接的问题。

```python
class ASRMonitor:
    def __init__(self):
        self.con = ESLconnection(ESL_HOST, ESL_PORT, ESL_PASSWORD)
        self.con.events("plain", "CUSTOM asr::**")
        self.baseline = None
        self.csv_writer = ...

    def collect(self):
        """每 5 秒采集一次，写入时序 CSV"""
        row = {
            "timestamp": now(),
            "active_sessions": parse_asr_status(self.con.api("asr status")),
            "fd_count": get_fd_count(),
            "thread_count": get_thread_count(),
            "rss_kb": get_rss(),
            "ws_connections": get_ws_conn_count(),
            "close_wait": get_close_wait_count(),
        }
        self.csv_writer.writerow(row)
        return row

    def check_alerts(self, row):
        """实时告警，自动判定 pass/fail"""
        if row["active_sessions"] > 0 and test_idle:
            alert("active_sessions 未归零！")
        if row["close_wait"] > 10:
            alert("CLOSE_WAIT 累积！可能 WS 连接泄漏")
        if self.baseline and row["rss_kb"] > self.baseline["rss_kb"] * 1.5:
            alert("RSS 超基线 50%，疑似内存泄漏")
```

### 输出格式

**时序 CSV**（`metrics_YYYYMMDD_HHMMSS.csv`）：

```
timestamp,active_sessions,fd_count,thread_count,rss_kb,ws_connections,close_wait
2026-05-08T10:00:00,0,142,45,128432,0,0
2026-05-08T10:00:05,5,157,53,145672,5,0
...
```

**告警日志**（`alerts_YYYYMMDD.log`）：

```
[10:23:15] ALERT: CLOSE_WAIT 累积=15，疑似 WS 连接泄漏
[10:45:30] ALERT: RSS=312448KB 超基线(128432KB) 143%
```

**自动判定摘要**（测试结束时生成）：

```
=== Test Result: PASS ===
Duration: 3600s
Peak active_sessions: 20
Post-test active_sessions: 0  ✓
RSS growth: 12% (threshold: 50%)  ✓
FD growth: +8 (threshold: +100)  ✓
CLOSE_WAIT peak: 3 (threshold: 10)  ✓
Alerts: 0
```

Track A / Track B 共用此监控层。

## Track A 优化：现有方案改进

### 优化 1：ESL Python 压测脚本改用持久连接

**问题**：当前脚本每次 `make_call` 都新建 ESL 连接，50 路并发 = 50 个连接开销。

**改进**：复用单个持久 ESL 连接发 `bgapi`，结果通过事件回调收集。

```python
class ASRStressTest:
    def __init__(self):
        self.con = ESLconnection(ESL_HOST, ESL_PORT, ESL_PASSWORD)
        self.con.events("plain", "BACKGROUND_JOB")

    def launch_calls(self, count, mode="websocket"):
        for i in range(count):
            job_id = self.con.bgapi(
                f"originate sofia/internal/8{i:04d}@localhost "
                f"&lua(continuous_asr.lua)"
            )
            time.sleep(0.5)
```

### 优化 2：修复 soak test 迭代计算 bug

**Bug**：`ITERATIONS=$((DURATION_HOURS * 3600 / INTERVAL / CONCURRENT))` — 除以 CONCURRENT 无意义。

**修复**：

```bash
ITERATIONS=$((DURATION_HOURS * 3600 / INTERVAL))
```

### 优化 3：线程安全测试增加断言脚本

当前 Phase 3 只有执行方法，缺少自动化结果判定。增加：

```bash
check_thread_safety() {
    local log=$1
    local result="PASS"

    # 检查 ASan 报错
    if grep -q "ERROR: AddressSanitizer" "$log"; then
        echo "FAIL: ASan detected memory error"
        result="FAIL"
    fi

    # 检查 double-free
    if grep -qi "double-free\|attempting free on unallocated" "$log"; then
        echo "FAIL: double-free detected"
        result="FAIL"
    fi

    # 检查死锁（fs_cli 30s 超时）
    if ! timeout 30 fs_cli -x "asr status" >/dev/null 2>&1; then
        echo "FAIL: FreeSWITCH unresponsive (possible deadlock)"
        result="FAIL"
    fi

    echo "Thread safety check: $result"
}
```

### 优化 4：异常场景测试脚本化

当前 Phase 5 只有表格描述，补充为可执行脚本：

```bash
# E5.1 WS 中途断网测试
test_ws_disconnect() {
    ALIYUN_IP=$(dig +short nls-gateway-cn-shanghai.aliyuncs.com | head -1)
    fs_cli -x "originate sofia/internal/8001@localhost &lua(continuous_asr.lua)" &
    CALL_PID=$!
    sleep 3
    iptables -A OUTPUT -d $ALIYUN_IP -j DROP
    sleep 5
    iptables -D OUTPUT -d $ALIYUN_IP -j DROP
    wait $CALL_PID
}
```

## 测试执行计划

| 阶段 | 工具 | 时长 | 前置条件 |
|------|------|------|----------|
| Phase 1: 冒烟测试 | Lua | 1h | FS 运行 + 阿里云 ASR 可用 |
| Phase 3: 线程安全专项 | Lua + ASan + 断言脚本 | 4h | Phase 1 通过 |
| Phase 5: 异常场景 | Lua + 网络工具 + 断言脚本 | 4h | Phase 1 通过 |
| Phase 6: SIP 并发容量 | SIPp + ESL monitor | 4h | Phase 1 通过 + SIPp 安装 + Sofia 配置 |
| Phase 7: 长时间稳定性 | SIPp + ESL monitor | 24h | Phase 3+6 通过 |

**并行策略**：Phase 3 和 Phase 6 可并行（测试不同层面），其余串行。串行约 37h，并行可缩减到 ~28h。

## 与原方案的对比

| 维度 | 原方案 | 混合方案 |
|------|--------|----------|
| 并发容量测试 | originate（绕过 SIP 栈） | SIPp（真实 SIP 信令 + RTP） |
| CPS 控制 | bash `&` + sleep（粗略） | SIPp `-r` `-rp`（精确） |
| SIP 级指标 | 无 | SIPp 内置（呼叫建立时延、应答率、错误分布） |
| 线程安全测试 | Lua 脚本（保持不变） | Lua + ASan + 自动断言 |
| 监控方式 | 4 终端手动 watch | 持久 ESL 连接 + 时序 CSV + 自动告警 |
| 结果判定 | 人工判断 | 自动 pass/fail 判定 + 告警阈值 |
| 长时间稳定性 | originate 循环 | SIPp 精确产压 + ESL 全程监控 |
