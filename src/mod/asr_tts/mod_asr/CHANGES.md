# mod_asr 改动总结与测试指南

## 一、项目概述

在 FreeSWITCH 1.10.6 中新增 `mod_asr` 模块，实现对云 ASR 服务的原生支持。当前已实现阿里云智能语音 ASR Provider，架构设计支持后续扩展更多 Provider（如腾讯云、科大讯飞等）。

### 核心能力

- **WebSocket 流式识别**：实时推送音频帧，实时接收中间/最终识别结果
- **REST 一句话识别**：缓冲音频数据，空闲后自动提交一次性识别请求
- **Dialplan 集成**：通过 `switch_asr_interface_t` 标准 ASR 接口，可在拨号计划中使用 `play_and_detect_speech`
- **fs_cli 管理**：通过 `asr` API 命令查看模块状态、Provider 列表、活跃会话

---

## 二、文件清单与改动目的

### 新增文件

| 文件 | 行数 | 目的 |
|------|------|------|
| `src/mod/asr_tts/mod_asr/mod_asr.h` | 148 | 公共头文件：Provider 接口定义、会话结构体、全局配置结构、函数声明 |
| `src/mod/asr_tts/mod_asr/mod_asr.c` | 443 | 模块入口：`SWITCH_MODULE_LOAD/SHUTDOWN`、`switch_asr_interface_t` 回调实现、API 命令 |
| `src/mod/asr_tts/mod_asr/asr_provider.c` | 55 | Provider 框架：注册、查找、列表功能 |
| `src/mod/asr_tts/mod_asr/asr_session.c` | 224 | 会话管理：创建/销毁、工作线程生命周期、音频缓冲读写、结果存取 |
| `src/mod/asr_tts/mod_asr/asr_ws_client.c` | 484 | WebSocket 客户端：原始 Socket + OpenSSL 实现 WebSocket 协议，非阻塞数据检测 |
| `src/mod/asr_tts/mod_asr/asr_rest_client.c` | 144 | REST HTTP 客户端：基于 switch_curl 的通用 HTTP 请求封装 |
| `src/mod/asr_tts/mod_asr/provider_aliyun.c` | 420 | 阿里云 Provider：Token 获取、WS 连接/识别/结果处理、REST 自动提交 |
| `src/mod/asr_tts/mod_asr/provider_aliyun.h` | 39 | 阿里云常量定义（WS/REST/Token URL）和 Provider 会话上下文结构体 |
| `src/mod/asr_tts/mod_asr/Makefile.am` | 9 | Autotools 构建定义：6 个源文件编译，链接 libfreeswitch.la |
| `conf/vanilla/autoload_configs/asr.conf.xml` | 29 | XML 配置模板：全局设置 + 阿里云 Provider 配置 |

### 修改文件

| 文件 | 改动 |
|------|------|
| `modules.conf` | 新增 `asr_tts/mod_asr` 行（第 63 行），启用模块编译 |

---

## 三、架构设计

```
┌───────────────────────────────────────────────────┐
│  FreeSWITCH Core (switch_core_asr.c)              │
│  调用 switch_asr_interface_t 回调                  │
├───────────────────────────────────────────────────┤
│  mod_asr.c                                        │
│  asr_open / asr_close / asr_feed / asr_check_     │
│  results / asr_get_results / asr_text_param ...   │
├───────────────────────────────────────────────────┤
│  asr_session.c (工作线程)                          │
│  ┌─────────────────────────────────────┐          │
│  │ while(running):                     │          │
│  │   cond_timedwait(200ms)             │          │
│  │   从 audio_buffer 取音频 → feed()   │          │
│  │   poll_results() ← 非阻塞结果轮询   │          │
│  └─────────────────────────────────────┘          │
├──────────┬────────────────────────────────────────┤
│ aliyun   │ (future: tencent, xfyun, ...)         │
│ provider │                                       │
├──────────┴────────────────────────────────────────┤
│  asr_ws_client.c  │  asr_rest_client.c            │
│  (Socket+OpenSSL) │  (switch_curl)                │
└───────────────────────────────────────────────────┘
```

### 关键设计决策

1. **`provider_private` 与 `ws_handle` 分离**：`session->provider_private` 存储 Provider 上下文（如 `aliyun_asr_ctx_t`），`session->ws_handle` 存储 WebSocket 连接句柄（`ws_conn_t`），避免互相覆盖。

2. **工作线程 + 非阻塞轮询**：每个 ASR 会话一个独立工作线程，使用 `switch_thread_cond_timedwait(200ms)` 定时唤醒。每次循环都调用 `provider->poll_results()`，实现：
   - **WS 模式**：通过 `asr_ws_has_data()` 非阻塞检测，有数据时才读取 WebSocket 帧
   - **REST 模式**：通过 `idle_count` 计数，连续 3 个空闲周期（约 600ms）后自动提交识别请求

3. **`check_results` 轻量化**：FreeSWITCH 媒体线程调用的 `check_results` 只检查 session 状态标志，不执行任何阻塞操作。所有网络 I/O 和结果解析都在工作线程中完成。

4. **Provider 可插拔**：通过 `asr_provider_interface_t` 定义统一接口，新增 Provider 只需实现该接口并调用 `asr_provider_register()` 注册。

---

## 四、各模块改动详解

### 4.1 mod_asr.h — 公共类型与接口定义

**改动目的**：定义所有模块共享的类型、接口、全局变量声明。

**关键内容**：
- `asr_session_flag_t`：9 个会话标志位（HAS_TEXT, READY, BARGE, NOINPUT 等）
- `asr_session_state_t`：6 种会话状态（IDLE → LISTENING → RESULT_READY / ERROR → CLOSED）
- `asr_mode_t`：WEBSOCKET / REST 两种识别模式
- `asr_provider_interface_t`：Provider 接口，12 个回调 + `poll_results`（新增）
- `asr_session_t`：会话结构体，含 `ws_handle`（WS 句柄）和 `idle_count`（REST 空闲计数器）
- `asr_globals_t`：全局配置，含 providers/sessions 哈希表

**新增字段说明**：
- `poll_results`：Provider 回调，由工作线程定期调用，处理 WS 接收或 REST 自动提交
- `ws_handle`：独立于 `provider_private`，专存 WebSocket 连接
- `idle_count`：REST 模式空闲计数，达到 `ASR_REST_IDLE_THRESHOLD`(3) 后触发识别

### 4.2 mod_asr.c — 模块入口与 ASR 接口

**改动目的**：实现 FreeSWITCH 标准 ASR 接口，衔接拨号计划和 Provider。

**关键实现**：
- `mod_asr_asr_open`：解析 dest 参数（格式 `provider:mode`），创建 session，启动工作线程
- `mod_asr_asr_feed`：将音频数据写入 session 的 audio_buffer，通知工作线程
- `mod_asr_asr_check_results`：仅检查 session 状态标志（非阻塞），不调用 provider
- `mod_asr_asr_get_results`：返回 XML 格式识别结果，处理 noinput/nomatch 场景
- `mod_asr_api`：fs_cli 命令入口，支持 `asr status`/`asr providers`/`asr list`

### 4.3 asr_session.c — 会话管理与工作线程

**改动目的**：管理 ASR 会话生命周期，工作线程驱动音频推送和结果轮询。

**工作线程流程**：
```
worker_thread:
  provider->open()           // 建立 WS/REST 连接
  while(running):
    cond_timedwait(200ms)    // 等待音频或超时
    if (audio_buffer 有数据):
      读取音频 → provider->feed()   // 发送到 ASR 服务
      idle_count = 0
    else:
      idle_count++           // REST 模式空闲计数
    provider->poll_results() // 非阻塞结果轮询
  provider->close()          // 断开连接，释放资源
```

### 4.4 asr_ws_client.c — WebSocket 客户端

**改动目的**：提供 WebSocket 连接、帧收发、非阻塞数据检测功能。

**关键实现**：
- 基于 raw socket + OpenSSL，避免依赖 curl 7.86+ 的 WebSocket 支持
- `asr_ws_connect`：DNS 解析 → TCP 连接 → SSL 握手 → HTTP Upgrade → 设置 1s socket 接收超时
- `asr_ws_has_data`：非阻塞检测（`select()` + `SSL_pending()`），零超时返回是否有可读数据
- `asr_ws_recv_text`：读取 WebSocket 帧，解析 opcode 和 payload，返回文本内容
- 所有 WS 连接信息存储在 `session->ws_handle`（`ws_conn_t*`），不覆盖 `provider_private`

### 4.5 asr_rest_client.c — REST HTTP 客户端

**改动目的**：封装通用 HTTP 请求，供 Provider 的 REST 识别使用。

**关键实现**：
- 基于 `switch_curl` API（FreeSWITCH 内置 curl 封装）
- 支持 GET/POST/PUT/DELETE 方法
- 支持自定义请求头和请求体
- 响应通过回调函数累积到动态缓冲区

### 4.6 provider_aliyun.c — 阿里云 ASR Provider

**改动目的**：实现阿里云智能语音 ASR 服务的对接。

**WS 模式流程**：
```
open:
  获取 Token → WS 连接（token 放在 URL 参数 + Header）

feed:
  首次 feed → 发送 StartRecognition JSON 指令
  后续 feed → 发送音频二进制帧

poll_results:
  if (asr_ws_has_data):
    读取 WS 文本帧 → 解析 JSON
    SpeechRecognizerResultChanged → 记录中间结果（日志）
    SpeechRecognizerCompleted / RecognitionCompleted → 设置最终结果
    TaskFailed → 记录错误

close:
  发送 StopRecognition → 断开 WS
```

**REST 模式流程**：
```
open:
  获取 Token → 创建 rest_audio_buffer

feed:
  音频写入 rest_audio_buffer（最大 1MB）

poll_results:
  if (idle_count >= 3 && !rest_submitted && buffer 有数据):
    rest_submitted = true
    读取缓冲区全部音频 → HTTP POST 到阿里云 REST API
    解析 JSON 响应 → 设置识别结果

close:
  销毁 rest_audio_buffer
```

**配置参数**：
| 参数 | 说明 | 默认值 |
|------|------|--------|
| access-key-id | 阿里云 AccessKey ID | 无（必填） |
| access-key-secret | 阿里云 AccessKey Secret | 无（必填） |
| app-key | 阿里云智能语音 AppKey | 无（必填） |
| region | 区域 | cn-shanghai |
| format | 音频格式 | pcm |
| sample-rate | 采样率 | 16000 |
| enable-intermediate-result | 中间结果 | true |
| enable-punctuation | 标点预测 | true |
| rest-timeout | REST 超时(ms) | 10000 |

---

## 五、编译产出

```bash
# 编译产物
src/mod/asr_tts/mod_asr/.libs/mod_asr.so   # 291,480 bytes

# 编译通过 FreeSWITCH 严格 C90 标志：
# -std=c99 -pedantic -Wdeclaration-after-statement -Werror
```

---

## 六、测试流程

### 前置条件

1. FreeSWITCH 已编译安装（`make install`）
2. 阿里云智能语音服务已开通，获取 AccessKey ID、AccessKey Secret、AppKey
3. FreeSWITCH 已注册至少一个 SIP 终端（如 Zoiper / MicroSIP）

### 步骤 1：部署配置

```bash
# 1. 安装模块
make install

# 2. 编辑配置，填入真实的阿里云密钥
vi /usr/local/freeswitch/conf/autoload_configs/asr.conf.xml
```

将 `$${ALIYUN_AK_ID}`、`$${ALIYUN_AK_SECRET}`、`$${ALIYUN_ASR_APP_KEY}` 替换为真实值，或在 `vars.xml` 中定义这些变量：

```xml
<!-- vars.xml 中添加 -->
<X-PRE-PROCESS cmd="set" data="ALIYUN_AK_ID=你的AccessKeyId"/>
<X-PRE-PROCESS cmd="set" data="ALIYUN_AK_SECRET=你的AccessKeySecret"/>
<X-PRE-PROCESS cmd="set" data="ALIYUN_ASR_APP_KEY=你的AppKey"/>
```

### 步骤 2：启动与模块加载

```bash
# 启动 FreeSWITCH
/usr/local/freeswitch/bin/freeswitch -ncwait

# 进入 fs_cli
fs_cli
```

在 fs_cli 中：

```
# 手动加载模块
load mod_asr

# 预期输出：
# [INFO] mod_asr loaded successfully
# [INFO] Aliyun ASR provider registered
```

### 步骤 3：验证 API 命令

```
# 查看模块状态
asr status

# 预期输出：
# mod_asr Status:
#   Default Provider: aliyun
#   Default Mode: websocket
#   Max Sessions: 100
#   Active Sessions: 0

# 查看已注册 Provider
asr providers

# 预期输出：
# ASR Providers:
#   aliyun                open=0x...  feed=0x...  get_results=0x...

# 查看活跃会话（此时应为空）
asr list

# 预期输出：
# Active ASR Sessions:
```

**验证标准**：三个命令均能正常返回，aliyun Provider 已注册。

### 步骤 4：验证 Token 获取

```
# 开启 DEBUG 日志
fs_cli -x "log debug"

# 尝试触发 ASR open（通过 dialplan 或直接调用）
# 观察 FreeSWITCH 日志，应看到：
# [INFO] Aliyun ASR opened: mode=websocket, app_key=xxx
# 或
# [ERROR] Failed to get Aliyun token  ← Token 获取失败，检查密钥配置
```

### 步骤 5：Dialplan 集成测试 — WebSocket 模式

在 `conf/vanilla/dialplan/default.xml` 中添加测试分机号：

```xml
<extension name="asr_ws_test">
  <condition field="destination_number" expression="^5001$">
    <action application="answer"/>
    <action application="play_and_detect_speech" data="silence_stream://2000 asr:aliyun:websocket"/>
    <action application="log" data="INFO ASR Result: ${detect_speech_result}"/>
    <action application="play_and_detect_speech" data="silence_stream://2000 asr:aliyun:websocket"/>
  </condition>
</extension>
```

```bash
# 重新加载拨号计划
fs_cli -x "reloadxml"
```

**测试操作**：
1. 用 SIP 软电话拨打 `5001`
2. 对着麦克风说话
3. 观察 fs_cli 日志输出

**预期日志**：
```
[INFO] Aliyun ASR opened: mode=websocket, app_key=xxx
[INFO] WebSocket connected: wss://nls-gateway-cn-shanghai.aliyuncs.com/ws/v1?token=xxx
[DEBUG] Aliyun intermediate: 你好
[DEBUG] Aliyun intermediate: 你好世界
[INFO] ASR Result: <result ...><input mode="speech">你好世界</input></result>
```

**验证标准**：
- WebSocket 连接成功建立
- 中间结果在日志中持续输出
- 最终识别结果返回到拨号计划变量 `detect_speech_result`

### 步骤 6：Dialplan 集成测试 — REST 模式

```xml
<extension name="asr_rest_test">
  <condition field="destination_number" expression="^5002$">
    <action application="answer"/>
    <action application="play_and_detect_speech" data="silence_stream://2000 asr:aliyun:rest"/>
    <action application="log" data="INFO ASR REST Result: ${detect_speech_result}"/>
  </condition>
</extension>
```

```bash
fs_cli -x "reloadxml"
```

**测试操作**：
1. 拨打 `5002`
2. 说一句话后停顿约 1 秒
3. 等待 REST 识别结果

**预期日志**：
```
[INFO] Aliyun ASR opened: mode=rest, app_key=xxx
[INFO] Aliyun REST auto-submit: idle_count=3, audio=xxxxx
[INFO] ASR REST Result: <result ...><input mode="speech">识别的文本</input></result>
```

**验证标准**：
- 音频缓冲后自动提交 REST 请求
- HTTP 请求成功，JSON 响应解析正确
- 识别结果返回到拨号计划

### 步骤 7：参数传递测试

通过拨号计划设置 Provider 参数：

```xml
<extension name="asr_param_test">
  <condition field="destination_number" expression="^5003$">
    <action application="answer"/>
    <action application="set" data="detect_speech_params=app-key=xxx;format=wav;sample-rate=8000"/>
    <action application="play_and_detect_speech" data="silence_stream://2000 asr:aliyun:websocket"/>
  </condition>
</extension>
```

**验证标准**：日志中显示 `app_key=xxx` 被正确设置，format/sample-rate 参数生效。

### 步骤 8：多路并发测试

同时用 2-3 个 SIP 终端拨打 ASR 分机号，验证：
- 每路会话独立工作
- `asr list` 显示多个活跃会话
- 各路会话的识别结果互不干扰

**验证命令**：
```
asr list

# 预期输出：
# Active ASR Sessions:
#   ID: 0x...  Provider: aliyun  Mode: ws  State: 2
#   ID: 0x...  Provider: aliyun  Mode: ws  State: 2
```

### 步骤 9：资源释放验证

挂断所有通话后：

```
asr list

# 预期输出：
# Active ASR Sessions:
# （空，无活跃会话）

asr status

# 预期输出：
#   Active Sessions: 0
```

**验证标准**：会话正确销毁，无内存泄漏告警。

### 步骤 10：模块卸载测试

```
unload mod_asr

# 预期输出：
# [INFO] mod_asr shutdown

# 再次加载
load mod_asr

# 预期输出：
# [INFO] mod_asr loaded successfully
```

---

## 七、常见问题排查

| 现象 | 可能原因 | 排查方法 |
|------|---------|---------|
| `load mod_asr` 失败 | 编译未安装或依赖库缺失 | 检查 `make install`，`ldd mod_asr.so` |
| "Aliyun ASR provider requires access-key-id" | 配置缺失 | 检查 `asr.conf.xml` 中的密钥配置 |
| "Failed to get Aliyun token" | AccessKey 无效或网络不通 | 用 `curl` 直接测试 Token URL |
| "TCP connect failed" | 网络防火墙 | `telnet nls-gateway-cn-shanghai.aliyuncs.com 443` |
| "SSL handshake failed" | 证书或 TLS 版本问题 | 检查 OpenSSL 版本 |
| "WebSocket handshake failed" | Token 无效或 URL 错误 | 检查 Token 是否过期，URL 是否正确 |
| REST 模式无结果 | 空闲超时未触发 | 确认说话后停顿 > 600ms |
| 识别结果乱码 | 音频格式不匹配 | 确认 codec=L16, format=pcm, sample-rate=16000 |
| 会话不释放 | 工作线程未退出 | 检查 `asr list`，确认 `unload` 后无残留 |

---

## 八、扩展指南 — 添加新 Provider

1. 创建 `provider_xxx.h`：定义常量 URL 和 Provider 会话上下文结构体
2. 创建 `provider_xxx.c`：实现 `asr_provider_interface_t` 的所有回调，包括 `poll_results`
3. 在 `mod_asr.h` 中添加 `switch_status_t asr_provider_xxx_load(switch_memory_pool_t *pool);` 声明
4. 在 `mod_asr.c` 的 `mod_asr_load` 中调用 `asr_provider_xxx_load(pool)`
5. 在 `Makefile.am` 的 `mod_asr_la_SOURCES` 中添加 `provider_xxx.c`
6. 在 `asr.conf.xml` 中添加对应 `<provider>` 配置段
7. 运行 `automake --add-missing && ./config.status --file=src/mod/asr_tts/mod_asr/Makefile && make`
