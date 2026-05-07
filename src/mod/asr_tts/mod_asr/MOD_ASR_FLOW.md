# mod_asr 程序调用流程

## 模块架构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                    FreeSWITCH Core                               │
│              (switch_core_asr.c / dialplan)                      │
└──────────────────────────┬──────────────────────────────────────┘
                           │ ASR接口回调
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    mod_asr 框架层                                │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐    │
│  │  mod_asr.c   │  │asr_session.c │  │ asr_provider.c     │    │
│  │ (入口+ASR    │  │(会话管理+    │  │ (Provider注册/     │    │
│  │  接口回调)   │  │ worker线程)  │  │  查找框架)         │    │
│  └──────┬───────┘  └──────┬───────┘  └────────┬───────────┘    │
└─────────┼─────────────────┼───────────────────┼────────────────┘
          │                 │                   │
          │    ┌────────────┼───────────────────┘
          │    │            │
          ▼    ▼            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Provider 接口层 (asr_provider_interface_t)          │
│                    函数指针表（策略模式）                          │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
┌─────────────────┐ ┌─────────────┐ ┌─────────────┐
│ provider_aliyun │ │ provider_xx │ │ provider_yy │  ← 可扩展
│   (阿里云)      │ │  (预留)     │ │  (预留)     │
└────────┬────────┘ └─────────────┘ └─────────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐ ┌────────────┐
│WS客户端│ │REST客户端   │
│(原始   │ │(switch_curl│
│socket+ │ │ HTTP封装)  │
│OpenSSL)│ │            │
└────────┘ └────────────┘
```

## 文件职责

| 文件 | 职责 |
|------|------|
| `mod_asr.h` | 全局头文件：枚举、结构体、接口声明 |
| `provider_aliyun.h` | 阿里云常量URL、会话上下文结构体 |
| `mod_asr.c` | 模块入口：加载/卸载、ASR接口回调、CLI命令、配置加载 |
| `asr_provider.c` | Provider框架：注册、查找、列举 |
| `asr_session.c` | 会话管理：创建/销毁、worker线程、音频缓冲、结果存取 |
| `asr_ws_client.c` | WebSocket客户端：原始socket+OpenSSL实现RFC 6455 |
| `asr_rest_client.c` | REST客户端：switch_curl HTTP请求封装 |
| `provider_aliyun.c` | 阿里云Provider：NLS协议、Token获取、WS/REST双模式 |

## 1. 模块加载流程

```
FreeSWITCH启动
  │
  ▼
mod_asr_load()                          [mod_asr.c]
  ├── 初始化全局状态(asr_globals)
  │   ├── pool = 模块内存池
  │   ├── mutex = 嵌套互斥锁
  │   ├── providers = 空哈希表
  │   └── sessions = 空哈希表
  │
  ├── mod_asr_do_config()               [mod_asr.c]
  │   ├── 打开 asr.conf XML
  │   ├── 解析 <settings> 下的参数
  │   │   ├── default-provider (默认"aliyun")
  │   │   ├── default-mode (默认"websocket")
  │   │   └── max-sessions (默认100)
  │   └── 设置默认值
  │
  ├── asr_provider_aliyun_load()        [provider_aliyun.c]
  │   ├── aliyun_load_config()
  │   │   ├── 打开 asr.conf XML
  │   │   ├── 解析 <providers>/<provider name="aliyun"> 下的参数
  │   │   │   ├── access-key-id (必填)
  │   │   │   ├── access-key-secret (必填)
  │   │   │   ├── app-key (必填)
  │   │   │   ├── region (默认"cn-shanghai")
  │   │   │   ├── format (默认"pcm")
  │   │   │   ├── sample-rate (默认16000)
  │   │   │   ├── enable-intermediate-result
  │   │   │   ├── enable-punctuation
  │   │   │   └── rest-timeout (默认10000)
  │   │   └── 校验必填字段
  │   │
  │   └── asr_provider_register(&aliyun_provider)  [asr_provider.c]
  │       └── switch_core_hash_insert(providers, "aliyun", provider)
  │
  ├── 注册ASR接口
  │   └── switch_asr_interface_t → 绑定12个回调函数
  │
  └── 注册CLI命令
      └── "asr" → mod_asr_api (status/providers/list)
```

## 2. 一次ASR识别的完整流程（WebSocket模式）

```
Dialplan执行:
  <action application="play_and_detect_speech"
          data="silence_stream://2000 asr:aliyun {mode=websocket}"/>
  │
  ▼
FreeSWITCH调用 mod_asr_asr_open()           [mod_asr.c]
  ├── 解析dest="aliyun:websocket"
  │   ├── provider_name = "aliyun"
  │   └── mode = ASR_MODE_WEBSOCKET
  │
  ├── asr_session_create()                   [asr_session.c]
  │   ├── asr_provider_find("aliyun")        [asr_provider.c]
  │   │   └── switch_core_hash_find → 返回aliyun_provider
  │   ├── switch_core_alloc分配session
  │   ├── 初始化mutex/cond/audio_buffer
  │   └── 注册到全局sessions哈希表
  │
  ├── provider->open() = aliyun_asr_open()   [provider_aliyun.c]
  │   ├── 创建aliyun_asr_ctx_t (provider私有数据)
  │   │   └── 从aliyun_globals复制配置到会话级
  │   │
  │   ├── aliyun_get_token()                 [provider_aliyun.c]
  │   │   ├── 构造规范化查询串 (AccessKeyId, Action, Format...)
  │   │   ├── 构造待签名字符串 (GET&%2F&URL编码查询串)
  │   │   ├── aliyun_hmac_sha1_base64() 签名
  │   │   ├── 构造完整URL (POP API + 查询串 + 签名)
  │   │   ├── switch_curl_easy_perform() 发送GET请求
  │   │   └── cJSON_Parse解析Token
  │   │
  │   ├── asr_ws_connect()                   [asr_ws_client.c]
  │   │   ├── ws_parse_url() 解析wss://URL
  │   │   ├── ws_tcp_connect() DNS解析+TCP连接
  │   │   ├── SSL_connect() TLS握手
  │   │   ├── ws_perform_handshake() WS升级握手
  │   │   │   ├── 生成Sec-WebSocket-Key (Base64随机数)
  │   │   │   ├── 发送HTTP GET + Upgrade头
  │   │   │   ├── 接收101 Switching Protocols
  │   │   │   └── 保存leftover数据
  │   │   └── 设置socket超时1秒
  │   │
  │   ├── asr_ws_send_text(StartTranscription)  发送开始命令
  │   │   └── ws_send_frame(WS_OPCODE_TEXT, JSON命令)
  │   │
  │   └── 等待TranscriptionStarted事件 (5秒超时轮询)
  │       ├── asr_ws_has_data() 检查数据
  │       ├── asr_ws_recv_text() 接收WS帧
  │       └── aliyun_process_ws_result() 解析事件
  │
  └── asr_session_start_worker()             [asr_session.c]
      └── switch_thread_create(worker_thread)

═══════════════════════════════════════════════════════════════════

Worker线程运行: asr_session_worker_thread()   [asr_session.c]

  while(running) {
    ┌─────────────────────────────────────────────────┐
    │ 1. cond_timedwait(200ms) 等待音频               │
    │                                                 │
    │ 2. 如果audio_buffer有数据:                       │
    │    ├── switch_buffer_read() 读取音频             │
    │    ├── idle_count = 0  (重置空闲计数)            │
    │    └── provider->feed() = aliyun_asr_feed()     │
    │        [provider_aliyun.c]                      │
    │        └── WS模式: asr_ws_send_binary()         │
    │            └── ws_send_frame(WS_OPCODE_BINARY)  │
    │                                                 │
    │ 3. 如果audio_buffer无数据:                       │
    │    └── idle_count++  (REST模式用)               │
    │                                                 │
    │ 4. provider->poll_results()                     │
    │    = aliyun_asr_poll_results()  [provider_aliyun.c]│
    │    ├── WS模式: 循环接收WS消息                    │
    │    │   ├── asr_ws_has_data() → asr_ws_recv_text()│
    │    │   └── aliyun_process_ws_result()            │
    │    │       ├── TranscriptionResultChanged: 中间结果│
    │    │       ├── SentenceBegin: 设置START_OF_SPEECH │
    │    │       ├── SentenceEnd:                      │
    │    │       │   └── asr_session_set_result()      │
    │    │       │       ├── 设置result_text/result_xml │
    │    │       │       ├── state = RESULT_READY      │
    │    │       │       └── 设置HAS_TEXT标志           │
    │    │       └── TaskFailed: 错误日志               │
    │    └── REST模式: 检查idle_count>=3时自动提交     │
    │        └── aliyun_rest_recognize()               │
    └─────────────────────────────────────────────────┘
  }

═══════════════════════════════════════════════════════════════════

FreeSWITCH主线程: 轮询结果

  mod_asr_asr_check_results()               [mod_asr.c]
    └── 检查session->state == RESULT_READY?
        或 检查flags (HAS_TEXT/NOINPUT/NOMATCH等)

  mod_asr_asr_get_results()                 [mod_asr.c]
    ├── 检查BARGE标志 → 返回BREAK (打断播放)
    ├── 检查START_OF_SPEECH → 返回BREAK (触发begin-speaking事件)
    ├── provider->get_results() = aliyun_asr_get_results()
    │   └── asr_session_get_result()        [asr_session.c]
    │       ├── 复制result_xml
    │       ├── 清除HAS_TEXT标志
    │       ├── 释放result_text/result_xml
    │       ├── state = LISTENING  ← 支持连续识别！
    │       └── 清除START_OF_SPEECH
    ├── 或返回NOINPUT XML
    └── 或返回NOMATCH XML

═══════════════════════════════════════════════════════════════════

连续识别循环（同一WS连接多轮结果）:

  SentenceEnd → set_result() → RESULT_READY
    → get_results() → LISTENING (不关闭WS)
    → 下一轮SentenceEnd → set_result() → RESULT_READY
    → get_results() → LISTENING ...
    → 直到通话结束

═══════════════════════════════════════════════════════════════════

会话关闭:

  mod_asr_asr_close()                       [mod_asr.c]
    └── asr_session_destroy()                [asr_session.c]
        ├── asr_session_stop_worker()
        │   ├── running = FALSE
        │   ├── cond_signal() 唤醒worker
        │   └── switch_thread_join() 等待退出
        │
        ├── provider->close() = aliyun_asr_close()  [provider_aliyun.c]
        │   ├── WS模式:
        │   │   ├── asr_ws_send_text(StopTranscription)
        │   │   └── asr_ws_disconnect()     [asr_ws_client.c]
        │   │       ├── ws_send_frame(WS_OPCODE_CLOSE)
        │   │       ├── SSL_shutdown() + SSL_free()
        │   │       └── close(sockfd)
        │   └── REST模式:
        │       └── switch_buffer_destroy()
        │
        ├── 全局注销 (hash_delete + active_sessions--)
        ├── switch_buffer_destroy(audio_buffer)
        └── 释放result_text/result_xml
```

## 3. REST模式流程

```
与WS模式的差异:

  aliyun_asr_open()                         [provider_aliyun.c]
    ├── 同样获取Token
    ├── 不建立WS连接
    └── 创建rest_audio_buffer (最大1MB)

  aliyun_asr_feed()                         [provider_aliyun.c]
    └── 音频写入rest_audio_buffer (而非发送WS帧)

  aliyun_asr_poll_results()                 [provider_aliyun.c]
    └── 当idle_count >= ASR_REST_IDLE_THRESHOLD (3)
        且rest_submitted == FALSE
        且buffer有数据时:
        ├── rest_submitted = TRUE (防重复提交)
        └── aliyun_rest_recognize()          [provider_aliyun.c]
            ├── switch_buffer_read() 取出全部音频
            ├── 构造REST URL (appkey/format/sample_rate参数)
            ├── 设置HTTP头 (Content-Type + X-NLS-Token)
            ├── asr_rest_request()            [asr_rest_client.c]
            │   ├── switch_curl_easy_init()
            │   ├── 设置URL/SSL/超时/方法(POST)
            │   ├── switch_curl_easy_perform()
            │   └── 检查HTTP状态码(2xx)
            └── cJSON_Parse解析result字段
                └── asr_session_set_result()
```

## 4. 数据流汇总

### 音频流

```
FreeSWITCH媒体线程
  │ mod_asr_asr_feed(ah, data, len)
  ▼
asr_session_feed()                          [asr_session.c]
  │ switch_buffer_write(audio_buffer, data, len)
  │ cond_signal() 唤醒worker
  ▼
asr_session_worker_thread()                 [asr_session.c]
  │ switch_buffer_read(audio_buffer, data, avail)
  ▼
provider->feed() = aliyun_asr_feed()        [provider_aliyun.c]
  │
  ├── WS: asr_ws_send_binary()              [asr_ws_client.c]
  │       └── ws_send_frame(WS_OPCODE_BINARY)
  │           └── SSL_write() → 阿里云NLS服务器
  │
  └── REST: switch_buffer_write(rest_audio_buffer)
            (等待poll_results触发提交)
```

### 结果流

```
阿里云NLS服务器
  │ WS Binary/Text帧
  ▼
asr_ws_recv_text()                          [asr_ws_client.c]
  │ ws_recv_raw() → 解析WS帧 → 返回文本
  ▼
aliyun_process_ws_result()                  [provider_aliyun.c]
  │ cJSON_Parse → 匹配事件名
  │ SentenceEnd → 提取result/confidence
  ▼
asr_session_set_result()                    [asr_session.c]
  │ result_text = text
  │ result_xml = MRCP格式XML
  │ state = RESULT_READY
  │ flags |= HAS_TEXT
  ▼
mod_asr_asr_check_results()                 [mod_asr.c]
  │ 检查state==RESULT_READY → SUCCESS
  ▼
mod_asr_asr_get_results()                   [mod_asr.c]
  │ asr_session_get_result()
  │ 返回result_xml，重置state=LISTENING
  ▼
FreeSWITCH Core → 触发DETECTED_SPEECH事件
```

## 5. 关键设计决策

| 决策 | 原因 |
|------|------|
| Worker线程模型 | 避免WS接收和provider->feed()阻塞FreeSWITCH主线程 |
| cond_timedwait(200ms) | 兼顾响应速度和CPU效率；200ms超时保证poll_results定期执行 |
| provider->open()在主线程调用 | 同步检测连接错误，及时反馈给FreeSWITCH |
| WS客户端用原始socket+OpenSSL | libcurl WebSocket支持需7.86+，FreeSWITCH内置版本不满足 |
| REST自动提交(3x200ms空闲) | 无需显式结束信号，600ms无新音频即认为一句话结束 |
| session->ws_handle与provider_private分离 | ws_handle由框架管理(连接/断开)，provider_private由provider管理 |
| get_results后state回到LISTENING | 支持连续识别：同一WS连接接收多轮SentenceEnd结果 |
| 全局配置复制到会话级 | 会话可通过text_param覆盖默认值，互不影响 |

## 6. 线程安全模型

```
┌─────────────────┐     ┌──────────────────┐
│  FreeSWITCH     │     │  Worker Thread   │
│  主线程/媒体线程 │     │  (per session)   │
├─────────────────┤     ├──────────────────┤
│ asr_open()      │     │ worker_thread()  │
│ asr_close()     │     │   provider feed  │
│ asr_feed()      │────▸│   poll_results   │
│ check_results() │     │                  │
│ get_results()   │◂────│  set_result()    │
│ text_param()    │     │                  │
└─────────────────┘     └──────────────────┘

共享数据保护:
  session->mutex 保护:
    - state, flags, result_text, result_xml
    - audio_buffer (feed写/worker读)

  conn->write_mutex 保护:
    - WS发送操作 (主线程close发StopTranscription + worker发音频)

  asr_globals.mutex 保护:
    - providers/sessions哈希表
    - active_sessions计数
```

## 7. 配置文件格式 (conf/autoload_configs/asr.conf.xml)

```xml
<configuration name="asr.conf" description="ASR Module">
  <settings>
    <param name="default-provider" value="aliyun"/>
    <param name="default-mode" value="websocket"/>
    <param name="max-sessions" value="100"/>
  </settings>

  <providers>
    <provider name="aliyun">
      <param name="access-key-id" value="LTAI5t..."/>
      <param name="access-key-secret" value="xxx..."/>
      <param name="app-key" value="NXPxxx..."/>
      <param name="region" value="cn-shanghai"/>
      <param name="format" value="pcm"/>
      <param name="sample-rate" value="16000"/>
      <param name="enable-intermediate-result" value="true"/>
      <param name="enable-punctuation" value="true"/>
      <param name="rest-timeout" value="10000"/>
    </provider>
  </providers>
</configuration>
```

## 8. Dialplan使用示例

```xml
<!-- WebSocket实时流式识别 -->
<action application="play_and_detect_speech"
        data="silence_stream://2000 asr:aliyun {mode=websocket}"/>

<!-- REST一句话识别 -->
<action application="play_and_detect_speech"
        data="silence_stream://2000 asr:aliyun {mode=rest}"/>

<!-- 使用默认配置 -->
<action application="play_and_detect_speech"
        data="silence_stream://2000 asr:aliyun"/>

<!-- 获取识别结果 -->
<action application="set" data="asr_result=${detect_speech_result}"/>
```

## 9. CLI命令

```
fs_cli> asr status       # 查看模块状态（默认provider/mode/会话数）
fs_cli> asr providers    # 列出已注册的provider及其接口函数
fs_cli> asr list         # 列出当前活跃的ASR会话
```

## 10. 状态机

```
会话状态 (asr_session_state_t):

  IDLE ──asr_session_start_worker()──▸ LISTENING
                                           │
                              SentenceEnd  │  get_results()
                              set_result() │  清除结果+重置标志
                                           │
                                       RESULT_READY
                                           │
                              get_results() │
                              state=LISTENING│
                                           ▼
                                       LISTENING  ←── 连续识别循环
                                           │
                              asr_session_stop_worker()
                                           │
                                           ▼
                                       CLOSED

标志位 (asr_session_flag_t) 与状态的配合:

  LISTENING + START_OF_SPEECH → get_results返回BREAK (begin-speaking事件)
  RESULT_READY + HAS_TEXT     → get_results返回识别文本XML
  RESULT_READY + NOINPUT      → get_results返回<noinput/>XML
  RESULT_READY + NOMATCH      → get_results返回<nomatch/>XML
  CLOSED                      → feed返回BREAK
```
