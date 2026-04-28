# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FreeSWITCH 1.10.6 — a Software Defined Telecom Stack. A modular, open-source soft-switch that handles voice, video, and text communication via SIP, WebRTC, and other protocols. Written in C, built with GNU Autotools.

## Build System

```bash
# Full build from source (already configured in this tree)
./configure
make

# Rebuild after configure changes
./configure && make

# Install
make install

# Clean build artifacts
make clean

# Key configure options
./configure --enable-optimization          # Max compiler optimizations
./configure --disable-debug                # Strip debug symbols
./configure --enable-core-odbc-support     # ODBC database support
./configure --enable-zrtp                  # ZRTP encryption
./configure --enable-address-sanitizer     # ASan for memory debugging
./configure --enable-pool-sanitizer        # Sanitizer-friendly pool behavior
```

## Module Build Control

`modules.conf` at the project root controls which modules are built. Lines starting with `#` are excluded. After changing `modules.conf`, re-run `./configure && make`.

## Testing

```bash
# Run all unit tests
cd tests/unit && bash run-tests.sh

# Run a specific unit test
cd tests/unit && bash test.sh <relative_test_path>

# Discover available tests
make print_tests
```

Unit tests are in `tests/unit/` — each subdirectory contains a standalone C test binary (e.g., `switch_core/switch_core.c`, `switch_event/switch_event.c`). Tests require `libfreeswitch.la` to already be built.

## Architecture

### Core Engine (`src/`)

The core is `libfreeswitch`, built from `src/switch_*.c` files. Key subsystems:

- **switch_core.c** — Main loop, initialization, session management
- **switch_core_session.c** — Session lifecycle (the central abstraction: an active call leg)
- **switch_core_state_machine.c** — Channel state machine (CS_NEW → CS_INIT → CS_ROUTING → CS_EXECUTE → CS_HANGUP → CS_REPORTING → CS_DESTROY)
- **switch_core_media.c** — Media negotiation, SDP, codecs
- **switch_core_codec.c** — Codec registration and negotiation
- **switch_channel.c** — Channel variables, state transitions, flags
- **switch_event.c** — Event system (pub/sub for internal events)
- **switch_rtp.c** — RTP media transport
- **switch_ivr*.c** — IVR primitives (play, record, bridge, originate, menu)
- **switch_loadable_module.c** — Dynamic module loading

### Module System (`src/mod/`)

Modules are dynamically loaded shared libraries. Each implements a standard interface defined by `SWITCH_MODULE_DEFINITION(name, load_fn, shutdown_fn, runtime_fn)` in `<switch.h>`. Reference skeleton: `src/mod/applications/mod_skel/`.

Module categories under `src/mod/`:
- **endpoints/** — Protocol interfaces (mod_sofia for SIP, mod_verto for WebRTC, mod_rtmp, etc.)
- **applications/** — Call features (mod_conference, mod_voicemail, mod_fifo, mod_callcenter, mod_dptools, etc.)
- **codecs/** — Audio/video codecs (mod_opus, mod_g729, mod_h26x, etc.)
- **event_handlers/** — External integrations (mod_event_socket/ESL, mod_json_cdr, mod_cdr_csv, etc.)
- **formats/** — File format handlers (mod_sndfile, mod_shout, mod_vlc, etc.)
- **languages/** — Scripting (mod_lua, mod_python, mod_v8/JS, mod_perl, etc.)
- **dialplans/** — Routing logic (mod_dialplan_xml is default)
- **asr_tts/** — Speech synthesis/recognition
- **databases/** — mod_pgsql, mod_mariadb
- **loggers/** — mod_console, mod_syslog, mod_logfile
- **say/** — Language-specific number/phrase rendering
- **xml_int/** — XML CDR, XML CURL, XML RPC interfaces

### Sofia-SIP (`src/mod/endpoints/mod_sofia/`)

The primary SIP stack. Uses the Nokia Sofia-SIP library internally. Key files: `sofia.c` (core logic), `sofia_glue.c` (SIP-SDP bridging), `sofia_reg.c` (registration), `sofia_presence.c` (presence), `sofia_media.c` (media handling), `rtp.c` (RTP).

### Event Socket Library (`libs/esl/`)

Client library for controlling FreeSWITCH via TCP socket (the `mod_event_socket` protocol). Used by `fs_cli` (CLI tool) and `fs_ivrd` (IVR daemon). Language bindings in `libs/esl/{python,perl,ruby,java,lua,php,managed}/`.

### Bundled Libraries (`libs/`)

apr, apr-util, srtp, libteletone, esl, libyuv, libvpx, libzrtp, iksemel, libdingaling, libnatpmp, miniupnpc, libsndfile, libscgi, unimrcp, xmlrpc-c, freetdm.

### Configuration (`conf/`)

XML-based configuration. `conf/vanilla/` is the default full config. Key locations:
- `freeswitch.xml` — Master config (includes all others)
- `vars.xml` — Global variables (IP addresses, domain, etc.)
- `autoload_configs/` — Per-module .conf.xml files
- `sip_profiles/` — Sofia SIP profile definitions (internal, external, etc.)
- `dialplan/` — XML dialplan routing rules
- `directory/` — SIP user directory

## Key Data Structures

- **switch_session** — Active call leg, holds a switch_channel
- **switch_channel** — State machine, variables, flags, caller profile
- **switch_event** — Event system messages (pub/sub)
- **switch_caller_profile** — Caller identity (ANI, DNIS, etc.)
- **switch_loadable_module_interface** — Module registration struct (endpoint, codec, application, API, dialplan interfaces)

## Conventions

- Module names follow `mod_<name>` pattern
- All modules include `<switch.h>` as the single unified header
- Module macros: `SWITCH_MODULE_DEFINITION`, `SWITCH_MODULE_LOAD_FUNCTION`, `SWITCH_MODULE_SHUTDOWN_FUNCTION`, `SWITCH_MODULE_RUNTIME_FUNCTION`
- Channel states are prefixed `CS_` (e.g., `CS_EXECUTE`, `CS_HANGUP`)
- Switch status codes are prefixed `SWITCH_STATUS_` (e.g., `SWITCH_STATUS_SUCCESS`, `SWITCH_STATUS_FALSE`)
- API commands registered via `SWITCH_API_INTERFACE` are callable from `fs_cli` and ESL
- Applications registered via `SWITCH_APPLICATION_INTERFACE` are callable from dialplan via `<action application="..." data="..."/>`
