-- continuous_asr.lua v2 - with robust error handling
session:answer()

local max_rounds = tonumber(session:getVariable("asr_max_rounds")) or 10
local max_silence = tonumber(session:getVariable("asr_max_silence")) or 15
local results = {}
local last_activity = os.time()
local round = 0
local done = false

session:consoleLog("INFO", "=== ASR SCRIPT START ===\n")
session:consoleLog("INFO", string.format("max_rounds=%d max_silence=%d ready=%s\n",
    max_rounds, max_silence, tostring(session:ready())))

session:setVariable("fire_asr_events", "true")

function input_cb(s, type, obj)
    if type == "event" then
        local st = obj:getHeader("Speech-Type") or ""
        if st == "detected-speech" then
            round = round + 1
            local body = obj:getBody() or ""
            s:consoleLog("INFO", string.format("ASR round %d: %s\n", round, body))
            if body ~= "" then
                table.insert(results, body)
            end
            last_activity = os.time()
        elseif st == "begin-speaking" then
            s:consoleLog("INFO", "ASR: Start of speech\n")
            last_activity = os.time()
        elseif st == "closed" then
            s:consoleLog("INFO", "ASR: Engine closed\n")
        end
    end
    return ""
end

session:setInputCallback("input_cb", "")
session:consoleLog("INFO", "=== setInputCallback done ===\n")

-- Use 3-arg detect_speech: engine grammar name
-- This is the correct FreeSWITCH syntax for detect_speech
local ok, err = pcall(function()
    session:execute("detect_speech", "mod_asr {provider=aliyun,mode=websocket}default default")
end)
if not ok then
    session:consoleLog("ERR", "detect_speech FAILED: " .. tostring(err) .. "\n")
end

session:consoleLog("INFO", string.format("=== detect_speech returned, ready=%s ===\n", tostring(session:ready())))

-- Check if session is still alive
if not session:ready() then
    session:consoleLog("ERR", "=== SESSION NOT READY AFTER detect_speech ===\n")
    -- Try to find out why
    local hup_cause = session:getVariable("hangup_cause") or "unknown"
    session:consoleLog("ERR", "=== hangup_cause=" .. hup_cause .. " ===\n")
    return
end

-- Main loop
local loop_count = 0
-- while session:ready() and not done do
while session:ready() do
    loop_count = loop_count + 1

    if loop_count <= 3 then
        session:consoleLog("INFO", string.format("=== loop %d, round=%d, time_diff=%d ===\n",
            loop_count, round, os.time() - last_activity))
    end

    session:streamFile("silence_stream://2000")

    if loop_count <= 3 then
        session:consoleLog("INFO", string.format("=== streamFile returned, loop %d ===\n", loop_count))
    end

    if max_rounds > 0 and round >= max_rounds then
        session:consoleLog("INFO", string.format("ASR: Max rounds reached (%d)\n", max_rounds))
        -- done = true
    elseif os.time() - last_activity >= max_silence then
        session:consoleLog("INFO", string.format("ASR: Silence timeout (%ds)\n", max_silence))
        done = true
    end
end

session:consoleLog("INFO", string.format("=== loop exited: count=%d done=%s ready=%s ===\n",
    loop_count, tostring(done), tostring(session:ready())))

session:execute("detect_speech", "stop")
session:consoleLog("INFO", string.format("ASR complete: %d rounds, results=%s\n",
    round, table.concat(results, " | ")))
session:setVariable("asr_all_results", table.concat(results, " | "))
