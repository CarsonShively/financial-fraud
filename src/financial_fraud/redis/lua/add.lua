-- add_txn.lua (step-based, parity-compatible)
-- Returns {1, prev_distinct_step} where prev_distinct_step is "" if none.
--
-- Fields:
--   dest_last_seen_step  = current step we've seen (for ADVANCE delta calc)
--   dest_prev_seen_step  = previous DISTINCT step (< current step), stable within same step

local key    = KEYS[1]
local step   = tonumber(ARGV[1])
local amount = tonumber(ARGV[2])
local N      = tonumber(ARGV[3])

local SCHEMA_FIELD     = "dest_schema_N"
local FIRST_SEEN_FIELD = "dest_first_seen_step"
local LAST_SEEN_FIELD  = "dest_last_seen_step"
local PREV_SEEN_FIELD  = "dest_prev_seen_step"

local function init_schema(n)
  redis.call("HSET", key, SCHEMA_FIELD, n)
  redis.call("HSET", key, FIRST_SEEN_FIELD, step)

  redis.call("HSET", key, "dest_cnt_cur", 0)
  redis.call("HSET", key, "dest_sum_cur", 0.0)

  for i = 1, n do
    redis.call("HSET", key, "dest_cnt_b"..i, 0)
    redis.call("HSET", key, "dest_sum_b"..i, 0.0)
  end
end

-- Create if missing
if redis.call("EXISTS", key) == 0 then
  if not N then return {0, "missing_N"} end
  init_schema(N)
end

-- Enforce schema if N provided
if N then
  local schemaN = tonumber(redis.call("HGET", key, SCHEMA_FIELD))
  if (not schemaN) or (schemaN ~= N) then
    return {0, "schema_mismatch"}
  end
end

local last_seen_raw = redis.call("HGET", key, LAST_SEEN_FIELD)
local last_seen = last_seen_raw and tonumber(last_seen_raw) or nil

local prev_seen_raw = redis.call("HGET", key, PREV_SEEN_FIELD)
-- prev_seen_raw may be nil or a string step

-- Determine the "previous DISTINCT step" to return (stable within same step)
local prev_for_gap = ""

if not last_seen then
  -- first ever txn: no previous step
  prev_for_gap = ""
  -- set current step as last_seen
  redis.call("HSET", key, LAST_SEEN_FIELD, step)
elseif step > last_seen then
  -- step advanced: previous distinct step becomes old last_seen
  prev_for_gap = last_seen_raw
  redis.call("HSET", key, PREV_SEEN_FIELD, last_seen_raw)
  redis.call("HSET", key, LAST_SEEN_FIELD, step)
else
  -- same step (or out-of-order): return stored prev distinct step
  if prev_seen_raw then
    prev_for_gap = prev_seen_raw
  else
    prev_for_gap = ""
  end
end

-- Add txn to current step totals
redis.call("HINCRBY", key, "dest_cnt_cur", 1)
redis.call("HINCRBYFLOAT", key, "dest_sum_cur", amount)

return {1, prev_for_gap}
