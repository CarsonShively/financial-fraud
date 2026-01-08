-- advance.lua
-- Advance sliding-window per-dest buckets WITHOUT adding the current txn.
-- Contract:
--   - NO-OP if the entity doesn't exist yet (don't create it here)
--   - Shift buckets based on (step - dest_last_seen_step)
--   - Reset cur for the new step
--   - DO NOT set dest_last_seen_step here (set it in add_txn.lua)

-- KEYS[1] = entity hash key
-- ARGV[1] = step (int)
-- ARGV[2] = N (int)

local key  = KEYS[1]
local step = tonumber(ARGV[1])
local N    = tonumber(ARGV[2])

-- If entity doesn't exist, don't create it during ADVANCE.
if redis.call("EXISTS", key) == 0 then
  return 0
end

local last_seen_raw = redis.call("HGET", key, "dest_last_seen_step")
if not last_seen_raw then
  -- Entity exists but not initialized properly; don't "fix" here.
  return 0
end

local last_seen = tonumber(last_seen_raw)
local delta = step - last_seen

-- out-of-order or same-step: nothing to do
if delta <= 0 then
  return 0
end

-- Read current-step accumulators (the "cur" bucket)
local cur_cnt = redis.call("HGET", key, "dest_cnt_cur") or "0"
local cur_sum = redis.call("HGET", key, "dest_sum_cur") or "0"

-- Shift history buckets by delta
if delta >= (N + 1) then
  -- Too large a gap: everything falls out of window
  for i = 1, N do
    redis.call("HSET", key, "dest_cnt_b"..i, 0)
    redis.call("HSET", key, "dest_sum_b"..i, 0.0)
  end
else
  -- Shift from high to low to avoid overwriting
  for i = N, 1, -1 do
    local src = i - delta

    if src == 0 then
      -- old "cur" becomes the new b(delta) destination position
      redis.call("HSET", key, "dest_cnt_b"..i, cur_cnt)
      redis.call("HSET", key, "dest_sum_b"..i, cur_sum)
    elseif src >= 1 then
      local c = redis.call("HGET", key, "dest_cnt_b"..src) or "0"
      local s = redis.call("HGET", key, "dest_sum_b"..src) or "0"
      redis.call("HSET", key, "dest_cnt_b"..i, c)
      redis.call("HSET", key, "dest_sum_b"..i, s)
    else
      -- src < 0 means it shifted in from beyond window => zero fill
      redis.call("HSET", key, "dest_cnt_b"..i, 0)
      redis.call("HSET", key, "dest_sum_b"..i, 0.0)
    end
  end
end

-- Reset cur for this new step (ready for ADD to accumulate)
redis.call("HSET", key, "dest_cnt_cur", 0)
redis.call("HSET", key, "dest_sum_cur", 0.0)

-- IMPORTANT: do NOT update dest_last_seen_step here
return 1
