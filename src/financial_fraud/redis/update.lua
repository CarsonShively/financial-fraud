-- Sliding-window per-dest buckets (count + sum), matching SQL schema:
-- dest_last_seen_step = cur_step
-- dest_cnt_b1..bN / dest_sum_b1..bN represent (cur_step-1 .. cur_step-N)
-- dest_cnt_cur / dest_sum_cur represent current step totals (cur_step)

-- KEYS[1] = entity hash key (dest)
-- ARGV[1] = step (int)
-- ARGV[2] = amount (float as string)
-- ARGV[3] = N (int, e.g. 24)

local key = KEYS[1]
local step = tonumber(ARGV[1])
local amount = tonumber(ARGV[2])
local N = tonumber(ARGV[3])

-- Use a schema marker to avoid per-bucket HEXISTS loops
local SCHEMA_FIELD = "dest_schema_N"

local function init_schema()
  redis.call("HSET", key, SCHEMA_FIELD, N)
  redis.call("HSET", key, "dest_last_seen_step", step)

  -- current-step accumulators (cur_step)
  redis.call("HSET", key, "dest_cnt_cur", 0)
  redis.call("HSET", key, "dest_sum_cur", 0.0)

  -- previous-step buckets (cur_step-1 .. cur_step-N)
  for i = 1, N do
    redis.call("HSET", key, "dest_cnt_b"..i, 0)
    redis.call("HSET", key, "dest_sum_b"..i, 0.0)
  end
end

-- Ensure schema exists and matches N
local schemaN = tonumber(redis.call("HGET", key, SCHEMA_FIELD))
local last_seen_raw = redis.call("HGET", key, "dest_last_seen_step")

local did_init = 0
if (not last_seen_raw) or (schemaN ~= N) then
  init_schema()
  did_init = 1
end

local last_seen = tonumber(redis.call("HGET", key, "dest_last_seen_step")) or step
local delta = step - last_seen

-- Out-of-order event (older step than what we’ve already seen)
if delta < 0 then
  return {0, did_init}
end

-- If step advanced, shift buckets and roll current accumulators into b1.
-- b1 should become totals from the previous step, matching SQL's (cur_step - 1).
if delta > 0 then
  local cur_cnt = redis.call("HGET", key, "dest_cnt_cur") or "0"
  local cur_sum = redis.call("HGET", key, "dest_sum_cur") or "0"

  if delta >= (N + 1) then
    -- Gap too large: everything falls out of window
    for i = 1, N do
      redis.call("HSET", key, "dest_cnt_b"..i, 0)
      redis.call("HSET", key, "dest_sum_b"..i, 0.0)
    end
  else
    -- Shift from high -> low to avoid overwriting
    for i = N, 1, -1 do
      local src = i - delta
      if src == 0 then
        -- previous current-step totals become b1 after we advance
        redis.call("HSET", key, "dest_cnt_b"..i, cur_cnt)
        redis.call("HSET", key, "dest_sum_b"..i, cur_sum)
      elseif src >= 1 then
        local c = redis.call("HGET", key, "dest_cnt_b"..src) or "0"
        local s = redis.call("HGET", key, "dest_sum_b"..src) or "0"
        redis.call("HSET", key, "dest_cnt_b"..i, c)
        redis.call("HSET", key, "dest_sum_b"..i, s)
      else
        redis.call("HSET", key, "dest_cnt_b"..i, 0)
        redis.call("HSET", key, "dest_sum_b"..i, 0.0)
      end
    end
  end

  -- Reset current-step accumulators for the new step
  redis.call("HSET", key, "dest_cnt_cur", 0)
  redis.call("HSET", key, "dest_sum_cur", 0.0)

  -- Update last seen to the new current step
  redis.call("HSET", key, "dest_last_seen_step", step)
end

-- Always add this txn to the current step totals
redis.call("HINCRBY", key, "dest_cnt_cur", 1)
redis.call("HINCRBYFLOAT", key, "dest_sum_cur", amount)

return {1, did_init}
