-- advance.lua
-- Advance per-dest buckets when we encounter a new step.
-- Goal (matches SQL):
--   b1..bN represent totals for steps [step-1 .. step-N]
--   cur holds totals for the *current* step (excluded from features)

-- KEYS[1] = entity hash key
-- ARGV[1] = step (int)  -- current step for incoming txn
-- ARGV[2] = N (int)     -- history length (e.g., 24)

local key  = KEYS[1]
local step = tonumber(ARGV[1])
local N    = tonumber(ARGV[2])

if redis.call("EXISTS", key) == 0 then
  return 0
end

local last_seen_raw = redis.call("HGET", key, "dest_last_seen_step")
if not last_seen_raw then
  return 0
end

local last_seen = tonumber(last_seen_raw)
local gap = step - last_seen

-- same-step or out-of-order: nothing to do
if gap <= 0 then
  return 0
end

-- Read totals accumulated for last_seen step (the "cur" bucket)
local cur_cnt = redis.call("HGET", key, "dest_cnt_cur") or "0"
local cur_sum = redis.call("HGET", key, "dest_sum_cur") or "0"

-- If the gap is too large, everything falls out of the window
if gap >= (N + 1) then
  for i = 1, N do
    redis.call("HSET", key, "dest_cnt_b"..i, 0)
    redis.call("HSET", key, "dest_sum_b"..i, 0.0)
  end
else
  -- Shift existing buckets "older" by gap steps:
  -- after shifting, b_gap is free for inserting cur,
  -- and b1..b(gap-1) should be zeros (no activity in those missing steps).
  for i = N, 1, -1 do
    local src = i - gap
    if src >= 1 then
      local c = redis.call("HGET", key, "dest_cnt_b"..src) or "0"
      local s = redis.call("HGET", key, "dest_sum_b"..src) or "0"
      redis.call("HSET", key, "dest_cnt_b"..i, c)
      redis.call("HSET", key, "dest_sum_b"..i, s)
    else
      redis.call("HSET", key, "dest_cnt_b"..i, 0)
      redis.call("HSET", key, "dest_sum_b"..i, 0.0)
    end
  end

  -- Place the just-finished step totals (for last_seen) into b_gap
  redis.call("HSET", key, "dest_cnt_b"..gap, cur_cnt)
  redis.call("HSET", key, "dest_sum_b"..gap, cur_sum)
end

-- Reset cur for accumulating the new current step
redis.call("HSET", key, "dest_cnt_cur", 0)
redis.call("HSET", key, "dest_sum_cur", 0.0)

-- IMPORTANT: do NOT update dest_last_seen_step here
return 1
