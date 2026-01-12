-- Align ring-buffer to current step.
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

if gap <= 0 then
  return 0
end

local cur_cnt = redis.call("HGET", key, "dest_cnt_cur") or "0"
local cur_sum = redis.call("HGET", key, "dest_sum_cur") or "0"

if gap >= (N + 1) then
  for i = 1, N do
    redis.call("HSET", key, "dest_cnt_b"..i, 0)
    redis.call("HSET", key, "dest_sum_b"..i, 0.0)
  end
else
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

  redis.call("HSET", key, "dest_cnt_b"..gap, cur_cnt)
  redis.call("HSET", key, "dest_sum_b"..gap, cur_sum)
end

redis.call("HSET", key, "dest_cnt_cur", 0)
redis.call("HSET", key, "dest_sum_cur", 0.0)

return 1