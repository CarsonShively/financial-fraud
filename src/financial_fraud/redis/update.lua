local key = KEYS[1]
local step = tonumber(ARGV[1])
local amount = tonumber(ARGV[2])
local N = tonumber(ARGV[3])

local function ensure_schema()
  local last_seen = redis.call("HGET", key, "dest_last_seen_step")

  local missing_bucket = false
  for i = 1, N do
    if redis.call("HEXISTS", key, "dest_cnt_b"..i) == 0 then
      missing_bucket = true
      break
    end
    if redis.call("HEXISTS", key, "dest_sum_b"..i) == 0 then
      missing_bucket = true
      break
    end
  end

  if (not last_seen) or missing_bucket then
    redis.call("HSET", key, "dest_last_seen_step", step)
    for i = 1, N do
      redis.call("HSET", key, "dest_cnt_b"..i, 0)
      redis.call("HSET", key, "dest_sum_b"..i, 0.0)
    end
    return true
  end

  return false
end

local did_init = ensure_schema()

local last_seen = redis.call("HGET", key, "dest_last_seen_step")
last_seen = tonumber(last_seen) or step

local delta = step - last_seen
if delta < 0 then
  return {0, did_init and 1 or 0}
end

if delta > 0 then
  if delta >= N then
    for i = 1, N do
      redis.call("HSET", key, "dest_cnt_b"..i, 0)
      redis.call("HSET", key, "dest_sum_b"..i, 0.0)
    end
  else
    for i = N, 1, -1 do
      local src = i - delta
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
  end
  redis.call("HSET", key, "dest_last_seen_step", step)
end

redis.call("HINCRBY", key, "dest_cnt_b1", 1)
redis.call("HINCRBYFLOAT", key, "dest_sum_b1", amount)

redis.call("HSET", key, "dest_last_seen_step", step)

return {1, did_init and 1 or 0}
