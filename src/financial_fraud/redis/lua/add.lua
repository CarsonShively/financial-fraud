-- Add current transaction hash to redis.
local key    = KEYS[1]
local step   = tonumber(ARGV[1])
local amount = tonumber(ARGV[2])
local N      = tonumber(ARGV[3])

local SCHEMA_FIELD = "dest_schema_N"
local LAST_SEEN_FIELD = "dest_last_seen_step"

local function init_schema(n)
  redis.call("HSET", key, SCHEMA_FIELD, n)
  redis.call("HSET", key, LAST_SEEN_FIELD, step)

  redis.call("HSET", key, "dest_cnt_cur", 0)
  redis.call("HSET", key, "dest_sum_cur", 0.0)

  for i = 1, n do
    redis.call("HSET", key, "dest_cnt_b"..i, 0)
    redis.call("HSET", key, "dest_sum_b"..i, 0.0)
  end
end

if redis.call("EXISTS", key) == 0 then
  if not N then return 0 end
  init_schema(N)
else
  if N then
    local schemaN = tonumber(redis.call("HGET", key, SCHEMA_FIELD))
    if (not schemaN) or (schemaN ~= N) then
      return 0
    end
  end

  local last_seen_raw = redis.call("HGET", key, LAST_SEEN_FIELD)
  if (not last_seen_raw) or (step > tonumber(last_seen_raw)) then
    redis.call("HSET", key, LAST_SEEN_FIELD, step)
  end
end

redis.call("HINCRBY", key, "dest_cnt_cur", 1)
redis.call("HINCRBYFLOAT", key, "dest_sum_cur", amount)

return 1