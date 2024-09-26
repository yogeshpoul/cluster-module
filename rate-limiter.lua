local task_key = KEYS[1]
local queue_key = KEYS[2]
local current_time = tonumber(ARGV[1])
local per_second_limit = tonumber(ARGV[2])
local per_minute_limit = tonumber(ARGV[3])

-- Get valid tasks within the last minute
local tasks = redis.call('lrange', task_key, 0, -1)
local valid_tasks = {}
for i, ts in ipairs(tasks) do
    if (current_time - tonumber(ts)) <= 60000 then
        table.insert(valid_tasks, ts)
    end
end

-- Check per-minute limit
if #valid_tasks >= per_minute_limit then
    redis.call('rpush', queue_key, current_time)
    return "queued"
end

-- Check per-second limit
if #valid_tasks == 0 or (current_time - tonumber(valid_tasks[#valid_tasks])) >= 1000 then
    redis.call('rpush', task_key, current_time)
    return "processed"
else
    redis.call('rpush', queue_key, current_time)
    return "queued"
end
