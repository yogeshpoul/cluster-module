const express = require('express');
const Redis = require('ioredis');
const cluster = require('cluster');
const os = require('os');
const bodyParser = require('body-parser');
const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');
require('dotenv').config();

const app = express();
app.use(bodyParser.json());

const PORT = process.env.PORT || 3000;
const numCPUs = 2; // Two worker processes

// Initialize Redis client using the URL from the environment variable
const redis = new Redis(process.env.REDIS_URL);

// Load rate limits from config.yaml
const config = yaml.load(fs.readFileSync('./config.yaml', 'utf8'));
const { per_second, per_minute } = config.rate_limit;

// Load the Lua script into Redis
const rateLimiterScript = fs.readFileSync(path.join(__dirname, 'rate-limiter.lua'), 'utf8');
let rateLimiterSha;

redis.script('load', rateLimiterScript).then(sha => {
    rateLimiterSha = sha;
    console.log('Rate limiter script loaded with SHA:', sha);
});

// Task function
async function task(user_id) {
    const logEntry = `${user_id} - task completed at ${Date.now()}\n`;
    try {
        fs.appendFileSync(path.join(__dirname, 'task-log.txt'), logEntry);
    } catch (err) {
        console.error(`Error writing to log file: ${err}`);
    }
}

// Rate limiting and queuing logic using Redis Lua script
async function rateLimit(user_id) {
    const currentTime = Date.now();
    const taskKey = `tasks:${user_id}`;
    const queueKey = `${taskKey}:queue`;

    // Execute the Lua script atomically
    const result = await redis.evalsha(rateLimiterSha, 2, taskKey, queueKey, currentTime, per_second, per_minute);
    console.log(`Rate limiting result for user ${user_id}: ${result}`);

    if (result === 'processed') {
        await task(user_id);
        return { status: 'success', message: 'Task processed' };
    } else {
        return { status: 'queued', message: 'Task queued for later processing' };
    }
}

// Processing the queue
async function processQueue() {
    const userKeys = await redis.keys('tasks:*:queue');
    for (const key of userKeys) {
        const user_id = key.split(':')[1];
        const queue = await redis.lrange(key, 0, -1);

        if (queue.length > 0) {
            const currentTime = Date.now();
            const taskTime = parseInt(queue[0]);

            const lastTaskTime = (await redis.lrange(`tasks:${user_id}`, -1, -1))[0];

            if (!lastTaskTime || (currentTime - lastTaskTime) >= 1000) {
                await task(user_id);
                await redis.lrem(key, 1, taskTime);
                await redis.rpush(`tasks:${user_id}`, currentTime);
                console.log(`Processed queued task for user ${user_id}`);
            }
        }
    }
}

// API route
app.post('/api/v1/task', async (req, res) => {
    const { user_id } = req.body;

    if (!user_id) {
        return res.status(400).json({ error: 'user_id is required' });
    }

    const result = await rateLimit(user_id);
    res.status(result.status === 'success' ? 200 : 202).json({ message: result.message, user_id });
});

// Process the queue more frequently
setInterval(processQueue, 500);

// Start cluster
if (cluster.isMaster) {
    for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
    }

    cluster.on('exit', (worker) => {
        console.log(`Worker ${worker.process.pid} died`);
    });
} else {
    app.listen(PORT, () => {
        console.log(`Worker ${process.pid} started on port ${PORT}`);
    });
}
