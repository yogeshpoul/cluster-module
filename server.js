// server.js
const express = require('express');
const Redis = require('ioredis');
const cluster = require('cluster');
const os = require('os');
const bodyParser = require('body-parser');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const app = express();
app.use(bodyParser.json());

const PORT = process.env.PORT || 3000;
const numCPUs = os.cpus().length;

// Initialize Redis client using the URL from the environment variable
const redis = new Redis(process.env.REDIS_URL);

// Task function
async function task(user_id) {
    const logEntry = `${user_id} - task completed at ${Date.now()}\n`;
    fs.appendFileSync(path.join(__dirname, 'task-log.txt'), logEntry);
}

// Rate limiting and queuing logic
const TASK_RATE_LIMIT = { perSecond: 1, perMinute: 20 };

async function rateLimit(user_id) {
    const currentTime = Date.now();
    const taskKey = `tasks:${user_id}`;
    const queueKey = `${taskKey}:queue`;

    // Get timestamps of the last tasks
    const lastTasks = await redis.lrange(taskKey, 0, -1);
    
    // Filter valid tasks within the last minute
    const validTasks = lastTasks.filter(ts => (currentTime - ts) <= 60000);

    console.log(`Current valid tasks for ${user_id}: ${validTasks.length}`);

    // Check if the user has exceeded the per-minute limit
    if (validTasks.length >= TASK_RATE_LIMIT.perMinute) {
        await redis.rpush(queueKey, currentTime);
        console.log(`Queued request for user ${user_id} at ${currentTime}`);
        return { status: 'queued', message: 'Task queued for later processing' };
    }

    // Check if the user can execute a task now (per-second limit)
    if (validTasks.length < TASK_RATE_LIMIT.perSecond) {
        await task(user_id);
        await redis.rpush(taskKey, currentTime); // Log the successful task
        console.log(`Processed task for user ${user_id} at ${currentTime}`);
        return { status: 'success', message: 'Task processed' };
    } else {
        // If here, user is over the per-second limit, queue the request
        await redis.rpush(queueKey, currentTime);
        console.log(`Queued request for user ${user_id} at ${currentTime}`);
        return { status: 'queued', message: 'Task queued for later processing' };
    }
}

// Processing the queue
async function processQueue() {
    const userKeys = await redis.keys('tasks:*:queue'); // Get all users with queued tasks
    for (const key of userKeys) {
        const user_id = key.split(':')[1];
        const queue = await redis.lrange(key, 0, -1); // Get the queue for the user
        
        if (queue.length > 0) {
            const currentTime = Date.now();
            const taskTime = parseInt(queue[0]); // Get the timestamp of the first queued task

            // Check if a second has passed since the last task execution
            if ((currentTime - taskTime) >= 1000) {
                await task(user_id);
                await redis.lrem(key, 1, taskTime); // Remove the processed task
                await redis.rpush(`tasks:${user_id}`, currentTime); // Log successful task
                console.log(`Processed queued task for user ${user_id} at ${currentTime}`);
            } else {
                console.log(`Cannot process queued task for user ${user_id}, waiting for interval.`);
            }
        }
    }
}

// API Route
app.post('/api/v1/task', async (req, res) => {
    const { user_id } = req.body;

    if (!user_id) {
        return res.status(400).json({ error: 'user_id is required' });
    }

    const result = await rateLimit(user_id);
    res.status(result.status === 'success' ? 200 : 202).json({ message: result.message, user_id });
});

// Schedule a job to process the queues every second
setInterval(processQueue, 1000);

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
        console.log(`Worker ${process.pid} started and listening on port ${PORT}`);
    });
}
