
import Redis from 'ioredis';
import { redisOptions } from '../config/redis.config'


const redis = new Redis(redisOptions);

redis.on('connect', () => {
    console.log('Connected to Redis');
});

redis.on('error', (err) => {
    console.error('Redis connection error:', err);
});


export default redis;
