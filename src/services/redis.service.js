import Redis from "ioredis";
import config from "../config/index.js";
import logger from "../utils/logger.js";

const redis=new Redis({
    host:config.redis.host,
    port:config.redis.port,
    maxRetriesPerRequest:3,
    retryStrategy:(times)=>{
        if(times>3){
            logger.error('Max Redis connection attempts reached. Giving up.');
            return null;
        }
        return times*200;
    }
});

redis.on('connect',()=>{
    logger.info('Connected to Redis successfully');
});

redis.on('error',(err)=>{
    logger.error('Redis connection error:', err);
});

const buildDedupKey=(notificationId,channel)=>{
    return `dedup:${notificationId}:${channel}`;
};

const isDuplicate=async(notificationId,channel)=>{
    const dedupKey=buildDedupKey(notificationId,channel);
    const result=await redis.get(dedupKey);
    return result!==null;
};

const markAsProcessed=async(notificationId,channel)=>{
    const dedupKey=buildDedupKey(notificationId,channel);
    await redis.set(dedupKey,'1','EX',86400);
    logger.info(`Marked notification ${notificationId} for channel ${channel} as processed in Redis : ${dedupKey}`);
}

export default {
    redis,
    isDuplicate,
    markAsProcessed
}