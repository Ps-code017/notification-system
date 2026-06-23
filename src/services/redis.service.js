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

const RATE_LIMIT_MAX=5;
const RATE_LIMIT_WINDOW=3600; 

const buildRateLimitKey=(userId,channel)=>{
    return `rate_limit:${userId}:${channel}`;
}

const checkRateLimit=async(userId,channel)=>{
    const rateLimitKey=buildRateLimitKey(userId,channel);
   const current=await redis.get(rateLimitKey);
   const currentCount=current?parseInt(current):0;

    if(currentCount>=RATE_LIMIT_MAX){
        const ttl=await redis.ttl(rateLimitKey);
        logger.warn(`Rate limit exceeded for user ${userId} on channel ${channel}. Current count: ${currentCount}, TTL: ${ttl} seconds`);
        
        return {
        allowed: false,
        count: currentCount,
        remaining: 0,
        resetsIn: ttl,
        };
        return {
            allowed: true,
            count: currentCount,
            remaining: RATE_LIMIT_MAX - currentCount,
            resetsIn: null,
        };
    }  
};

const incrementRateLimit = async (userId, channel) => {
  const key = buildRateLimitKey(userId, channel);

  const newCount = await redis.incr(key);

  if (newCount === 1) {
    await redis.expire(key, RATE_LIMIT_WINDOW);
    logger.info(
      `Rate limit window started for user ${userId} on channel ${channel} — ` +
      `window: ${RATE_LIMIT_WINDOW}s`
    );
  }

  logger.info(
    `Rate limit incremented for user ${userId} on channel ${channel} — ` +
    `count: ${newCount}/${RATE_LIMIT_MAX}`
  );

  return newCount;
};

const getRateLimitStatus = async (userId, channel) => {
  const key = buildRateLimitKey(userId, channel);
  const current = await redis.get(key);
  const ttl = await redis.ttl(key);
  const currentCount = current ? parseInt(current) : 0;

  return {
    count: currentCount,
    max: RATE_LIMIT_MAX,
    remaining: Math.max(0, RATE_LIMIT_MAX - currentCount),
    resetsIn: ttl > 0 ? ttl : null,
    windowSeconds: RATE_LIMIT_WINDOW,
  };
};

export default {
    redis,
    isDuplicate,
    markAsProcessed,
    checkRateLimit,
    incrementRateLimit,
    getRateLimitStatus,
}   
