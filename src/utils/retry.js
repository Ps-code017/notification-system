import logger from "./logger.js";

const sleep=(ms) => new Promise(resolve => setTimeout(resolve, ms));

const calculateBackoffDelay = (attemptCount, baseDelay = 5000, maxDelay = 30000) => {
  const delay = baseDelay * Math.pow(2, attemptCount);

  return Math.min(delay, maxDelay);
};

const withRetry = async (fn, maxAttempts = 3,context='operation') => {
    let attemptCount = 0;
    while (attemptCount < maxAttempts) {
        try {
            return await fn();
        } catch (error) {
            attemptCount++;
            logger.warn(`${context} failed on attempt ${attemptCount}/${maxAttempts}: ${error.message}`);

            if (attemptCount >= maxAttempts) {
                logger.error(`Failed after ${maxAttempts} attempts in ${context}`);
                throw error;
            }
            const delay = calculateBackoffDelay(attemptCount-1);
            logger.info( `${context} retrying in ${delay / 1000}s ` + `(attempt ${attemptCount + 1}/${maxAttempts})...`);

            await sleep(delay);
        }
    }
};
export default{
    withRetry,
    calculateBackoffDelay,
    sleep
}