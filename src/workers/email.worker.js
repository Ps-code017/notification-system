import kafkaService from '../services/kafka.service.js';
import emailService from '../services/email.service.js';
import redisService from '../services/redis.service.js';
import retryUtil from '../utils/retry.js';
import pool from '../db/postgres.js';
import logger from '../utils/logger.js';


const MAX_ATTEMPTS = 3;

const consumer = kafkaService.kafka.consumer({
  groupId: 'email-notification-group',
});


const permanentlyFail = async (notificationId, errorMessage) => {
  try {
    await pool.query(
      `UPDATE delivery_attempts
       SET status = 'failed',
           error_message = $1,
           last_attempted_at = NOW()
       WHERE notification_id = $2
       AND channel = 'email'`,
      [errorMessage, notificationId]
    );

    await pool.query(
      `UPDATE notifications
       SET status = 'failed',
           updated_at = NOW()
       WHERE id = $1`,
      [notificationId]
    );

    logger.warn(`Notification ${notificationId} permanently failed: ${errorMessage}`);
  } catch (err) {
    logger.error(`Failed to record permanent failure for ${notificationId}: ${err.message}`);
  }
};


const attemptEmailDelivery = async (notificationId, userEmail, title, message) => {
  
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    
    await client.query(
      `UPDATE delivery_attempts
       SET status = 'processing',
           attempt_count = attempt_count + 1,
           last_attempted_at = NOW()
       WHERE notification_id = $1
       AND channel = 'email'`,
      [notificationId]
    );

    logger.info(`Attempting email delivery for notification: ${notificationId}`);

    
    await emailService.sendEmail({
      to: userEmail,
      title,
      message,
    });

    
    await client.query(
      `UPDATE delivery_attempts
       SET status = 'delivered',
           delivered_at = NOW(),
           error_message = NULL
       WHERE notification_id = $1
       AND channel = 'email'`,
      [notificationId]
    );

    
    const pendingResult = await client.query(
      `SELECT COUNT(*) as pending_count
       FROM delivery_attempts
       WHERE notification_id = $1
       AND status NOT IN ('delivered', 'failed')`,
      [notificationId]
    );

    const pendingCount = parseInt(pendingResult.rows[0].pending_count);

    if (pendingCount === 0) {
      await client.query(
        `UPDATE notifications
         SET status = 'delivered',
             updated_at = NOW()
         WHERE id = $1`,
        [notificationId]
      );
      logger.info(`All channels delivered — notification ${notificationId} marked delivered`);
    }

    
    await client.query('COMMIT');
    logger.info(`Attempt successful for notification: ${notificationId}`);

  } catch (err) {
    
    try {
      await client.query('ROLLBACK');
    } catch (rollbackErr) {
      logger.error(`Rollback failed for ${notificationId}: ${rollbackErr.message}`);
    }

    await pool.query(
      `UPDATE delivery_attempts
       SET error_message = $1,
           last_attempted_at = NOW()
       WHERE notification_id = $2
       AND channel = 'email'`,
      [err.message, notificationId]
    );

    throw err;

  } finally {
    client.release();
  }
};


const processNotification = async (notificationData) => {
  const { notificationId, userId, channels, title, message } = notificationData;

  if (!channels.includes('email')) {
    logger.info(`Notification ${notificationId} has no email channel — skipping`);
    return;
  }

  const duplicate = await redisService.isDuplicate(notificationId, 'email');
  if (duplicate) {
    logger.warn(`Duplicate detected — skipping: ${notificationId}:email`);
    return;
  }


  const attemptResult = await pool.query(
    `SELECT attempt_count FROM delivery_attempts
     WHERE notification_id = $1 AND channel = 'email'`,
    [notificationId]
  );

  if (attemptResult.rows.length === 0) {
    logger.error(`No delivery attempt row found for notification: ${notificationId}`);
    return;
  }

  const currentAttemptCount = parseInt(attemptResult.rows[0].attempt_count);

  if (currentAttemptCount >= MAX_ATTEMPTS) {
    logger.warn(
      `Notification ${notificationId} already at max attempts ` +
      `(${currentAttemptCount}/${MAX_ATTEMPTS}) — skipping`
    );
    return;
  }

  
  const userResult = await pool.query(
    `SELECT email FROM users WHERE id = $1`,
    [userId]
  );

  if (userResult.rows.length === 0) {
    
    logger.error(`User ${userId} not found — permanently failing ${notificationId}`);
    await permanentlyFail(notificationId, `User ${userId} not found`);
    return;
  }

  const userEmail = userResult.rows[0].email;

  try {
    await retryUtil.withRetry(
      async () => {
        await attemptEmailDelivery(notificationId, userEmail, title, message);
      },
      MAX_ATTEMPTS - currentAttemptCount,
      `email:${notificationId}`
    );

    await redisService.markAsProcessed(notificationId, 'email');
    logger.info(`Email delivery successful for notification: ${notificationId}`);

  } catch (err) {
    logger.error(
      `Email permanently failed for notification ${notificationId} ` +
      `after ${MAX_ATTEMPTS} total attempts: ${err.message}`
    );
    await permanentlyFail(notificationId, err.message);
  }
};

const startEmailWorker = async () => {
  
  await emailService.verifyConnection();

  await consumer.connect();
  logger.info('Email worker connected to Kafka');

  await consumer.subscribe({ topic: 'notifications', fromBeginning: false });
  logger.info('Email worker subscribed to notifications topic');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const notificationData = JSON.parse(message.value.toString());

      logger.info(
        `Email worker received message — ` +
        `topic: ${topic}, partition: ${partition}, ` +
        `notificationId: ${notificationData.notificationId}`
      );

      await processNotification(notificationData);
    },
  });

  logger.info('Email worker is running and processing messages from Kafka');
};

const shutdown = async (signal) => {
  logger.info(`Email worker received ${signal} — shutting down gracefully`);
  try {
    await consumer.disconnect();
    await pool.end();
    await redisService.redis.quit();
    logger.info('Email worker shut down cleanly');
    process.exit(0);
  } catch (err) {
    logger.error(`Error during shutdown: ${err.message}`);
    process.exit(1);
  }
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));


startEmailWorker().catch((err) => {
  logger.error(`Email worker failed to start: ${err.message}`);
  process.exit(1);
});