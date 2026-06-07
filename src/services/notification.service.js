import pool from '../db/postgres.js';
import kafkaService from './kafka.service.js';
import logger from '../utils/logger.js';

const createNotification = async ({ userId, type, title, message }) => {

  const client = await pool.connect();

  try {
    
    const userResult = await client.query(
      `SELECT id, email_enabled, sms_enabled, push_enabled 
       FROM users 
       WHERE id = $1`,
      [userId]
    );

    if (userResult.rows.length === 0) {
      throw new Error(`User ${userId} not found`);
    }

    const user = userResult.rows[0];

    
    const channels = [];
    if (user.email_enabled) channels.push('email');
    if (user.sms_enabled) channels.push('sms');
    if (user.push_enabled) channels.push('push');

    if (channels.length === 0) {
      throw new Error(`User ${userId} has no notification channels enabled`);
    }

    logger.info(`User ${userId} has channels enabled: ${channels.join(', ')}`);


    await client.query('BEGIN');

    
    const notifResult = await client.query(
      `INSERT INTO notifications (user_id, type, title, message, status)
       VALUES ($1, $2, $3, $4, 'pending')
       RETURNING *`,
      [userId, type, title, message]
    );

    const notification = notifResult.rows[0];
    logger.info(`Notification row created: ${notification.id}`);

    
    for (const channel of channels) {
      await client.query(
        `INSERT INTO delivery_attempts (notification_id, channel, status)
         VALUES ($1, $2, 'pending')`,
        [notification.id, channel]
      );
      logger.info(`Delivery attempt created for channel: ${channel}`);
    }

    await client.query('COMMIT');
    
    logger.info(`Transaction committed for notification: ${notification.id}`);

   
    await kafkaService.publishNotification({
      notificationId: notification.id,
      userId,
      type,
      title,
      message,
      channels,
    });

    return notification;

  } catch (err) {

    
    try {
      await client.query('ROLLBACK');
      logger.warn(`Transaction rolled back due to: ${err.message}`);
    } catch (rollbackErr) {
      
      logger.error(`Rollback failed: ${rollbackErr.message}`);
    }

    
    throw err;

  } finally {
    client.release();
    logger.info('DB client released back to pool');
  }
};

const getNotificationById = async (notificationId) => {
  try {
    const notifResult = await pool.query(
      `SELECT * FROM notifications WHERE id = $1`,
      [notificationId]
    );

    if (notifResult.rows.length === 0) {
      return null;
    }

    const attemptsResult = await pool.query(
      `SELECT * FROM delivery_attempts 
       WHERE notification_id = $1
       ORDER BY channel ASC`,
      [notificationId]
    );

    return {
      notification: notifResult.rows[0],
      deliveryAttempts: attemptsResult.rows,
    };

  } catch (err) {
    logger.error(`Failed to fetch notification ${notificationId}: ${err.message}`);
    throw err;
  }
};

export default {
  createNotification,
  getNotificationById,
};