import { Router } from 'express';
import pool from '../../db/postgres.js';
import redisService from '../../services/redis.service.js';
import logger from '../../utils/logger.js';

const router = Router();

// POST /api/users
// Create a new user
router.post('/', async (req, res) => {
  const { email, phone, fcmToken } = req.body;

  if (!email) {
    return res.status(400).json({ error: 'email is required' });
  }

  try {
    const result = await pool.query(
      `INSERT INTO users (email, phone, fcm_token)
       VALUES ($1, $2, $3)
       RETURNING *`,
      [email, phone || null, fcmToken || null]
    );

    return res.status(201).json({
      message: 'User created successfully',
      user: result.rows[0],
    });

  } catch (err) {
    if (err.code === '23505') {
      return res.status(409).json({ error: 'Email already exists' });
    }
    logger.error(`Failed to create user: ${err.message}`);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/users/:id
// Fetch user profile and current preferences
router.get('/:id', async (req, res) => {
  const { id } = req.params;

  try {
    const result = await pool.query(
      `SELECT id, email, phone, email_enabled, sms_enabled, push_enabled, created_at
       FROM users WHERE id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    return res.json({ user: result.rows[0] });

  } catch (err) {
    logger.error(`Failed to fetch user ${id}: ${err.message}`);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// PATCH /api/users/:id/preferences
// Update which notification channels a user wants
router.patch('/:id/preferences', async (req, res) => {
  const { id } = req.params;
  const { emailEnabled, smsEnabled, pushEnabled } = req.body;

  // Build dynamic update query
  // Only update fields that were actually sent in the request
  // If someone sends only emailEnabled, don't touch sms or push
  const updates = [];
  const values = [];
  let paramCount = 1;

  if (typeof emailEnabled !== 'undefined') {
    updates.push(`email_enabled = $${paramCount}`);
    values.push(emailEnabled);
    paramCount++;
  }

  if (typeof smsEnabled !== 'undefined') {
    updates.push(`sms_enabled = $${paramCount}`);
    values.push(smsEnabled);
    paramCount++;
  }

  if (typeof pushEnabled !== 'undefined') {
    updates.push(`push_enabled = $${paramCount}`);
    values.push(pushEnabled);
    paramCount++;
  }

  // If no valid fields were sent, return early
  if (updates.length === 0) {
    return res.status(400).json({
      error: 'At least one preference field required: emailEnabled, smsEnabled, pushEnabled'
    });
  }

  values.push(new Date());
  values.push(id);

  try {
    const result = await pool.query(
      `UPDATE users
       SET ${updates.join(', ')}, updated_at = $${paramCount}
       WHERE id = $${paramCount + 1}
       RETURNING id, email, email_enabled, sms_enabled, push_enabled`,
      values
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    logger.info(`Preferences updated for user ${id}`);

    return res.json({
      message: 'Preferences updated successfully',
      user: result.rows[0],
    });

  } catch (err) {
    logger.error(`Failed to update preferences for user ${id}: ${err.message}`);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/users/:id/rate-limit-status
// Shows current rate limit usage across all channels
router.get('/:id/rate-limit-status', async (req, res) => {
  const { id } = req.params;

  try {
    const userResult = await pool.query(
      'SELECT id FROM users WHERE id = $1',
      [id]
    );

    if (userResult.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    const [emailStatus, smsStatus, pushStatus] = await Promise.all([
      redisService.getRateLimitStatus(id, 'email'),
      redisService.getRateLimitStatus(id, 'sms'),
      redisService.getRateLimitStatus(id, 'push'),
    ]);

    return res.json({
      userId: id,
      rateLimits: {
        email: emailStatus,
        sms: smsStatus,
        push: pushStatus,
      },
    });

  } catch (err) {
    logger.error(`Failed to fetch rate limit status for user ${id}: ${err.message}`);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;