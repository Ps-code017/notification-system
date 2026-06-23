import { Router } from 'express';
import notificationService from '../../services/notification.service.js';
import logger from '../../utils/logger.js';

const router = Router();

router.post('/', async (req, res) => {
  const { userId, type, title, message } = req.body;

  if (!userId || !type || !title || !message) {
    return res.status(400).json({
      error: 'userId, type, title, and message are all required'
    });
  }

  try {
    const notification = await notificationService.createNotification({
      userId,
      type,
      title,
      message,
    });

    return res.status(202).json({
      message: 'Notification accepted for delivery',
      notificationId: notification.id,
    });

  } catch (err) {
    logger.error(`POST /api/notifications failed: ${err.message}`);

    if (err.isRateLimit) {
      const maxResetsIn = Math.max(
        ...err.blockedChannels.map(c => c.resetsIn || 0)
      );
      res.set('Retry-After', maxResetsIn);
      return res.status(429).json({
        error: err.message,
        retryAfter: maxResetsIn,
      });
    }

    if (err.message.includes('not found')) {
      return res.status(404).json({ error: err.message });
    }

    if (err.message.includes('no notification channels enabled')) {
      return res.status(400).json({ error: err.message });
    }

    return res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/:id', async (req, res) => {
  const { id } = req.params;

  try {
    const result = await notificationService.getNotificationById(id);

    if (!result) {
      return res.status(404).json({ error: 'Notification not found' });
    }

    return res.json(result);

  } catch (err) {
    logger.error(`GET /api/notifications/${id} failed: ${err.message}`);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;