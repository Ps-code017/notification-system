import express from 'express';
import config from './src/config/index.js';
import logger from './src/utils/logger.js';
import pool from './src/db/postgres.js';

const app = express();

app.use(express.json());

app.get('/health', async(req, res) => {
  // res.json({ status: 'ok' });
  try{
    await pool.query('SELECT 1');
    res.json({ status: 'ok',db: 'connected' });
  } catch (error) {
    logger.error('Error occurred while checking health:', error);
    res.status(500).json({ status: 'error', message: 'Database connection failed' });
  }
});

app.listen(config.port, () => {
  logger.info(`Server running on port ${config.port}`);
});