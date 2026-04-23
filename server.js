import express from 'express';
import config from './src/config/index.js';
import logger from './src/utils/logger.js';

const app = express();

app.use(express.json());

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.listen(config.port, () => {
  logger.info(`Server running on port ${config.port}`);
});