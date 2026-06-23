import pool from './postgres.js';
import logger from '../utils/logger.js';

async function migrate() {
  try {
    await pool.query(`
      ALTER TABLE users
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
    `);

    logger.info('Migration v2 successful — updated_at added to users');
  } catch (err) {
    logger.error(`Migration v2 failed: ${err.message}`);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

migrate();