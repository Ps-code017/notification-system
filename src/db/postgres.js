import pg from 'pg';
import config from '../config/index.js';
import logger from '../utils/logger.js';

const {Pool} = pg;

const pool = new Pool({
    user: config.db.user,
    host: config.db.host,
    database: config.db.database,
    password: config.db.password,
    port: config.db.port,
});

pool.on('connect', () => {
    logger.info('Connected to PostgreSQL database');
});

pool.on('error', (err) => {
    logger.error('PostgreSQL connection error:', err);
    process.exit(1);
});
export default pool;

