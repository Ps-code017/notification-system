import pool from './postgres.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import logger from '../utils/logger.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function migrate() {
    try{
        const schema=fs.readFileSync(path.join(__dirname,'schema.sql'),'utf-8');
        await pool.query(schema);
        logger.info('Database migration completed successfully.');
    }catch (error) {
        logger.error('Error occurred while migrating database:', error);
    }finally{
        await pool.end();
    }
}
migrate();