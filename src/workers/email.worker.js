import kafkaService from "../services/kafka.service.js";
import emailService from "../services/email.service.js";
import pool from "../db/postgres.js";
import logger from "../utils/logger.js";

const consumer=kafkaService.kafka.consumer({
    groupId: 'email-notification-group'
})

const processNotification=async(notificationData)=>{
    const {notificationId,userId,channels,title,message}=notificationData;

    if(!channels.includes('email')){
        logger.info(`Notification ${notificationId} does not require email delivery. Skipping.`);
        return;
    }

    const client=await pool.connect();

    try{
        const userResult=await client.query(
            `SELECT email FROM users WHERE id=$1`,
            [userId]
        );
        if(userResult.rows.length===0){
            throw new Error(`User ${userId} not found`);
        }
        const userEmail=userResult.rows[0].email;

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
        logger.info(`Delivery attempt for notification ${notificationId} marked as processing.`);

        await emailService.sendEMail({
            to: userEmail,
            title,
            message
        });

         await client.query(
            `UPDATE delivery_attempts
            SET status = 'delivered',
                delivered_at = NOW()
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
            // All channels done — update parent notification status
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
        logger.info(`Notification ${notificationId} processed successfully for email delivery.`);

    }catch(err){
        try {
            await client.query('ROLLBACK');
        } catch (rollbackErr) {
            logger.error(`Error occurred while rolling back transaction for notification ${notificationId}: ${rollbackErr.message}`);
        }

        try{
            await pool.query(
                `UPDATE delivery_attempts
                SET status = 'failed',
                    attempt_count = attempt_count + 1,
                    last_attempted_at = NOW(),
                    error_message = $1
                WHERE notification_id = $2
                AND channel = 'email'`,
                [err.message, notificationId]
            );
            await pool.query(
                `UPDATE notifications
                SET status = 'failed',
                    updated_at = NOW()
                WHERE id = $1`,
                [notificationId]
            );
        }catch(updateErr){
            logger.error(`Error occurred while updating delivery attempt or notification status for notification ${notificationId}: ${updateErr.message}`);
        }
        logger.error(`Error occurred while processing notification ${notificationId} for email delivery: ${err.message}`);

        throw err;
    }finally{
        client.release();
    }
}

const startEmailWorker=async()=>{
    await emailService.verifyConnection();
    await consumer.connect();
    logger.info('Email worker connected to Kafka');
    await consumer.subscribe({topic:'notifications',fromBeginning:false});
    logger.info('Email worker subscribed to notifications topic');

    await consumer.run({
        eachMessage:async({topic,partition, message})=>{

            const notificationData=JSON.parse(message.value.toString());
            logger.info(`Email worker received message — ` +`topic: ${topic}, partition: ${partition}, ` +`notificationId: ${notificationData.notificationId}`);

            await processNotification(notificationData);
        }
    });

    logger.info('Email worker is running and processing messages from Kafka');
}

startEmailWorker().catch((err)=>{
    logger.error('Error occurred in email worker:', err);
    process.exit(1);
})

