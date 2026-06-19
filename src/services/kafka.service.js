import pkg from "kafkajs";
import config from "../config/index.js";
import logger from "../utils/logger.js";

const kafka=new pkg.Kafka({
    clientId:'notification-system',
    brokers: [config.kafka.broker],
})

const producer=kafka.producer();

let isProducerConnected=false;

const connectProducer=async()=>{
    if(isProducerConnected) return;

    await producer.connect();
    isProducerConnected=true;
    logger.info("Kafka producer connected");
}

const publishNotification=async(notificationData)=>{
    await connectProducer();

    await producer.send({
        topic:'notifications',
        messages:[
            {
                key:notificationData.userId,
                value:JSON.stringify(notificationData)
            }
        ],
    });
    logger.info(`Notification published for user ${notificationData.userId}`);
};

const disconnectProducer=async()=>{
    if(isProducerConnected){
        await producer.disconnect();
        isProducerConnected=false;
        logger.info("Kafka producer disconnected");
    }
}

export default {
    publishNotification,
    disconnectProducer,
    kafka,
}