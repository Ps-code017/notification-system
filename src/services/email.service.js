import nodemailer from "nodemailer";
import config from "../config/index.js";
import logger from "../utils/logger.js";

const transporter = nodemailer.createTransport({
  host: config.email.host,
  port: config.email.port,
  auth: {
    user: config.email.user,
    pass: config.email.pass
  }
});

const verifyConnection=async()=>{
    try{
        await transporter.verify();
        logger.info('SMTP connection verified successfully');
    } catch (error) {
        logger.error('Error occurred while verifying SMTP connection:', error);
    }
}

const sendEMail=async({to,title,message})=>{
    const mailOptions = {
        from: config.email.from,
        to,
        subject: title,
        text: message
    };
    const info = await transporter.sendMail(mailOptions);
    logger.info(`Email sent: ${info.messageId}`);
    return info;
}

export default {
    sendEMail,
    verifyConnection
};