import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from "@aws-sdk/client-sqs";

const snsClient = new SNSClient();
const sqsClient = new SQSClient();

export const handler = async (event) => {
    const queueUrl = process.env.AWS_SQS_QUEUE_URL;
    const snsTopicArn = process.env.AWS_SNS_TOPIC_ARN;

    try {
        for (const record of event.Records) {
            const message = JSON.parse(record.body);
            const { email, message: customMessage } = message;

            await sendEmailNotification(email, customMessage, snsTopicArn);

            await sqsClient.send(new DeleteMessageCommand({
                QueueUrl: queueUrl,
                ReceiptHandle: record.receiptHandle
            }));
        }

        return {
            statusCode: 200,
            body: `Successfully processed messages and sent notifications.`
        };
    } catch (error) {
        console.error(error);
        return {
            statusCode: 500,
            body: `Failed to process messages: ${error.message}`
        };
    }
};

const sendEmailNotification = (email, message, snsTopicArn) => {
    const params = {
        TopicArn: snsTopicArn,
        Subject: 'Order Processed',
        Message: `The Order was processed. Custom message: ${message}`,
        MessageAttributes: {
            'email': {
                DataType: 'String',
                StringValue: email
            }
        }
    };

    return snsClient.send(new PublishCommand(params));
};
