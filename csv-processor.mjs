import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import csv from 'csv-parser';
import { configDotenv } from "dotenv";
import { createWriteStream, createReadStream } from 'fs';
import path from 'path';
import { pipeline } from 'stream/promises';

const s3Client = new S3Client();
const sqsClient = new SQSClient();

require('dotenv').config();

export const handler = async (event) => {
    const bucketName = event.Records[0].s3.bucket.name;
    const objectKey = event.Records[0].s3.object.key;
    const downloadPath = `/tmp/${path.basename(objectKey)}`;
    const queueUrl =  process.env.AWS_SQS_QUEUE_URL;
    const errorQueueUrl =  process.env.AWS_SQS_ERROR_QUEUE_URL;

    const params = {
        Bucket: bucketName,
        Key: objectKey
    };

    try {
        const data = await s3Client.send(new GetObjectCommand(params));
        await pipeline(data.Body, createWriteStream(downloadPath));

        const { validEntries, invalidEntries } = await extractEmailsAndMessagesFromCSV(downloadPath);

        for (const { email, message } of validEntries) {
            await sendMessageToSQS(email, message, queueUrl);
        }

        if (invalidEntries.length > 0) {
            console.error('Invalid entries found:', invalidEntries);
            for (const error of invalidEntries) {
                await sendMessageToSQS(error, `Invalid entry: ${JSON.stringify(error)}`, errorQueueUrl);
            }
        }

        return {
            statusCode: 200,
            body: `Successfully processed ${objectKey} and sent messages to SQS.`
        };
    } catch (error) {
        console.error(error);
        return {
            statusCode: 500,
            body: `Failed to process ${objectKey}: ${error.message}`
        };
    }
};

const extractEmailsAndMessagesFromCSV = (filePath) => {
    return new Promise((resolve, reject) => {
        const validEntries = [];
        const invalidEntries = [];
        createReadStream(filePath)
            .pipe(csv())
            .on('data', (row) => {
                if (row.email && row.message) {
                    validEntries.push({ email: row.email, message: row.message });
                } else {
                    invalidEntries.push(row);
                }
            })
            .on('end', () => {
                resolve({ validEntries, invalidEntries });
            })
            .on('error', reject);
    });
};

const sendMessageToSQS = (email, message, queueUrl) => {
    const params = {
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify({ email, message })
    };

    return sqsClient.send(new SendMessageCommand(params));
};
