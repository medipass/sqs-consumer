import { Consumer } from "../../../../dist/esm/consumer.js";

import { QUEUE_URL, sqs } from "../sqs.js";

export const consumer = Consumer.create({
  queueUrl: QUEUE_URL,
  sqs,
  pollingWaitTimeMs: 1000,
  pollingCompleteWaitTimeMs: 5000,
  batchSize: 10,
  async handleMessage(message) {
    await new Promise((resolve) => setTimeout(resolve, 1500));
    return message;
  },
});
