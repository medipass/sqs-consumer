import { AWSError } from 'aws-sdk';
import * as SQS from 'aws-sdk/clients/sqs';
import { PromiseResult } from 'aws-sdk/lib/request';
import * as Debug from 'debug';
import { EventEmitter } from 'events';
import { autoBind } from './bind';
import { SQSError, TimeoutError } from './errors';

const debug = Debug('sqs-consumer');

type ReceieveMessageResponse = PromiseResult<SQS.Types.ReceiveMessageResult, AWSError>;
// type ReceiveMessageRequest = SQS.Types.ReceiveMessageRequest;
export type SQSMessage = SQS.Types.Message;

const requiredOptions = [
  'queueUrl',
  // only one of handleMessage / handleMessagesBatch is required
  'handleMessage|handleMessageBatch'
];

interface TimeoutResponse {
  timeout: NodeJS.Timeout;
  pending: Promise<void>;
}

type SqsUrl = string;

function createTimeout(duration: number): TimeoutResponse[] {
  let timeout;
  const pending = new Promise((_, reject) => {
    timeout = setTimeout((): void => {
      reject(new TimeoutError());
    }, duration);
  });
  return [timeout, pending];
}

function assertOptions(options: ConsumerOptions): void {
  requiredOptions.forEach((option) => {
    const possibilities = option.split('|');
    if (!possibilities.find((p) => options[p])) {
      throw new Error(`Missing SQS consumer option [ ${possibilities.join(' or ')} ].`);
    }
  });

  if (options.batchSize > 10 || options.batchSize < 1) {
    throw new Error('SQS batchSize option must be between 1 and 10.');
  }

  if (options.heartbeatInterval && !(options.heartbeatInterval < options.visibilityTimeout)) {
    throw new Error('heartbeatInterval must be less than visibilityTimeout.');
  }
}

function isConnectionError(err: Error): boolean {
  if (err instanceof SQSError) {
    return (err.statusCode === 403 || err.code === 'CredentialsError' || err.code === 'UnknownEndpoint');
  }
  return false;
}

function toSQSError(err: AWSError, message: string): SQSError {
  const sqsError = new SQSError(message);
  sqsError.code = err.code;
  sqsError.statusCode = err.statusCode;
  sqsError.region = err.region;
  sqsError.retryable = err.retryable;
  sqsError.hostname = err.hostname;
  sqsError.time = err.time;

  return sqsError;
}

function hasMessages(response: ReceieveMessageResponse): boolean {
  return response.Messages && response.Messages.length > 0;
}

export interface ConsumerOptions {
  attributeNames?: string[];
  authenticationErrorTimeout?: number;
  batchSize?: number;
  handleMessageTimeout?: number;
  heartbeatInterval?: number;
  messageAttributeNames?: string[];
  pollingWaitTimeMs?: number;
  queueUrl?: string|string[];
  region?: string;
  sqs?: SQS;
  stopped?: boolean;
  terminateVisibilityTimeout?: boolean;
  visibilityTimeout?: number;
  waitTimeSeconds?: number;
  handleMessage?(message: SQSMessage): Promise<void>;
  handleMessageBatch?(messages: SQSMessage[]): Promise<void>;
}

interface Events {
  'response_processed': [SqsUrl];
  'empty': [SqsUrl];
  'message_received': [SqsUrl, SQSMessage];
  'message_processed': [SqsUrl, SQSMessage];
  'error': [Error, void | SQSMessage | SQSMessage[]];
  'timeout_error': [Error, SQSMessage];
  'processing_error': [Error, SQSMessage];
  'stopped': [];
  // 'queue_attribute_retrieval_error': [Error, string[]];
}

export class Consumer extends EventEmitter {
  private queueUrls: string[];
  private queueUrl: string;
  private handleMessage: (message: SQSMessage) => Promise<void>;
  private handleMessageBatch: (message: SQSMessage[]) => Promise<void>;
  private handleMessageTimeout: number;
  private attributeNames: string[];
  private messageAttributeNames: string[];
  private stopped: boolean;
  private batchSize: number;
  private visibilityTimeout: number;
  private waitTimeSeconds: number;
  private authenticationErrorTimeout: number;
  private pollingWaitTimeMs: number;
  private terminateVisibilityTimeout: boolean;
  private heartbeatInterval: number;
  private sqs: SQS;

  constructor(options: ConsumerOptions) {
    super();
    assertOptions(options);
    this.handleMessage = options.handleMessage;
    this.handleMessageBatch = options.handleMessageBatch;
    this.handleMessageTimeout = options.handleMessageTimeout;
    this.attributeNames = options.attributeNames || [];
    this.messageAttributeNames = options.messageAttributeNames || [];
    this.stopped = true;
    this.batchSize = options.batchSize || 1;
    this.visibilityTimeout = options.visibilityTimeout;
    this.terminateVisibilityTimeout = options.terminateVisibilityTimeout || false;
    this.heartbeatInterval = options.heartbeatInterval;
    this.authenticationErrorTimeout = options.authenticationErrorTimeout || 10000;
    this.pollingWaitTimeMs = options.pollingWaitTimeMs || 0;
    this.waitTimeSeconds = options.waitTimeSeconds || 20;

    this.queueUrls = typeof options.queueUrl === 'string' ? [options.queueUrl] : options.queueUrl;
    this.queueUrl = this.queueUrls[0];

    this.sqs = options.sqs || new SQS({
      region: options.region || process.env.AWS_REGION || 'eu-west-1'
    });

    autoBind(this);
  }

  emit<T extends keyof Events>(event: T, ...args: Events[T]) {
    return super.emit(event, ...args);
  }

  on<T extends keyof Events>(event: T, listener: (...args: Events[T]) => void): this {
    return super.on(event, listener);
  }

  once<T extends keyof Events>(event: T, listener: (...args: Events[T]) => void): this {
    return super.once(event, listener);
  }

  public get isRunning(): boolean {
    return !this.stopped;
  }

  public static create(options: ConsumerOptions): Consumer {
    return new Consumer(options);
  }

  public start(): void {
    if (this.stopped) {
      debug('Starting consumer');
      this.stopped = false;
      this.poll();
    }
  }

  public stop(): void {
    debug('Stopping consumer');
    this.stopped = true;
  }

  private async handleSqsResponse(response: ReceieveMessageResponse): Promise<void> {
    debug('Received SQS response');
    debug(response);

    if (response) {
      if (hasMessages(response)) {
        if (this.handleMessageBatch) {
          // prefer handling messages in batch when available
          await this.processMessageBatch(response.Messages);
        } else {
          await Promise.all(response.Messages.map(this.processMessage));
        }
        this.emit('response_processed', this.queueUrl);
      } else {
        this.emit('empty', this.queueUrl);
      }
    }
  }

  private async processMessage(message: SQSMessage): Promise<void> {
    this.emit('message_received', this.queueUrl, message);

    let heartbeat;
    try {
      if (this.heartbeatInterval) {
        heartbeat = this.startHeartbeat(async (elapsedSeconds) => {
          return this.changeVisabilityTimeout(message, elapsedSeconds + this.visibilityTimeout);
        });
      }
      await this.executeHandler(message);
      await this.deleteMessage(message);
      this.emit('message_processed', this.queueUrl, message);
    } catch (err) {
      this.emitError(err, message);

      if (this.terminateVisibilityTimeout) {
        await this.changeVisabilityTimeout(message, 0);
      }
    } finally {
      clearInterval(heartbeat);
    }
  }

  private async receiveMessage(): Promise<ReceieveMessageResponse> {
    for (const [i, queueUrl] of this.queueUrls.entries()) {
      try {
        const receiveParams = {
          QueueUrl: queueUrl,
          AttributeNames: this.attributeNames,
          MessageAttributeNames: this.messageAttributeNames,
          MaxNumberOfMessages: this.batchSize,
          WaitTimeSeconds: this.waitTimeSeconds,
          VisibilityTimeout: this.visibilityTimeout
        };
        const response = await this.sqs.receiveMessage(receiveParams).promise();
        if (hasMessages(response) || i === this.queueUrls.length - 1) {
          this.queueUrl = queueUrl;
          return response;
        }
      } catch (err) {
        throw toSQSError(err, `SQS receive message failed: ${err.message}`);
      }
    }
  }

  private async deleteMessage(message: SQSMessage): Promise<void> {
    debug('Deleting message %s', message.MessageId);

    const deleteParams = {
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle
    };

    try {
      await this.sqs
        .deleteMessage(deleteParams)
        .promise();
    } catch (err) {
      throw toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }

  private async executeHandler(message: SQSMessage): Promise<void> {
    let timeout;
    let pending;
    try {
      if (this.handleMessageTimeout) {
        [timeout, pending] = createTimeout(this.handleMessageTimeout);
        await Promise.race([
          this.handleMessage(message),
          pending
        ]);
      } else {
        await this.handleMessage(message);
      }
    } catch (err) {
      if (err instanceof TimeoutError) {
        err.message = `Message handler timed out after ${this.handleMessageTimeout}ms: Operation timed out.`;
      } else {
        err.message = `Unexpected message handler failure: ${err.message}`;
      }
      throw err;
    } finally {
      clearTimeout(timeout);
    }
  }

  private async changeVisabilityTimeout(message: SQSMessage, timeout: number): Promise<PromiseResult<any, AWSError>> {
    try {
      return this.sqs
        .changeMessageVisibility({
          QueueUrl: this.queueUrl,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: timeout
        })
        .promise();
    } catch (err) {
      this.emit('error', err, message);
    }
  }

  private emitError(err: Error, message: SQSMessage): void {
    if (err.name === SQSError.name) {
      this.emit('error', err, message);
    } else if (err instanceof TimeoutError) {
      this.emit('timeout_error', err, message);
    } else {
      this.emit('processing_error', err, message);
    }
  }

  private poll(): void {
    if (this.stopped) {
      this.emit('stopped');
      return;
    }

    debug('Polling for messages');
    let currentPollingTimeout = this.pollingWaitTimeMs;
    this.receiveMessage()
      .then(this.handleSqsResponse)
      .catch((err) => {
        this.emit('error', err);
        if (isConnectionError(err)) {
          // there was an authentication error, so wait a bit before re-polling
          debug('There was an authentication error. Pausing before retrying.');
          currentPollingTimeout = this.authenticationErrorTimeout;
        }
        return;
      }).then(() => {
        setTimeout(this.poll, currentPollingTimeout);
      }).catch((err) => {
        this.emit('error', err);
      });
  }

  private async processMessageBatch(messages: SQSMessage[]): Promise<void> {
    messages.forEach((message) => {
      this.emit('message_received', this.queueUrl, message);
    });

    let heartbeat;
    try {
      if (this.heartbeatInterval) {
        heartbeat = this.startHeartbeat(async (elapsedSeconds) => {
          return this.changeVisabilityTimeoutBatch(messages, elapsedSeconds + this.visibilityTimeout);
        });
      }
      await this.executeBatchHandler(messages);
      await this.deleteMessageBatch(messages);
      messages.forEach((message) => {
        this.emit('message_processed', this.queueUrl, message);
      });
    } catch (err) {
      this.emit('error', err, messages);

      if (this.terminateVisibilityTimeout) {
        await this.changeVisabilityTimeoutBatch(messages, 0);
      }
    } finally {
      clearInterval(heartbeat);
    }
  }

  private async deleteMessageBatch(messages: SQSMessage[]): Promise<void> {
    debug('Deleting messages %s', messages.map((msg) => msg.MessageId).join(' ,'));

    const deleteParams = {
      QueueUrl: this.queueUrl,
      Entries: messages.map((message) => ({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle
      }))
    };

    try {
      await this.sqs
        .deleteMessageBatch(deleteParams)
        .promise();
    } catch (err) {
      throw toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }

  private async executeBatchHandler(messages: SQSMessage[]): Promise<void> {
    try {
      await this.handleMessageBatch(messages);
    } catch (err) {
      err.message = `Unexpected message handler failure: ${err.message}`;
      throw err;
    }
  }

  private async changeVisabilityTimeoutBatch(messages: SQSMessage[], timeout: number): Promise<PromiseResult<any, AWSError>> {
    const params = {
      QueueUrl: this.queueUrl,
      Entries: messages.map((message) => ({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: timeout
      }))
    };
    try {
      return this.sqs
        .changeMessageVisibilityBatch(params)
        .promise();
    } catch (err) {
      this.emit('error', err, messages);
    }
  }

  private startHeartbeat(heartbeatFn: (elapsedSeconds: number) => void): NodeJS.Timeout {
    const startTime = Date.now();
    return setInterval(() => {
      const elapsedSeconds = Math.ceil((Date.now() - startTime) / 1000);
      heartbeatFn(elapsedSeconds);
    }, this.heartbeatInterval * 1000);
  }
}
