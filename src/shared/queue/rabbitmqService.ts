import { randomUUID } from "node:crypto";
import * as amqp from "amqplib";
import type { Channel, ConsumeMessage } from "amqplib";
import AsyncMutex from "../sync/asyncMutex";
import type { QueueEventType } from "../types/queueMessageType";
import QueueMessageType from "../types/queueMessageType";

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://localhost";
const EXCHANGE_NAME = "pcd_eventos";
const CONNECTION_RETRIES = Number(process.env.RABBITMQ_CONNECTION_RETRIES ?? 30);
const CONNECTION_RETRY_DELAY_MS = Number(process.env.RABBITMQ_CONNECTION_RETRY_DELAY_MS ?? 2000);
const SERVICE_NAME = process.env.SERVICE_NAME ?? process.env.npm_package_name ?? `pcd-${process.pid}`;
const QUEUE_NAME = `${EXCHANGE_NAME}.${SERVICE_NAME}`;

type QueueHandler<T = unknown> = (message: QueueMessageType<T>) => void | Promise<void>;

let channel: Channel | undefined;
const handlers = new Map<QueueEventType, QueueHandler[]>();
const queueMessages: QueueMessageType[] = [];
const queueMessagesMutex = new AsyncMutex();

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export async function listQueueMessages(): Promise<QueueMessageType[]> {
  return queueMessagesMutex.runExclusive(() => queueMessages.map((message) => ({ ...message })));
}

export function subscribeToQueue<T>(event: QueueEventType, handler: QueueHandler<T>): void {
  const eventHandlers = handlers.get(event) ?? [];
  eventHandlers.push(handler as QueueHandler);
  handlers.set(event, eventHandlers);
}

export async function connectQueue(): Promise<boolean> {
  for (let attempt = 1; attempt <= CONNECTION_RETRIES; attempt += 1) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      channel = await connection.createChannel();

      await channel.assertExchange(EXCHANGE_NAME, "fanout", { durable: false });
      await channel.assertQueue(QUEUE_NAME, { durable: false });
      await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, "");
      await channel.prefetch(1);
      await channel.consume(QUEUE_NAME, handleRabbitMessage);

      console.log(`RabbitMQ conectado e fila ${QUEUE_NAME} pronta.`);
      return true;
    } catch (_error) {
      channel = undefined;

      if (attempt < CONNECTION_RETRIES) {
        console.warn(`RabbitMQ indisponivel. Tentando novamente (${attempt}/${CONNECTION_RETRIES})...`);
        await wait(CONNECTION_RETRY_DELAY_MS);
      }
    }
  }

  console.warn(
    "RabbitMQ indisponivel. Usando fila assincrona em memoria; eventos nao serao compartilhados entre microservicos.",
  );
  return false;
}

export async function publishQueueMessage<T>(event: QueueEventType, data: T): Promise<QueueMessageType<T>> {
  const message: QueueMessageType<T> = {
    id: randomUUID(),
    event,
    data,
    createdAt: new Date().toISOString(),
  };

  await queueMessagesMutex.runExclusive(() => {
    queueMessages.unshift(message);
    queueMessages.splice(30);
  });

  if (!channel) {
    setTimeout(() => {
      void dispatchMessage(message).catch((error) => {
        console.error("Erro ao processar mensagem da fila em memoria:", error);
      });
    }, 300);
    return message;
  }

  channel.publish(EXCHANGE_NAME, "", Buffer.from(JSON.stringify(message)));
  return message;
}

async function handleRabbitMessage(msg: ConsumeMessage | null): Promise<void> {
  if (msg === null) {
    return;
  }

  try {
    const message = JSON.parse(msg.content.toString()) as QueueMessageType;
    await dispatchMessage(message);
    channel?.ack(msg);
  } catch (error) {
    console.error("Erro ao processar mensagem RabbitMQ:", error);
    channel?.nack(msg, false, true);
  }
}

async function dispatchMessage(message: QueueMessageType): Promise<void> {
  console.log("Mensagem recebida da fila:", message);

  const eventHandlers = handlers.get(message.event) ?? [];

  for (const handler of eventHandlers) {
    await handler(message);
  }
}
