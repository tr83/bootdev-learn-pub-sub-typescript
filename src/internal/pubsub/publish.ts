import amqp, { type Channel, type ConfirmChannel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const content = Buffer.from(JSON.stringify(value));

  return new Promise((resolve, reject) => {
    ch.publish(
      exchange,
      routingKey,
      content,
      { contentType: "application/json" },
      (err) => {
        if (err !== null) {
          reject(new Error("Message was NACKed by the broker"));
        } else {
          resolve();
        }
      },
    );
  });
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  try {
    const ch = await conn.createChannel();

    const queue = await ch.assertQueue(queueName, {
      durable: queueType === SimpleQueueType.Durable,
      autoDelete: queueType === SimpleQueueType.Transient,
      exclusive: queueType === SimpleQueueType.Transient,
    });

    await ch.bindQueue(queue.queue, exchange, key);
    
    return [ch, queue];
  }  catch (err) {
    console.error("Error declaring queue:", err);
    throw err;
  }
}