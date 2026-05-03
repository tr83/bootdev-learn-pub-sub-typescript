import amqp, { type Channel, type ConfirmChannel } from "amqplib";

export enum SimpleQueueType {
    Durable,
    Transient,
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
    } catch (err) {
        console.error("Error declaring queue:", err);
        throw err;
    }
}

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => void,
): Promise<void> {
    try {
        const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);

        await ch.consume(queue.queue, (msg) => {
            if (!msg) {
                return;
            }

            const data = JSON.parse(msg.content.toString()) as T;
            handler(data);
            ch.ack(msg);
        });
    } catch (err) {
        console.error("Error subscribing to queue:", err);
        throw err;
    }
}