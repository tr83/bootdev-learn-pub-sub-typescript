import { decode } from "@msgpack/msgpack";
import amqp, { type Channel, type ConfirmChannel } from "amqplib";

export enum SimpleQueueType {
    Durable,
    Transient,
}

export enum AckType {
    Ack,
    NackRequeue,
    NackDiscard,
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
            arguments: {
                "x-dead-letter-exchange": "peril_dlx"
            }
        });

        await ch.bindQueue(queue.queue, exchange, key);

        return [ch, queue];
    } catch (err) {
        console.error("Error declaring queue:", err);
        throw err;
    }
}

export async function subscribe<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    routingKey: string,
    simpleQueueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
    deserializer: (data: Buffer) => T,
): Promise<void> {
    const [ch, queue] = await declareAndBind(conn, exchange, queueName, routingKey, simpleQueueType);

    await ch.consume(queue.queue, async (msg) => {
        if (!msg) {
            return;
        }

        let data: T;
        try {
            data = deserializer(msg.content);
        } catch (err) {
            console.error("Could not unmarshal message:", err);
            return;
        }

        try {
            const ackType = await handler(data);
            switch (ackType) {
                case AckType.Ack:
                    ch.ack(msg);
                    break;
                case AckType.NackDiscard:
                    ch.nack(msg, false, false);
                    break;
                case AckType.NackRequeue:
                    ch.nack(msg, false, true);
                    break;
                default:
                    const unreachable: never = ackType;
                    console.error("Unexpected ack type:", unreachable);
                    return;
            }
        } catch (err) {
            console.error("Error handling message:", err);
            ch.nack(msg, false, false);
            return;
        }
    });
}

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
    const deserializer = (value: Buffer) => JSON.parse(value.toString())
    return subscribe(conn, exchange, queueName, key, queueType, handler, deserializer)
}

export async function subscribeMsgPack<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
    const deserializer = (value: Buffer): T => decode(value) as T
    return subscribe(conn, exchange, queueName, key, queueType, handler, deserializer)
}