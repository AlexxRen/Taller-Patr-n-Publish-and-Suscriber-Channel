import { Kafka } from "kafkajs";

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const TOPIC = process.env.KAFKA_TOPIC || "events.incoming";
const GROUP_ID = process.env.GROUP_ID || "sysA-group";

const kafka = new Kafka({
  clientId: "subscriber-a",
  brokers: KAFKA_BROKERS
});

const consumer = kafka.consumer({ groupId: GROUP_ID });

function safeParse(jsonStr) {
  try { return JSON.parse(jsonStr); } catch { return null; }
}

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log(`[SUBSCRIBER-A] Connected. brokers=${KAFKA_BROKERS.join(",")} topic=${TOPIC} groupId=${GROUP_ID}`);

  await consumer.run({
    eachMessage: async ({ message, partition }) => {
      const value = message.value?.toString() ?? "";
      const evt = safeParse(value);

      const correlationId = message.headers?.correlationId?.toString() || "n/a";
      const eventType = message.headers?.eventType?.toString() || "n/a";
      const key = message.key?.toString() || "n/a";

      console.log(`[SUBSCRIBER-A] partition=${partition} key=${key} eventType=${eventType} correlationId=${correlationId}`);
      console.log(`[SUBSCRIBER-A] payload=${evt ? JSON.stringify(evt) : value}`);
    }
  });
}

start().catch((e) => {
  console.error("[SUBSCRIBER-A] Fatal error:", e);
  process.exit(1);
});
