import express from "express";
import morgan from "morgan";
import { Kafka } from "kafkajs";
import crypto from "crypto";

const PORT = process.env.PORT || 3000;
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const TOPIC = process.env.KAFKA_TOPIC || "events.incoming";

const app = express();
app.use(express.json({ limit: "1mb" }));
app.use(morgan("dev"));

const kafka = new Kafka({
  clientId: "publisher-api",
  brokers: KAFKA_BROKERS
});

const producer = kafka.producer();

function validateEvent(evt) {
  const errors = [];
  if (!evt || typeof evt !== "object") errors.push("Body must be a JSON object.");
  if (!evt?.id || typeof evt.id !== "string") errors.push("Field 'id' (string) is required.");
  if (!evt?.type || typeof evt.type !== "string") errors.push("Field 'type' (string) is required.");
  if (!evt?.timestamp || typeof evt.timestamp !== "string") errors.push("Field 'timestamp' (string) is required.");
  return errors;
}

app.get("/health", (_req, res) => {
  res.json({ status: "ok", topic: TOPIC, brokers: KAFKA_BROKERS });
});

app.post("/api/messages", async (req, res) => {
  const evt = req.body;
  const errors = validateEvent(evt);

  if (errors.length) {
    return res.status(400).json({ error: "validation_error", details: errors });
  }

  const correlationId = req.header("x-correlation-id") || crypto.randomUUID();

  // Publicamos el evento como JSON string
  const payload = JSON.stringify(evt);

  try {
    await producer.send({
      topic: TOPIC,
      messages: [
        {
          key: evt.id, // clave por id (útil para particionado/orden)
          value: payload,
          headers: {
            correlationId: Buffer.from(correlationId),
            eventType: Buffer.from(evt.type)
          }
        }
      ]
    });

    console.log(`[PUBLISHER] Published event id=${evt.id} type=${evt.type} correlationId=${correlationId} topic=${TOPIC}`);

    return res.status(202).json({
      status: "accepted",
      topic: TOPIC,
      correlationId,
      eventId: evt.id
    });
  } catch (e) {
    console.error("[PUBLISHER] Kafka send error:", e);
    return res.status(500).json({ error: "kafka_publish_failed" });
  }
});

async function start() {
  await producer.connect();
  console.log(`[PUBLISHER] Connected to Kafka brokers=${KAFKA_BROKERS.join(",")} topic=${TOPIC}`);
  app.listen(PORT, () => console.log(`[PUBLISHER] API listening on http://localhost:${PORT}`));
}

start().catch((e) => {
  console.error("[PUBLISHER] Fatal startup error:", e);
  process.exit(1);
});
