// orders-api (Kafka) — PRODUCTOR de eventos
// Recibe pedidos por HTTP y publica el evento de dominio "OrderCreated" en el tópico orders.events.
// No conoce a los consumidores ni los llama: solo publica el hecho ocurrido.
const express = require('express');
const { Kafka, Partitioners, logLevel } = require('kafkajs');
const { randomUUID } = require('crypto');

const PORT = Number(process.env.PORT || 3000);
const BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const PARTITIONS = Number(process.env.TOPIC_PARTITIONS || 3);

const log = (...a) => console.log(new Date().toISOString(), '[orders-api]', ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const kafka = new Kafka({ clientId: 'orders-api', brokers: [BROKER], logLevel: logLevel.WARN, retry: { retries: 10 } });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
let ready = false;
let seq = 1000;

// Conexión con reintentos y creación del tópico con 3 particiones (necesario para repartir carga, CP-10)
async function connect() {
  for (let intento = 1; ; intento++) {
    try {
      const admin = kafka.admin();
      await admin.connect();
      const creado = await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: PARTITIONS, replicationFactor: 1 }] });
      await admin.disconnect();
      log(creado ? `Tópico "${TOPIC}" creado con ${PARTITIONS} particiones.` : `Tópico "${TOPIC}" ya existía.`);
      await producer.connect();
      ready = true;
      log(`Conectado a Kafka (${BROKER}).`);
      return;
    } catch (e) {
      log(`Kafka no disponible (intento ${intento}): ${e.message}`);
      await sleep(3000);
    }
  }
}

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => res.json({ servicio: 'orders-api', arquitectura: 'kafka', broker: ready ? 'conectado' : 'desconectado' }));

app.post('/orders', async (req, res) => {
  const { customerName, product, quantity, unitPrice } = req.body || {};

  // 1. Validación básica del JSON
  if (typeof customerName !== 'string' || !customerName.trim() ||
      typeof product !== 'string' || !product.trim() ||
      !Number.isInteger(quantity) || quantity <= 0 ||
      typeof unitPrice !== 'number' || unitPrice <= 0) {
    return res.status(400).json({
      status: 'rejected',
      message: 'customerName y product son obligatorios; quantity entero > 0; unitPrice número > 0',
    });
  }
  if (!ready) return res.status(503).json({ status: 'error', message: 'Kafka no disponible, intente de nuevo' });

  // 2. Identificadores
  const orderId = `ORD-${++seq}-${Date.now().toString(36).toUpperCase()}`;
  const correlationId = randomUUID();

  // 3. Evento de dominio: un HECHO ocurrido (en pasado), no un comando
  const event = {
    eventId: randomUUID(),
    eventType: 'OrderCreated',
    eventVersion: '1.0',
    occurredAt: new Date().toISOString(),
    source: 'orders-api',
    correlationId,
    data: { orderId, customerName, product, quantity, unitPrice, total: quantity * unitPrice },
  };

  try {
    // 4. Publicar con key = orderId (los eventos del mismo pedido van a la misma partición)
    const [meta] = await producer.send({
      topic: TOPIC,
      acks: -1,
      messages: [{ key: orderId, value: JSON.stringify(event), headers: { eventType: 'OrderCreated', correlationId } }],
    });
    log(`PUBLICADO eventType=OrderCreated orderId=${orderId} correlationId=${correlationId} topic=${TOPIC} partition=${meta.partition} offset=${meta.baseOffset}`);
  } catch (e) {
    log(`ERROR al publicar orderId=${orderId}: ${e.message}`);
    return res.status(503).json({ status: 'error', message: 'No se pudo publicar en Kafka' });
  }

  // 5. Responder de inmediato: no espera a inventario, facturación ni notificaciones (CP-07)
  res.status(202).json({
    status: 'accepted',
    message: 'Pedido recibido y evento OrderCreated publicado en Kafka',
    orderId,
    correlationId,
  });
});

app.listen(PORT, () => log(`Escuchando en el puerto ${PORT}`));
connect();
