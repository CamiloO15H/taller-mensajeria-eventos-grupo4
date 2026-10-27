// inventory-consumer (Kafka) — CONSUMIDOR de eventos
// Se suscribe a orders.events con su propio consumer group y reacciona a OrderCreated: Simulando reserva de inventario.
// No expone HTTP ni llama a la API: solo lee el log de eventos de Kafka.
const { Kafka, logLevel } = require('kafkajs');
const os = require('os');

const NAME = process.env.CONSUMER_NAME || 'inventory-consumer';
const BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'inventory-service-group';
const ACTION = process.env.ACTION || 'Simulando reserva de inventario';
const PROCESS_MS = Number(process.env.PROCESS_MS || 1000);
const INSTANCE = os.hostname(); // identifica la instancia cuando se escala (CP-10)

const log = (...a) => console.log(new Date().toISOString(), `[${NAME} ${INSTANCE}]`, ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const kafka = new Kafka({ clientId: `${NAME}-${INSTANCE}`, brokers: [BROKER], logLevel: logLevel.WARN, retry: { retries: 10 } });
const consumer = kafka.consumer({ groupId: GROUP_ID });

// Si el consumidor se cae sin poder recuperarse, el proceso termina y Docker lo reinicia (restart: unless-stopped)
consumer.on(consumer.events.CRASH, (e) => {
  log('El consumidor falló:', e.payload.error && e.payload.error.message);
  if (!e.payload.restart) process.exit(1);
});
consumer.on(consumer.events.GROUP_JOIN, (e) => {
  const asignadas = Object.entries(e.payload.memberAssignment).map(([t, p]) => `${t}[${p.join(',')}]`).join(' ');
  log(`Unido al grupo "${GROUP_ID}". Particiones asignadas: ${asignadas || 'ninguna'}`);
});

async function start() {
  for (let intento = 1; ; intento++) {
    try {
      await consumer.connect();
      // fromBeginning: un grupo NUEVO lee el tópico desde el inicio del log;
      // un grupo existente continúa desde su último offset confirmado.
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
      break;
    } catch (e) {
      log(`Kafka no disponible (intento ${intento}): ${e.message}`);
      await sleep(3000);
    }
  }
  log(`Suscrito a "${TOPIC}" con groupId="${GROUP_ID}"`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      let event;
      try {
        event = JSON.parse(message.value.toString());
      } catch {
        log(`Evento ilegible en partition=${partition} offset=${message.offset}. Se omite.`);
        return;
      }
      if (event.eventType !== 'OrderCreated') return; // solo reacciona al hecho que le interesa
      const d = event.data || {};
      log(`RECIBIDO eventType=${event.eventType} orderId=${d.orderId} correlationId=${event.correlationId} topic=${topic} partition=${partition} offset=${message.offset}`);
      log(`${ACTION} (${d.quantity} x ${d.product}, total ${d.total})...`);
      await sleep(PROCESS_MS);
      log(`PROCESADO orderId=${d.orderId} correlationId=${event.correlationId} partition=${partition} offset=${message.offset}`);
    },
  });
}

process.on('SIGTERM', async () => { log('Deteniendo consumidor (SIGTERM).'); await consumer.disconnect().catch(() => {}); process.exit(0); });
start();
