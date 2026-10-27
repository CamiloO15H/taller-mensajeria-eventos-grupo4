// notification-worker (RabbitMQ) — CONSUMIDOR de mensajes
// Escucha la cola "pedidos", simula el envío de una notificación y confirma (ack) al terminar.
// No expone endpoints HTTP: solo habla con RabbitMQ.
const amqp = require('amqplib');
const os = require('os');

const AMQP_URL = process.env.AMQP_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE = process.env.QUEUE || 'pedidos';
const PROCESS_MS = Number(process.env.PROCESS_MS || 8000); // tiempo simulado de procesamiento
const WORKER = os.hostname(); // identifica la instancia cuando se escala (CP-10)

const log = (...a) => console.log(new Date().toISOString(), `[notification-worker ${WORKER}]`, ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function start() {
  for (let intento = 1; ; intento++) {
    try {
      const conn = await amqp.connect(AMQP_URL, { clientProperties: { connection_name: `notification-worker-${WORKER}` } });
      conn.on('error', (e) => log('Error de conexión AMQP:', e.message));
      conn.on('close', () => {
        log('Conexión con RabbitMQ cerrada. Reintentando...');
        setTimeout(start, 2000);
      });

      const ch = await conn.createChannel();
      await ch.assertQueue(QUEUE, { durable: true });
      ch.prefetch(1); // un mensaje a la vez por worker: reparte la carga entre instancias
      log(`Conectado. Escuchando la cola "${QUEUE}" (prefetch=1, ack manual, procesamiento=${PROCESS_MS} ms)`);

      await ch.consume(QUEUE, async (msg) => {
        if (!msg) return;
        let data;
        try {
          data = JSON.parse(msg.content.toString());
        } catch {
          log('Mensaje inválido (no es JSON). Se descarta sin reencolar.');
          ch.nack(msg, false, false);
          return;
        }
        const redelivered = msg.fields.redelivered ? ' (REENTREGADO)' : '';
        log(`RECIBIDO orderId=${data.orderId} correlationId=${data.correlationId}${redelivered}`);
        log(`Simulando envío de notificación a ${data.customerName}: ${data.quantity} x ${data.product}...`);
        await sleep(PROCESS_MS);
        ch.ack(msg); // confirmación manual: solo aquí RabbitMQ elimina el mensaje
        log(`PROCESADO orderId=${data.orderId} correlationId=${data.correlationId} (ack enviado)`);
      }, { noAck: false });
      return;
    } catch (e) {
      log(`RabbitMQ no disponible (intento ${intento}): ${e.message}`);
      await sleep(3000);
    }
  }
}

process.on('SIGTERM', () => { log('Deteniendo worker (SIGTERM).'); process.exit(0); });
start();
