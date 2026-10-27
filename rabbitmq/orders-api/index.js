// orders-api (RabbitMQ) — PRODUCTOR de mensajes
// Recibe pedidos por HTTP y publica un mensaje "ProcessOrder" en la cola "pedidos".
// No conoce al worker ni lo llama: la única comunicación es a través de RabbitMQ.
const express = require('express');
const amqp = require('amqplib');
const { randomUUID } = require('crypto');

const PORT = Number(process.env.PORT || 3000);
const AMQP_URL = process.env.AMQP_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE = process.env.QUEUE || 'pedidos';

let channel = null; // canal con confirmaciones del broker (publisher confirms)
let seq = 1000;

const log = (...a) => console.log(new Date().toISOString(), '[orders-api]', ...a);

// Conexión con reintentos: depends_on no garantiza que RabbitMQ esté listo,
// y si el broker se reinicia (CP-12) la API se reconecta sola.
async function connect() {
  for (let intento = 1; ; intento++) {
    try {
      const conn = await amqp.connect(AMQP_URL, { clientProperties: { connection_name: 'orders-api' } });
      conn.on('error', (e) => log('Error de conexión AMQP:', e.message));
      conn.on('close', () => {
        log('Conexión con RabbitMQ cerrada. Reintentando...');
        channel = null;
        setTimeout(connect, 2000);
      });
      const ch = await conn.createConfirmChannel();
      await ch.assertQueue(QUEUE, { durable: true }); // la cola sobrevive a reinicios del broker
      channel = ch;
      log(`Conectado a RabbitMQ. Cola "${QUEUE}" declarada (durable).`);
      return;
    } catch (e) {
      log(`RabbitMQ no disponible (intento ${intento}): ${e.message}`);
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
}

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => res.json({ servicio: 'orders-api', arquitectura: 'rabbitmq', broker: channel ? 'conectado' : 'desconectado' }));

app.post('/orders', async (req, res) => {
  const { customerName, product, quantity } = req.body || {};

  // 1. Validación básica del JSON
  if (typeof customerName !== 'string' || !customerName.trim() ||
      typeof product !== 'string' || !product.trim() ||
      !Number.isInteger(quantity) || quantity <= 0) {
    return res.status(400).json({
      status: 'rejected',
      message: 'customerName y product son obligatorios; quantity debe ser un entero mayor que 0',
    });
  }
  if (!channel) {
    return res.status(503).json({ status: 'error', message: 'RabbitMQ no disponible, intente de nuevo' });
  }

  // 2. Identificadores para trazabilidad (CP-06)
  const orderId = `ORD-${++seq}-${Date.now().toString(36).toUpperCase()}`;
  const correlationId = randomUUID();

  // 3. Mensaje (comando: "procesar pedido")
  const message = {
    type: 'ProcessOrder',
    orderId,
    correlationId,
    customerName,
    product,
    quantity,
    createdAt: new Date().toISOString(),
  };

  try {
    // 4. Publicar mensaje persistente y esperar la confirmación del broker (no del worker)
    channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(message)), {
      persistent: true,
      contentType: 'application/json',
      messageId: orderId,
      correlationId,
      type: 'ProcessOrder',
    });
    await channel.waitForConfirms();
    log(`PUBLICADO orderId=${orderId} correlationId=${correlationId} cola=${QUEUE}`);
  } catch (e) {
    log(`ERROR al publicar orderId=${orderId}: ${e.message}`);
    return res.status(503).json({ status: 'error', message: 'No se pudo publicar en RabbitMQ' });
  }

  // 5. Responder de inmediato: la API no espera a que el worker procese (CP-07)
  res.status(202).json({
    status: 'accepted',
    message: 'Pedido recibido y enviado a RabbitMQ',
    orderId,
    correlationId,
  });
});

app.listen(PORT, () => log(`Escuchando en el puerto ${PORT}`));
connect();
