// Casos de prueba sobre la arquitectura de MENSAJES (RabbitMQ)
// Aplica: CP-01, CP-02, CP-03, CP-06, CP-07, CP-08, CP-09, CP-10, CP-11, CP-12
const { Evidencia, captura, contenedores, compose, logs, filtrar, crearPedido, health, esperar, sleep } = require('./lib');

const ARQ = 'rabbitmq';
const MGMT = process.env.RABBIT_MGMT || 'http://127.0.0.1:15672';
const AUTH = 'Basic ' + Buffer.from('admin:admin').toString('base64');
const COLA = 'pedidos';
const WORKER = 'notification-worker';
const pedido = (n = 1) => ({ customerName: `Cliente QA ${n}`, product: 'Libro de arquitectura', quantity: n });

// ---------- API de administración de RabbitMQ ----------
async function mgmt(ruta, opts = {}) {
  const r = await fetch(`${MGMT}/api${ruta}`, { ...opts, headers: { Authorization: AUTH, 'Content-Type': 'application/json', ...(opts.headers || {}) } });
  if (!r.ok) throw new Error(`Management API ${r.status} en ${ruta}`);
  const txt = await r.text();
  return txt ? JSON.parse(txt) : null;
}
const cola = () => mgmt(`/queues/%2F/${COLA}`);
const resumenCola = (q) => `Ready=${q.messages_ready} Unacked=${q.messages_unacknowledged} Total=${q.messages} Consumidores=${q.consumers}`;
// Ver mensajes de la cola sin consumirlos (se reencolan)
const espiar = (n = 50) => mgmt(`/queues/%2F/${COLA}/get`, { method: 'POST', body: JSON.stringify({ count: n, ackmode: 'ack_requeue_true', encoding: 'auto' }) });
const vaciar = () => mgmt(`/queues/%2F/${COLA}/contents`, { method: 'DELETE' });
// Las estadísticas de la consola se actualizan cada ~5 s: se espera a que el valor cambie
const esperarCola = (cond, timeout = 30000) => esperar(async () => { const q = await cola(); return cond(q) ? q : null; }, { timeout });

async function detenerWorker(ev) { ev.dato('$ docker compose stop notification-worker'); compose(ARQ, `stop ${WORKER}`); await esperarCola((q) => q.consumers === 0); }
async function iniciarWorker(ev) { ev.dato('$ docker compose start notification-worker'); compose(ARQ, `start ${WORKER}`); await esperarCola((q) => q.consumers >= 1); }
const procesado = async (desde, orderId, timeout = 40000) =>
  esperar(() => filtrar(logs(ARQ, WORKER, desde), `PROCESADO orderId=${orderId}`), { timeout });

async function preparar(ev) {
  const h = await health();
  if (!h || h.arquitectura !== 'rabbitmq') {
    ev.check('El laboratorio RabbitMQ está arriba (GET /health)', false,
      'Levante el entorno: cd rabbitmq && docker compose up -d --build  (y apague el de Kafka)');
    return false;
  }
  await esperar(async () => (await health()).broker === 'conectado', { timeout: 60000 });
  compose(ARQ, `up -d --scale ${WORKER}=1 --no-recreate`, { silent: true });
  return true;
}

// ================= CASOS =================
const casos = {
  async 'CP-01'() {
    const ev = new Evidencia(ARQ, 'CP-01', 'Creación de pedido vía API');
    if (!(await preparar(ev))) return ev.cerrar();
    ev.paso('POST /orders con un pedido válido');
    const body = { customerName: 'Carlos', product: 'Libro de arquitectura', quantity: 1 };
    ev.dato(`Body: ${JSON.stringify(body)}`);
    const r = await crearPedido(body);
    ev.dato(`HTTP ${r.status} en ${r.ms} ms → ${JSON.stringify(r.json)}`);
    ev.check('Responde 202 Accepted', r.status === 202);
    ev.check('status = "accepted" y trae orderId', r.json && r.json.status === 'accepted' && !!r.json.orderId, r.json && r.json.orderId);
    ev.paso('POST /orders con datos inválidos (quantity = 0)');
    const m = await crearPedido({ customerName: 'Carlos', product: 'Libro', quantity: 0 });
    ev.dato(`HTTP ${m.status} → ${JSON.stringify(m.json)}`);
    ev.check('Rechaza el JSON inválido con 400', m.status === 400);
    await captura(ev, 'Postman: POST http://localhost:3000/orders con el body válido → 202 accepted');
    return ev.cerrar();
  },

  async 'CP-02'() {
    const ev = new Evidencia(ARQ, 'CP-02', 'Envío de mensaje a RabbitMQ');
    if (!(await preparar(ev))) return ev.cerrar();
    ev.paso('Precondición: worker detenido, para que el mensaje quede visible en la cola');
    await detenerWorker(ev);
    const antes = await cola();
    ev.dato(`Cola antes: ${resumenCola(antes)}`);
    ev.paso('POST /orders');
    const r = await crearPedido(pedido(1));
    ev.dato(`HTTP ${r.status} → orderId=${r.json && r.json.orderId}`);
    const despues = await esperarCola((q) => q.messages_ready >= antes.messages_ready + 1);
    ev.dato(`Cola después: ${despues ? resumenCola(despues) : 'sin cambio'}`);
    ev.check('La cola "pedidos" existe y es durable', antes.durable === true, `durable=${antes.durable}`);
    ev.check('El contador Ready aumentó en 1', !!despues);
    ev.paso('Contenido del mensaje en la cola (vista sin consumir)');
    const msgs = await espiar();
    const m = msgs.find((x) => x.payload.includes(r.json.orderId));
    if (m) ev.bloque('Mensaje en RabbitMQ', `properties: ${JSON.stringify({ message_id: m.properties.message_id, correlation_id: m.properties.correlation_id, delivery_mode: m.properties.delivery_mode, type: m.properties.type })}\npayload: ${m.payload}`);
    ev.check('El mensaje con el orderId está en la cola', !!m);
    ev.check('El mensaje es persistente (delivery_mode = 2)', !!m && m.properties.delivery_mode === 2);
    await captura(ev, 'Consola RabbitMQ → Queues → pedidos: Ready = 1 y "Get messages" mostrando el JSON del pedido');
    ev.paso('Restaurar: iniciar el worker');
    await iniciarWorker(ev);
    return ev.cerrar();
  },

  async 'CP-03'() {
    const ev = new Evidencia(ARQ, 'CP-03', 'Consumo de mensaje (RabbitMQ)');
    if (!(await preparar(ev))) return ev.cerrar();
    const desde = new Date();
    ev.paso('POST /orders con el worker activo');
    const r = await crearPedido(pedido(2));
    ev.dato(`HTTP ${r.status} → orderId=${r.json.orderId}`);
    ev.paso('Esperar a que el worker lo procese y confirme (ack)');
    const ok = await procesado(desde, r.json.orderId);
    ev.bloque('Log del notification-worker', filtrar(logs(ARQ, WORKER, desde), r.json.orderId));
    ev.check('El worker registró RECIBIDO y PROCESADO (ack) para el pedido', !!ok);
    const q = await esperarCola((x) => x.messages === 0);
    ev.dato(`Cola al final: ${q ? resumenCola(q) : resumenCola(await cola())}`);
    ev.check('La cola queda vacía después del ack', !!q);
    await captura(ev, 'Terminal: docker compose logs notification-worker (líneas RECIBIDO / PROCESADO)');
    return ev.cerrar();
  },

  async 'CP-06'() {
    const ev = new Evidencia(ARQ, 'CP-06', 'Trazabilidad del flujo (API → RabbitMQ → worker)');
    if (!(await preparar(ev))) return ev.cerrar();
    const desde = new Date();
    ev.paso('Worker detenido un momento para observar el mensaje dentro del broker');
    await detenerWorker(ev);
    const r = await crearPedido(pedido(3));
    const { orderId, correlationId } = r.json;
    ev.dato(`Pedido: orderId=${orderId} correlationId=${correlationId}`);
    await esperarCola((q) => q.messages_ready >= 1);
    const enBroker = (await espiar()).find((x) => x.payload.includes(orderId));
    await captura(ev, `Consola RabbitMQ → Queues → pedidos → Get messages: mensaje con orderId ${orderId}`);
    await iniciarWorker(ev);
    await procesado(desde, orderId);
    const lApi = filtrar(logs(ARQ, 'orders-api', desde), orderId);
    const lWorker = filtrar(logs(ARQ, WORKER, desde), orderId);
    ev.bloque('1) orders-api', lApi);
    ev.bloque('2) RabbitMQ (cola pedidos)', enBroker ? `message_id=${enBroker.properties.message_id} correlation_id=${enBroker.properties.correlation_id}` : '');
    ev.bloque('3) notification-worker', lWorker);
    ev.check('La API registra el orderId y el correlationId', lApi.includes(correlationId));
    ev.check('El broker tiene el mensaje con el mismo correlationId', !!enBroker && enBroker.properties.correlation_id === correlationId);
    ev.check('El worker registra RECIBIDO y PROCESADO con el mismo correlationId', lWorker.includes('RECIBIDO') && lWorker.includes('PROCESADO') && lWorker.includes(correlationId));
    await captura(ev, `Terminal: docker compose logs orders-api notification-worker | findstr ${orderId}   (en Linux: grep)`);
    return ev.cerrar();
  },

  async 'CP-07'() {
    const ev = new Evidencia(ARQ, 'CP-07', 'Responsive: la API responde sin esperar al consumidor');
    if (!(await preparar(ev))) return ev.cerrar();
    ev.paso('Enviar 5 pedidos seguidos (el worker tarda 8 s por mensaje)');
    const tiempos = [];
    for (let i = 1; i <= 5; i++) { const r = await crearPedido(pedido(i)); tiempos.push(r.ms); ev.dato(`Pedido ${i}: HTTP ${r.status} en ${r.ms} ms`); }
    const q = await esperarCola((x) => x.messages >= 1, 10000);
    ev.dato(`Cola justo después: ${q ? resumenCola(q) : resumenCola(await cola())}`);
    const max = Math.max(...tiempos);
    ev.check('Todas las respuestas llegan en menos de 1 s', max < 1000, `máximo ${max} ms`);
    ev.check('La API respondió antes de que el worker terminara (hay mensajes pendientes)', !!q, 'el procesamiento sigue en segundo plano');
    await captura(ev, 'Esta salida (tiempos de respuesta) y la consola RabbitMQ con mensajes pendientes en la cola');
    ev.paso('Esperar a que el worker vacíe la cola');
    await esperarCola((x) => x.messages === 0, 90000);
    return ev.cerrar();
  },

  async 'CP-08'() {
    const ev = new Evidencia(ARQ, 'CP-08', 'Message-driven: sin comunicación directa entre servicios');
    if (!(await preparar(ev))) return ev.cerrar();
    ev.paso('Puertos publicados por cada contenedor');
    const cs = contenedores(ARQ);
    ev.bloque('docker compose ps', cs.map((c) => `${c.nombre.padEnd(40)} ${c.estado.padEnd(9)} ${c.puertos.join(', ') || '(sin puertos publicados)'}`).join('\n'));
    const ws = cs.filter((c) => c.servicio === WORKER);
    ev.check('El worker no publica puertos (no tiene API HTTP)', ws.length > 0 && ws.every((c) => c.puertos.length === 0));
    ev.paso('Variables de entorno de orders-api: solo conoce al broker');
    const env = compose(ARQ, 'exec -T orders-api printenv').split('\n').filter((l) => /AMQP|QUEUE|WORKER/i.test(l)).join('\n');
    ev.bloque('orders-api printenv', env);
    ev.check('orders-api no tiene ninguna referencia al worker', !/worker/i.test(env));
    ev.paso('Intento de llamada HTTP directa desde orders-api hacia el worker');
    const intento = compose(ARQ, `exec -T orders-api node -e "fetch('http://${WORKER}:3000').then(()=>console.log('ALCANZABLE')).catch(e=>console.log('NO_ALCANZABLE', (e.cause&&e.cause.code)||e.message))"`, { allowFail: true });
    ev.dato(intento);
    ev.check('El worker no es alcanzable por HTTP', intento.includes('NO_ALCANZABLE'));
    ev.paso('Conexiones AMQP abiertas en RabbitMQ');
    const conns = (await mgmt('/connections')).map((c) => (c.client_properties && c.client_properties.connection_name) || c.name);
    ev.bloque('Conexiones', conns.join('\n'));
    ev.check('Productor y consumidor están conectados SOLO al broker', conns.includes('orders-api') && conns.some((c) => c.startsWith('notification-worker')));
    await captura(ev, 'docker compose ps (el worker sin puertos) y Consola RabbitMQ → Connections');
    return ev.cerrar();
  },

  async 'CP-09'() {
    const ev = new Evidencia(ARQ, 'CP-09', 'Resilient: el sistema soporta la caída del consumidor');
    if (!(await preparar(ev))) return ev.cerrar();
    const desde = new Date();
    ev.paso('Enviar un pedido y "matar" al worker mientras lo procesa (sin ack)');
    const r1 = await crearPedido(pedido(1));
    await sleep(500); // el worker está libre (prefetch 1): lo recibe al instante y tarda 8 s en confirmarlo
    ev.dato(`Pedido en proceso: ${r1.json.orderId}`);
    ev.dato('$ docker compose kill notification-worker');
    compose(ARQ, `kill ${WORKER}`);
    const q1 = await esperarCola((q) => q.consumers === 0 && q.messages_ready >= 1);
    ev.dato(`Cola tras la caída: ${q1 ? resumenCola(q1) : resumenCola(await cola())}`);
    ev.check('El mensaje no confirmado vuelve a la cola (no se pierde)', !!q1);
    await captura(ev, 'Consola RabbitMQ → Queues → pedidos: Ready ≥ 1 y Consumers = 0 tras la caída del worker');
    ev.paso('Con el worker caído, la API sigue aceptando pedidos');
    const r2 = await crearPedido(pedido(2)); const r3 = await crearPedido(pedido(3));
    ev.dato(`HTTP ${r2.status} (${r2.ms} ms) y HTTP ${r3.status} (${r3.ms} ms)`);
    ev.check('La API responde 202 aunque no haya consumidor', r2.status === 202 && r3.status === 202);
    ev.paso('Recuperación: iniciar el worker');
    await iniciarWorker(ev);
    const ids = [r1, r2, r3].map((r) => r.json.orderId);
    const todos = await esperar(() => ids.every((id) => logs(ARQ, WORKER, desde).includes(`PROCESADO orderId=${id}`)), { timeout: 60000 });
    ev.bloque('Log del worker', ids.map((id) => filtrar(logs(ARQ, WORKER, desde), id)).join('\n'));
    ev.check('Los 3 pedidos se procesaron, incluido el que estaba en curso', !!todos);
    ev.check('El pedido interrumpido aparece como REENTREGADO', logs(ARQ, WORKER, desde).includes(`RECIBIDO orderId=${r1.json.orderId} correlationId=${r1.json.correlationId} (REENTREGADO)`));
    return ev.cerrar();
  },

  async 'CP-10'() {
    const ev = new Evidencia(ARQ, 'CP-10', 'Elastic: distribución de carga con múltiples consumidores');
    if (!(await preparar(ev))) return ev.cerrar();
    ev.paso('Escalar a 2 workers');
    ev.dato(`$ docker compose up -d --scale ${WORKER}=2`);
    compose(ARQ, `up -d --scale ${WORKER}=2 --no-recreate`);
    const q = await esperarCola((x) => x.consumers === 2, 60000);
    ev.dato(`Cola: ${q ? resumenCola(q) : resumenCola(await cola())}`);
    ev.check('RabbitMQ registra 2 consumidores en la cola', !!q);
    await captura(ev, 'Consola RabbitMQ → Queues → pedidos → Consumers: 2 consumidores');
    const desde = new Date();
    ev.paso('Enviar 6 pedidos');
    const ids = [];
    for (let i = 1; i <= 6; i++) ids.push((await crearPedido(pedido(i))).json.orderId);
    await esperar(() => ids.every((id) => logs(ARQ, WORKER, desde).includes(`PROCESADO orderId=${id}`)), { timeout: 60000 });
    const lineas = filtrar(logs(ARQ, WORKER, desde), 'PROCESADO').split('\n').filter((l) => ids.some((id) => l.includes(id)));
    const porInstancia = {};
    lineas.forEach((l) => { const m = l.match(/\[notification-worker ([^\]]+)\]/); if (m) porInstancia[m[1]] = (porInstancia[m[1]] || 0) + 1; });
    ev.bloque('Mensajes procesados por instancia', Object.entries(porInstancia).map(([k, v]) => `${k}: ${v}`).join('\n'));
    ev.bloque('Log combinado', lineas.join('\n'));
    ev.check('Los 6 pedidos se procesaron', ids.every((id) => lineas.some((l) => l.includes(id))));
    ev.check('La carga se repartió entre las 2 instancias', Object.keys(porInstancia).length >= 2);
    await captura(ev, 'Terminal: docker compose logs notification-worker (dos instancias distintas procesando)');
    ev.paso('Restaurar a 1 worker');
    compose(ARQ, `up -d --scale ${WORKER}=1 --no-recreate`);
    return ev.cerrar();
  },

  async 'CP-11'() {
    const ev = new Evidencia(ARQ, 'CP-11', 'Desacoplamiento temporal: mensajes publicados con el consumidor apagado');
    if (!(await preparar(ev))) return ev.cerrar();
    const desde = new Date();
    ev.paso('Detener el worker y publicar 3 pedidos');
    await detenerWorker(ev);
    const ids = [];
    for (let i = 1; i <= 3; i++) { const r = await crearPedido(pedido(i)); ids.push(r.json.orderId); ev.dato(`HTTP ${r.status} → ${r.json.orderId}`); }
    const q = await esperarCola((x) => x.messages_ready >= 3);
    ev.dato(`Cola con el worker apagado: ${q ? resumenCola(q) : resumenCola(await cola())}`);
    ev.check('Los 3 mensajes se acumulan en la cola (Ready ≥ 3)', !!q);
    await captura(ev, 'Consola RabbitMQ → Queues → pedidos: Ready = 3 con el worker apagado');
    ev.paso('Iniciar el worker más tarde');
    await sleep(3000);
    await iniciarWorker(ev);
    const ok = await esperar(() => ids.every((id) => logs(ARQ, WORKER, desde).includes(`PROCESADO orderId=${id}`)), { timeout: 60000 });
    ev.bloque('Log del worker', filtrar(logs(ARQ, WORKER, desde), 'PROCESADO'));
    ev.check('El worker procesa los 3 mensajes pendientes al iniciar', !!ok);
    const vacia = await esperarCola((x) => x.messages === 0);
    ev.check('La cola queda en 0', !!vacia);
    return ev.cerrar();
  },

  async 'CP-12'() {
    const ev = new Evidencia(ARQ, 'CP-12', 'Recuperación: reinicio del broker sin pérdida de mensajes');
    if (!(await preparar(ev))) return ev.cerrar();
    const desde = new Date();
    ev.paso('Detener el worker, vaciar la cola y publicar 3 pedidos');
    await detenerWorker(ev);
    await vaciar();
    const ids = [];
    for (let i = 1; i <= 3; i++) ids.push((await crearPedido(pedido(i))).json.orderId);
    const antes = await esperarCola((x) => x.messages_ready === 3);
    ev.dato(`Antes del reinicio: ${antes ? resumenCola(antes) : resumenCola(await cola())}`);
    await captura(ev, 'Consola RabbitMQ: Ready = 3 ANTES del reinicio');
    ev.paso('Reiniciar RabbitMQ');
    ev.dato('$ docker compose restart rabbitmq');
    compose(ARQ, 'restart rabbitmq');
    const despues = await esperar(async () => { const q = await cola(); return q.messages_ready === 3 ? q : null; }, { timeout: 90000, cada: 2000 });
    ev.dato(`Después del reinicio: ${despues ? resumenCola(despues) : 'no disponible'}`);
    ev.check('Los 3 mensajes siguen en la cola tras el reinicio (cola durable + mensajes persistentes)', !!despues);
    await captura(ev, 'Consola RabbitMQ (volver a iniciar sesión): Ready = 3 DESPUÉS del reinicio');
    ev.paso('La API se reconecta sola al broker');
    const api = await esperar(async () => (await health()).broker === 'conectado', { timeout: 60000, cada: 2000 });
    const r4 = await crearPedido(pedido(4));
    ev.dato(`Nuevo pedido después del reinicio: HTTP ${r4.status} → ${r4.json && r4.json.orderId}`);
    ev.check('La API acepta pedidos después del reinicio', !!api && r4.status === 202);
    if (r4.json && r4.json.orderId) ids.push(r4.json.orderId);
    ev.paso('Iniciar el worker y procesar lo pendiente');
    await iniciarWorker(ev);
    const ok = await esperar(() => ids.every((id) => logs(ARQ, WORKER, desde).includes(`PROCESADO orderId=${id}`)), { timeout: 60000 });
    ev.bloque('Log del worker', filtrar(logs(ARQ, WORKER, desde), 'PROCESADO'));
    ev.check(`Se procesaron los ${ids.length} pedidos sin pérdida`, !!ok);
    return ev.cerrar();
  },
};

module.exports = casos;
