// Casos de prueba sobre la arquitectura de EVENTOS (Apache Kafka)
// Aplica: CP-01, CP-04, CP-05, CP-06, CP-07, CP-08, CP-09, CP-10, CP-11, CP-12
const { Kafka, logLevel, AssignerProtocol } = require('kafkajs');
const { Evidencia, captura, contenedores, compose, logs, filtrar, crearPedido, health, esperar, sleep } = require('./lib');

const ARQ = 'kafka';
const TOPIC = 'orders.events';
const BOOTSTRAP = process.env.KAFKA_EXTERNAL || '127.0.0.1:29092';
const GRUPOS = {
  'inventory-consumer': 'inventory-service-group',
  'billing-consumer': 'billing-service-group',
  'notification-consumer': 'notification-service-group',
};
const CONSUMIDORES = Object.keys(GRUPOS);
const pedido = (n = 1) => ({ customerName: `Cliente QA ${n}`, product: 'Libro de arquitectura', quantity: n, unitPrice: 85000 });

const kafka = new Kafka({ clientId: 'qa-runner', brokers: [BOOTSTRAP], logLevel: logLevel.NOTHING, retry: { retries: 3 } });

async function conAdmin(fn) {
  const admin = kafka.admin();
  await admin.connect();
  try { return await fn(admin); } finally { await admin.disconnect().catch(() => {}); }
}
// Offsets del tópico: high watermark por partición
const offsetsTopico = () => conAdmin((a) => a.fetchTopicOffsets(TOPIC));
const totalEventos = (offs) => offs.reduce((s, p) => s + Number(p.high), 0);
// Lag de un grupo = eventos publicados que el grupo aún no ha confirmado
async function lag(groupId) {
  return conAdmin(async (a) => {
    const topic = await a.fetchTopicOffsets(TOPIC);
    const [g] = await a.fetchOffsets({ groupId, topics: [TOPIC] });
    let total = 0; const det = [];
    for (const p of topic) {
      const c = g.partitions.find((x) => x.partition === p.partition);
      const committed = c && Number(c.offset) >= 0 ? Number(c.offset) : Number(p.low);
      const l = Number(p.high) - committed; total += l;
      det.push(`p${p.partition}: committed=${committed} high=${p.high} lag=${l}`);
    }
    return { total, detalle: det.join(' | ') };
  });
}
// Lee del tópico el evento de un pedido (con un grupo temporal que lee desde el inicio)
async function leerEvento(orderId, timeout = 30000) {
  const c = kafka.consumer({ groupId: `qa-reader-${Date.now()}` });
  await c.connect();
  await c.subscribe({ topic: TOPIC, fromBeginning: true });
  let encontrado = null;
  await c.run({ eachMessage: async ({ partition, message }) => {
    if (message.key && message.key.toString() === orderId) encontrado = { partition, offset: message.offset, value: JSON.parse(message.value.toString()) };
  } });
  await esperar(() => encontrado, { timeout, cada: 500 });
  await c.disconnect();
  return encontrado;
}
const procesadoPor = (desde, servicio, orderId) => logs(ARQ, servicio, desde).includes(`PROCESADO orderId=${orderId}`);
const esperarProcesado = (desde, servicios, ids, timeout = 90000) =>
  esperar(() => servicios.every((s) => ids.every((id) => procesadoPor(desde, s, id))), { timeout, cada: 2000 });

async function preparar(ev) {
  const h = await health();
  if (!h || h.arquitectura !== 'kafka') {
    ev.check('El laboratorio Kafka está arriba (GET /health)', false,
      'Levante el entorno: cd kafka && docker compose up -d --build  (y apague el de RabbitMQ)');
    return false;
  }
  await esperar(async () => (await health()).broker === 'conectado', { timeout: 90000 });
  compose(ARQ, `up -d --no-recreate ${CONSUMIDORES.map((c) => `--scale ${c}=1`).join(' ')}`, { silent: true });
  return true;
}

// ================= CASOS =================
const casos = {
  async 'CP-01'() {
    const ev = new Evidencia(ARQ, 'CP-01', 'Creación de pedido vía API');
    if (!(await preparar(ev))) return ev.cerrar();
    ev.paso('POST /orders con un pedido válido');
    const body = { customerName: 'Carlos', product: 'Libro de arquitectura', quantity: 1, unitPrice: 85000 };
    ev.dato(`Body: ${JSON.stringify(body)}`);
    const r = await crearPedido(body);
    ev.dato(`HTTP ${r.status} en ${r.ms} ms → ${JSON.stringify(r.json)}`);
    ev.check('Responde 202 Accepted', r.status === 202);
    ev.check('status = "accepted" y trae orderId', r.json && r.json.status === 'accepted' && !!r.json.orderId, r.json && r.json.orderId);
    ev.paso('POST /orders con datos inválidos (sin unitPrice)');
    const m = await crearPedido({ customerName: 'Carlos', product: 'Libro', quantity: 1 });
    ev.dato(`HTTP ${m.status} → ${JSON.stringify(m.json)}`);
    ev.check('Rechaza el JSON inválido con 400', m.status === 400);
    await captura(ev, 'Postman: POST http://localhost:3000/orders con el body válido → 202 accepted');
    return ev.cerrar();
  },

  async 'CP-04'() {
    const ev = new Evidencia(ARQ, 'CP-04', 'Publicación de evento OrderCreated en Kafka');
    if (!(await preparar(ev))) return ev.cerrar();
    ev.paso('Tópico y particiones');
    const meta = await conAdmin((a) => a.fetchTopicMetadata({ topics: [TOPIC] }));
    const nPart = meta.topics[0].partitions.length;
    ev.dato(`Tópico ${TOPIC}: ${nPart} particiones`);
    ev.check('El tópico orders.events existe con al menos 2 particiones', nPart >= 2, `${nPart} particiones`);
    const antes = totalEventos(await offsetsTopico());
    ev.paso('POST /orders');
    const r = await crearPedido(pedido(1));
    ev.dato(`HTTP ${r.status} → orderId=${r.json.orderId}`);
    const despues = await esperar(async () => { const t = totalEventos(await offsetsTopico()); return t > antes ? t : null; }, { timeout: 15000 });
    ev.dato(`Eventos en el tópico: antes=${antes} después=${despues}`);
    ev.check('El tópico tiene un evento más', despues === antes + 1);
    ev.paso('Leer el evento publicado desde Kafka');
    const e = await leerEvento(r.json.orderId);
    if (e) ev.bloque(`Evento (partition=${e.partition}, offset=${e.offset}, key=${r.json.orderId})`, JSON.stringify(e.value, null, 2));
    const v = e && e.value;
    ev.check('eventType = "OrderCreated" (un hecho en pasado, no un comando)', !!v && v.eventType === 'OrderCreated');
    ev.check('Tiene los metadatos mínimos: eventId, eventVersion, occurredAt, source, correlationId, data', !!v && ['eventId', 'eventVersion', 'occurredAt', 'source', 'correlationId', 'data'].every((k) => v[k] !== undefined));
    ev.check('data.orderId coincide con el pedido creado', !!v && v.data.orderId === r.json.orderId);
    await captura(ev, `Kafka UI → Topics → orders.events → Messages: evento OrderCreated de ${r.json.orderId} (y pestaña de particiones)`);
    return ev.cerrar();
  },

  async 'CP-05'() {
    const ev = new Evidencia(ARQ, 'CP-05', 'Consumo del evento (Kafka)');
    if (!(await preparar(ev))) return ev.cerrar();
    const desde = new Date();
    ev.paso('POST /orders');
    const r = await crearPedido(pedido(2));
    ev.dato(`HTTP ${r.status} → orderId=${r.json.orderId}`);
    ev.paso('Esperar a que los 3 consumidores procesen el MISMO evento');
    await esperarProcesado(desde, CONSUMIDORES, [r.json.orderId]);
    for (const c of CONSUMIDORES) {
      ev.bloque(c, filtrar(logs(ARQ, c, desde), r.json.orderId));
      ev.check(`${c} recibió y procesó el evento`, procesadoPor(desde, c, r.json.orderId));
    }
    ev.paso('Offsets confirmados por cada consumer group');
    await sleep(6000); // los offsets se confirman automáticamente cada pocos segundos
    for (const c of CONSUMIDORES) {
      const l = await lag(GRUPOS[c]);
      ev.dato(`${GRUPOS[c]} → ${l.detalle}`);
      ev.check(`${GRUPOS[c]} sin eventos pendientes (lag = 0)`, l.total === 0);
    }
    await captura(ev, 'Logs de los 3 consumidores y Kafka UI → Consumers: los 3 grupos con lag 0');
    return ev.cerrar();
  },

  async 'CP-06'() {
    const ev = new Evidencia(ARQ, 'CP-06', 'Trazabilidad del flujo (API → Kafka → consumidores)');
    if (!(await preparar(ev))) return ev.cerrar();
    const desde = new Date();
    const r = await crearPedido(pedido(3));
    const { orderId, correlationId } = r.json;
    ev.dato(`Pedido: orderId=${orderId} correlationId=${correlationId}`);
    await esperarProcesado(desde, CONSUMIDORES, [orderId]);
    const lApi = filtrar(logs(ARQ, 'orders-api', desde), orderId);
    const e = await leerEvento(orderId);
    ev.bloque('1) orders-api', lApi);
    ev.bloque('2) Kafka (tópico orders.events)', e ? `partition=${e.partition} offset=${e.offset} eventId=${e.value.eventId} correlationId=${e.value.correlationId}` : '');
    ev.check('La API registra orderId y correlationId', lApi.includes(correlationId));
    ev.check('El evento en Kafka tiene el mismo correlationId', !!e && e.value.correlationId === correlationId);
    CONSUMIDORES.forEach((c, i) => {
      const l = filtrar(logs(ARQ, c, desde), orderId);
      ev.bloque(`${i + 3}) ${c}`, l);
      ev.check(`${c} registra el mismo correlationId`, l.includes(correlationId) && l.includes('PROCESADO'));
    });
    await captura(ev, `Logs de orders-api y de los 3 consumidores filtrados por ${orderId}, y el mensaje en Kafka UI`);
    return ev.cerrar();
  },

  async 'CP-07'() {
    const ev = new Evidencia(ARQ, 'CP-07', 'Responsive: la API responde sin esperar a los consumidores');
    if (!(await preparar(ev))) return ev.cerrar();
    const desde = new Date();
    ev.paso('Enviar 5 pedidos seguidos (cada consumidor tarda 1 s por evento)');
    const tiempos = []; const ids = [];
    for (let i = 1; i <= 5; i++) { const r = await crearPedido(pedido(i)); tiempos.push(r.ms); ids.push(r.json.orderId); ev.dato(`Pedido ${i}: HTTP ${r.status} en ${r.ms} ms`); }
    const pendientes = (await lag(GRUPOS['inventory-consumer'])).total;
    ev.dato(`Eventos aún pendientes en inventory-service-group justo después: ${pendientes}`);
    const max = Math.max(...tiempos);
    ev.check('Todas las respuestas llegan en menos de 1 s', max < 1000, `máximo ${max} ms`);
    ev.check('Las respuestas tardan menos que el procesamiento de un solo consumidor (1 s)', max < 1000);
    await captura(ev, 'Esta salida (tiempos de respuesta) y Kafka UI → Consumers con lag mientras procesan');
    await esperarProcesado(desde, CONSUMIDORES, ids);
    ev.dato('Los consumidores terminaron después, en segundo plano.');
    return ev.cerrar();
  },

  async 'CP-08'() {
    const ev = new Evidencia(ARQ, 'CP-08', 'Message-driven: sin comunicación directa entre servicios');
    if (!(await preparar(ev))) return ev.cerrar();
    ev.paso('Puertos publicados por cada contenedor');
    const cs = contenedores(ARQ);
    ev.bloque('docker compose ps', cs.map((c) => `${c.nombre.padEnd(40)} ${c.estado.padEnd(9)} ${c.puertos.join(', ') || '(sin puertos publicados)'}`).join('\n'));
    const lc = cs.filter((c) => CONSUMIDORES.includes(c.servicio));
    ev.check('Ningún consumidor publica puertos', lc.length >= 3 && lc.every((c) => c.puertos.length === 0));
    ev.paso('Variables de entorno de orders-api');
    const env = compose(ARQ, 'exec -T orders-api printenv').split('\n').filter((l) => /KAFKA|TOPIC|CONSUMER/i.test(l)).join('\n');
    ev.bloque('orders-api printenv', env);
    ev.check('orders-api solo conoce al broker y al tópico', !/consumer/i.test(env));
    ev.paso('Intento de llamada HTTP directa desde orders-api hacia un consumidor');
    const intento = compose(ARQ, `exec -T orders-api node -e "fetch('http://inventory-consumer:3000').then(()=>console.log('ALCANZABLE')).catch(e=>console.log('NO_ALCANZABLE', (e.cause&&e.cause.code)||e.message))"`, { allowFail: true });
    ev.dato(intento);
    ev.check('El consumidor no es alcanzable por HTTP', intento.includes('NO_ALCANZABLE'));
    ev.paso('Consumer groups registrados en Kafka (cada consumidor tiene el suyo)');
    const grupos = await conAdmin((a) => a.listGroups());
    const nombres = grupos.groups.map((g) => g.groupId).filter((g) => !g.startsWith('qa-reader'));
    ev.bloque('Grupos', nombres.join('\n'));
    ev.check('Existen los 3 grupos funcionales distintos', Object.values(GRUPOS).every((g) => nombres.includes(g)));
    await captura(ev, 'docker compose ps (consumidores sin puertos) y Kafka UI → Consumers: 3 grupos');
    return ev.cerrar();
  },

  async 'CP-09'() {
    const ev = new Evidencia(ARQ, 'CP-09', 'Resilient: el sistema soporta la caída de un consumidor');
    if (!(await preparar(ev))) return ev.cerrar();
    const desde = new Date();
    ev.paso('Detener billing-consumer');
    ev.dato('$ docker compose stop billing-consumer');
    compose(ARQ, 'stop billing-consumer');
    ev.paso('Publicar 3 pedidos con billing caído');
    const ids = [];
    for (let i = 1; i <= 3; i++) { const r = await crearPedido(pedido(i)); ids.push(r.json.orderId); ev.dato(`HTTP ${r.status} (${r.ms} ms) → ${r.json.orderId}`); }
    ev.check('La API sigue respondiendo 202', ids.every(Boolean));
    await esperarProcesado(desde, ['inventory-consumer', 'notification-consumer'], ids);
    ev.check('Inventario y notificaciones procesaron los 3 eventos sin depender de billing', ['inventory-consumer', 'notification-consumer'].every((c) => ids.every((id) => procesadoPor(desde, c, id))));
    const l1 = await lag(GRUPOS['billing-consumer']);
    ev.dato(`billing-service-group con el consumidor caído → ${l1.detalle}`);
    ev.check('Los eventos esperan a billing en el log (lag ≥ 3)', l1.total >= 3, `lag=${l1.total}`);
    await captura(ev, 'Kafka UI → Consumers → billing-service-group: lag ≥ 3 con el consumidor caído');
    ev.paso('Recuperación: iniciar billing-consumer');
    compose(ARQ, 'start billing-consumer');
    await esperarProcesado(desde, ['billing-consumer'], ids);
    ev.bloque('billing-consumer', filtrar(logs(ARQ, 'billing-consumer', desde), 'PROCESADO'));
    ev.check('billing procesó los 3 eventos pendientes al volver', ids.every((id) => procesadoPor(desde, 'billing-consumer', id)));
    return ev.cerrar();
  },

  async 'CP-10'() {
    const ev = new Evidencia(ARQ, 'CP-10', 'Elastic: distribución de carga con múltiples consumidores');
    if (!(await preparar(ev))) return ev.cerrar();
    ev.paso('Escalar inventory-consumer a 2 instancias (mismo consumer group)');
    ev.dato('$ docker compose up -d --scale inventory-consumer=2');
    compose(ARQ, 'up -d --no-recreate --scale inventory-consumer=2');
    const grupo = await esperar(() => conAdmin(async (a) => {
      const d = (await a.describeGroups([GRUPOS['inventory-consumer']])).groups[0];
      return d.state === 'Stable' && d.members.length === 2 ? d : null;
    }), { timeout: 90000, cada: 3000 });
    ev.check('El grupo inventory-service-group tiene 2 miembros', !!grupo);
    const asign = grupo ? grupo.members.map((m) => { const a = AssignerProtocol.MemberAssignment.decode(m.memberAssignment); return `${m.clientId}: particiones ${(a && a.assignment[TOPIC] || []).join(',')}`; }) : [];
    ev.bloque('Asignación de particiones por miembro', asign.join('\n'));
    ev.check('Cada miembro tiene al menos una partición asignada', asign.length === 2 && asign.every((x) => /\d/.test(x.split('particiones')[1])));
    await captura(ev, 'Kafka UI → Consumers → inventory-service-group: 2 miembros con particiones asignadas');
    const desde = new Date();
    await sleep(3000);
    ev.paso('Enviar 20 pedidos (claves distintas → particiones distintas)');
    const ids = [];
    for (let i = 1; i <= 20; i++) ids.push((await crearPedido(pedido(i))).json.orderId);
    await esperarProcesado(desde, ['inventory-consumer'], ids);
    const lineas = filtrar(logs(ARQ, 'inventory-consumer', desde), 'PROCESADO').split('\n').filter((l) => ids.some((id) => l.includes(id)));
    const porInstancia = {};
    lineas.forEach((l) => { const m = l.match(/\[inventory-consumer ([^\]]+)\]/); const p = l.match(/partition=(\d+)/);
      if (m) { porInstancia[m[1]] = porInstancia[m[1]] || { n: 0, part: new Set() }; porInstancia[m[1]].n++; if (p) porInstancia[m[1]].part.add(p[1]); } });
    ev.bloque('Eventos procesados por instancia', Object.entries(porInstancia).map(([k, v]) => `${k}: ${v.n} eventos, particiones ${[...v.part].join(',')}`).join('\n'));
    ev.check('Los 20 eventos se procesaron', ids.every((id) => lineas.some((l) => l.includes(id))));
    ev.check('La carga se repartió entre las 2 instancias', Object.keys(porInstancia).length >= 2);
    await captura(ev, 'Terminal: docker compose logs inventory-consumer (dos instancias y sus particiones)');
    ev.paso('Restaurar a 1 instancia');
    compose(ARQ, 'up -d --no-recreate --scale inventory-consumer=1');
    return ev.cerrar();
  },

  async 'CP-11'() {
    const ev = new Evidencia(ARQ, 'CP-11', 'Desacoplamiento temporal y consumidor posterior');
    if (!(await preparar(ev))) return ev.cerrar();
    const desde = new Date();
    ev.paso('A) Detener notification-consumer y publicar 3 pedidos');
    compose(ARQ, 'stop notification-consumer');
    const ids = [];
    for (let i = 1; i <= 3; i++) { const r = await crearPedido(pedido(i)); ids.push(r.json.orderId); ev.dato(`HTTP ${r.status} → ${r.json.orderId}`); }
    const l1 = await lag(GRUPOS['notification-consumer']);
    ev.dato(`notification-service-group apagado → ${l1.detalle}`);
    ev.check('Los eventos quedan pendientes para el grupo apagado (lag ≥ 3)', l1.total >= 3);
    await captura(ev, 'Kafka UI → Consumers → notification-service-group: lag ≥ 3 con el consumidor apagado');
    ev.paso('Iniciar notification-consumer más tarde');
    await sleep(3000);
    compose(ARQ, 'start notification-consumer');
    await esperarProcesado(desde, ['notification-consumer'], ids);
    ev.bloque('notification-consumer', filtrar(logs(ARQ, 'notification-consumer', desde), 'PROCESADO'));
    ev.check('Procesa los 3 eventos desde su último offset confirmado', ids.every((id) => procesadoPor(desde, 'notification-consumer', id)));
    ev.paso('B) Consumidor POSTERIOR: un grupo nuevo que arranca después de publicar');
    const grupoNuevo = `audit-service-group-${Date.now()}`;
    ev.dato(`$ AUDIT_GROUP_ID=${grupoNuevo} docker compose --profile tardio up -d audit-consumer`);
    const total = totalEventos(await offsetsTopico());
    try {
      process.env.AUDIT_GROUP_ID = grupoNuevo;
      process.env.COMPOSE_PROFILES = 'tardio'; // habilita el servicio audit-consumer para los comandos siguientes
      compose(ARQ, 'up -d --build audit-consumer');
      const auditOk = await esperar(() => ids.every((id) => procesadoPor(desde, 'audit-consumer', id)), { timeout: 120000, cada: 3000 });
      const la = filtrar(logs(ARQ, 'audit-consumer', desde), 'PROCESADO').split('\n').filter(Boolean);
      ev.dato(`El auditor leyó ${la.length} eventos del log (el tópico tiene ${total}).`);
      ev.bloque('audit-consumer (primeras y últimas líneas)', [...la.slice(0, 3), '...', ...la.slice(-3)].join('\n'));
      ev.check('El consumidor posterior lee desde el inicio del log, incluidos los 3 pedidos', !!auditOk);
      await captura(ev, 'Terminal: docker compose logs audit-consumer (lee desde offset 0) y Kafka UI → Consumers: el grupo nuevo');
    } finally {
      compose(ARQ, 'rm -sf audit-consumer', { allowFail: true });
      delete process.env.COMPOSE_PROFILES;
      delete process.env.AUDIT_GROUP_ID;
    }
    return ev.cerrar();
  },

  async 'CP-12'() {
    const ev = new Evidencia(ARQ, 'CP-12', 'Recuperación: reinicio del consumidor y del broker sin pérdida');
    if (!(await preparar(ev))) return ev.cerrar();
    const desde = new Date();
    ev.paso('Detener inventory-consumer y publicar 3 pedidos');
    compose(ARQ, 'stop inventory-consumer');
    const ids = [];
    for (let i = 1; i <= 3; i++) ids.push((await crearPedido(pedido(i))).json.orderId);
    const antes = totalEventos(await offsetsTopico());
    const l1 = await lag(GRUPOS['inventory-consumer']);
    ev.dato(`Antes del reinicio: eventos en el tópico=${antes}; inventory lag=${l1.total}`);
    await captura(ev, 'Kafka UI → Topics → orders.events: número de mensajes ANTES del reinicio');
    ev.paso('Reiniciar el broker Kafka');
    ev.dato('$ docker compose restart kafka');
    compose(ARQ, 'restart kafka');
    const despues = await esperar(async () => totalEventos(await offsetsTopico()), { timeout: 120000, cada: 3000 });
    ev.dato(`Después del reinicio: eventos en el tópico=${despues}`);
    ev.check('El log de eventos se conserva tras el reinicio (volumen persistente)', despues >= antes, `${despues} ≥ ${antes}`);
    await captura(ev, 'Kafka UI → Topics → orders.events: mismos mensajes DESPUÉS del reinicio');
    ev.paso('La API vuelve a publicar después del reinicio');
    const r4 = await esperar(async () => { const r = await crearPedido(pedido(4)); return r.status === 202 ? r : null; }, { timeout: 60000, cada: 3000 });
    ev.dato(`Nuevo pedido: ${r4 ? `HTTP 202 → ${r4.json.orderId}` : 'sin respuesta 202'}`);
    ev.check('La API acepta pedidos después del reinicio', !!r4);
    if (r4) ids.push(r4.json.orderId);
    ev.paso('Iniciar inventory-consumer');
    compose(ARQ, 'start inventory-consumer');
    await esperarProcesado(desde, CONSUMIDORES, ids, 150000);
    ev.bloque('inventory-consumer', filtrar(logs(ARQ, 'inventory-consumer', desde), 'PROCESADO'));
    ev.check(`Los ${ids.length} pedidos llegaron a los 3 consumidores sin pérdida`, CONSUMIDORES.every((c) => ids.every((id) => procesadoPor(desde, c, id))));
    await sleep(6000);
    const l2 = await lag(GRUPOS['inventory-consumer']);
    ev.dato(`inventory lag final → ${l2.detalle}`);
    ev.check('inventory-service-group quedó al día (lag = 0)', l2.total === 0);
    return ev.cerrar();
  },
};

module.exports = casos;
