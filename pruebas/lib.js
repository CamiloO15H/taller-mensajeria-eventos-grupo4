// Utilidades comunes del ejecutor de casos de prueba
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
const API = process.env.API_URL || 'http://127.0.0.1:3000';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- Registro de evidencia ----------
class Evidencia {
  constructor(arq, id, titulo) {
    this.arq = arq; this.id = id; this.titulo = titulo;
    this.lineas = []; this.checks = [];
    this.inicio = new Date();
    this.out(`${'='.repeat(78)}`);
    this.out(`${id} · ${titulo}`);
    this.out(`Arquitectura: ${arq === 'rabbitmq' ? 'RabbitMQ (mensajes)' : 'Apache Kafka (eventos)'}`);
    this.out(`Fecha de ejecución: ${this.inicio.toISOString()}`);
    this.out(`${'='.repeat(78)}`);
  }
  out(t = '') { console.log(t); this.lineas.push(t); }
  paso(t) { this.out(`\n▶ ${t}`); }
  dato(t) { this.out(`   ${t}`); }
  bloque(titulo, texto) {
    this.out(`   --- ${titulo} ---`);
    String(texto || '(vacío)').trim().split('\n').forEach((l) => this.out(`   | ${l}`));
  }
  check(desc, ok, detalle = '') {
    this.checks.push({ desc, ok });
    this.out(`   ${ok ? '✔' : '✘'} ${desc}${detalle ? ' → ' + detalle : ''}`);
    return ok;
  }
  cerrar() {
    const ok = this.checks.length > 0 && this.checks.every((c) => c.ok);
    const seg = ((Date.now() - this.inicio) / 1000).toFixed(1);
    this.out(`\nRESULTADO ${this.id} (${this.arq}): ${ok ? 'CUMPLE' : 'NO CUMPLE'}  ` +
      `[${this.checks.filter((c) => c.ok).length}/${this.checks.length} verificaciones, ${seg} s]`);
    const dir = path.join(ROOT, 'evidencias', this.arq);
    fs.mkdirSync(dir, { recursive: true });
    const file = path.join(dir, `${this.id}.txt`);
    fs.writeFileSync(file, this.lineas.join('\n') + '\n', 'utf8');
    console.log(`(evidencia guardada en ${path.relative(ROOT, file)})`);
    return { id: this.id, ok };
  }
}

// ---------- Docker Compose ----------
function compose(arq, args, { silent = false, allowFail = false } = {}) {
  const cmd = `docker compose ${args}`;
  try {
    return execSync(cmd, { cwd: path.join(ROOT, arq), encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], maxBuffer: 64 * 1024 * 1024 }).trim();
  } catch (e) {
    if (allowFail) return ((e.stdout || '') + (e.stderr || '')).trim();
    if (!silent) console.error(`Falló: ${cmd}\n${e.stderr || e.message}`);
    throw e;
  }
}

// Logs de un servicio desde un instante (incluye todas las réplicas si está escalado).
// Margen de 2 min: el reloj de la VM de Docker puede ir desfasado; las verificaciones filtran por orderId.
function logs(arq, service, since) {
  const s = since ? `--since ${new Date(since.getTime() - 120000).toISOString()}` : '';
  return compose(arq, `logs --no-color ${s} ${service}`, { allowFail: true });
}
// Contenedores y puertos publicados (docker compose ps en JSON; soporta ambos formatos de salida)
function contenedores(arq) {
  const raw = compose(arq, 'ps --format json');
  let lista;
  try { lista = JSON.parse(raw); } catch { lista = raw.split('\n').filter(Boolean).map((l) => JSON.parse(l)); }
  if (!Array.isArray(lista)) lista = [lista];
  return lista.map((c) => ({
    nombre: c.Name, servicio: c.Service, estado: c.State,
    puertos: (c.Publishers || []).filter((p) => p.PublishedPort).map((p) => `${p.PublishedPort}->${p.TargetPort}`),
  }));
}
const filtrar = (texto, patron) => texto.split('\n').filter((l) => l.includes(patron)).join('\n');

// ---------- HTTP ----------
async function crearPedido(body) {
  const t0 = performance.now();
  const r = await fetch(`${API}/orders`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const ms = Math.round(performance.now() - t0);
  let json = null;
  try { json = await r.json(); } catch {}
  return { status: r.status, json, ms };
}
async function health() {
  try { const r = await fetch(`${API}/health`); return await r.json(); } catch { return null; }
}

// Espera a que fn() devuelva un valor "verdadero" (o lanza al vencer el tiempo)
async function esperar(fn, { timeout = 30000, cada = 1000, desc = 'condición' } = {}) {
  const fin = Date.now() + timeout;
  let ultimo;
  while (Date.now() < fin) {
    try { ultimo = await fn(); if (ultimo) return ultimo; } catch (e) { ultimo = e.message; }
    await sleep(cada);
  }
  return null;
}

// Momento para tomar una captura. Con --pausa el ejecutor se detiene hasta que se presione Enter.
const PAUSA = process.argv.includes('--pausa');
async function captura(ev, texto) {
  ev.out(`   📸 CAPTURA: ${texto}`);
  if (!PAUSA) return;
  const rl = require('readline').createInterface({ input: process.stdin, output: process.stdout });
  await new Promise((r) => rl.question('      → Tome la captura y presione Enter para continuar... ', () => { rl.close(); r(); }));
}

module.exports = { ROOT, captura, contenedores, API, sleep, Evidencia, compose, logs, filtrar, crearPedido, health, esperar };
