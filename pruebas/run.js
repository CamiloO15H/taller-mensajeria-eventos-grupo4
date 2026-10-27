#!/usr/bin/env node
// Ejecutor de los casos de prueba del taller
// Uso:
//   node pruebas/run.js rabbitmq all        → los 10 casos de RabbitMQ
//   node pruebas/run.js kafka all           → los 10 casos de Kafka
//   node pruebas/run.js rabbitmq CP-09      → un solo caso
//   node pruebas/run.js kafka CP-04 CP-05   → varios casos
//   Agregar --pausa para que se detenga en cada momento de captura
const path = require('path');
const fs = require('fs');
const { ROOT } = require('./lib');

const [arq, ...pedidos] = process.argv.slice(2).filter((a) => a !== '--pausa');
if (!['rabbitmq', 'kafka'].includes(arq) || pedidos.length === 0) {
  console.log('Uso: node pruebas/run.js <rabbitmq|kafka> <all|CP-01 CP-02 ...>');
  process.exit(1);
}
const casos = require(`./${arq}`);
const ids = pedidos.includes('all') ? Object.keys(casos) : pedidos.map((p) => p.toUpperCase());
const invalidos = ids.filter((id) => !casos[id]);
if (invalidos.length) {
  console.log(`Casos que no aplican a ${arq}: ${invalidos.join(', ')}. Disponibles: ${Object.keys(casos).join(', ')}`);
  process.exit(1);
}

(async () => {
  const resultados = [];
  for (const id of ids) {
    try {
      resultados.push(await casos[id]());
    } catch (e) {
      console.error(`\n${id} terminó con error: ${e.message}`);
      resultados.push({ id, ok: false, error: e.message });
    }
    console.log('');
  }
  const resumen = [
    `RESUMEN ${arq.toUpperCase()} — ${new Date().toISOString()}`,
    ...resultados.map((r) => `${r.id}: ${r.ok ? 'CUMPLE' : 'NO CUMPLE'}${r.error ? ' (error: ' + r.error + ')' : ''}`),
    `Total: ${resultados.filter((r) => r.ok).length}/${resultados.length} cumplen`,
  ].join('\n');
  console.log(resumen);
  fs.mkdirSync(path.join(ROOT, 'evidencias', arq), { recursive: true });
  fs.writeFileSync(path.join(ROOT, 'evidencias', arq, '_RESUMEN.txt'), resumen + '\n', 'utf8');
  process.exit(resultados.every((r) => r.ok) ? 0 : 1);
})();
