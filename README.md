# Taller: Pruebas de integración en arquitecturas basadas en mensajes y eventos

**Curso:** Aseguramiento de la Calidad · ITM 2026-2 · Entrega: 1 de noviembre de 2026, 11:59 p. m. (Teams)
**Integrantes:** Maria Paulina Vargas Lenis · Camilo Ospina Hernández · César Augusto Barrientos · Valeria García Ocampo

Dos arquitecturas reactivas, cada una con su propio `docker-compose.yml`, y un ejecutor que corre los 12 casos de prueba de la guía y deja la evidencia en archivos.

| Carpeta | Qué hay |
|---|---|
| `rabbitmq/` | `orders-api` (productor) → RabbitMQ (cola `pedidos`) → `notification-worker` (consumidor, escalable) |
| `kafka/` | `orders-api` (productor) → Kafka (tópico `orders.events`, 3 particiones) → `inventory-consumer`, `billing-consumer`, `notification-consumer` (cada uno con su consumer group) + Kafka UI |
| `pruebas/` | Ejecutor automático de los casos (`run.js`) |
| `evidencias/` | Aquí queda la salida de cada caso (`CP-XX.txt`) y las capturas |
| `docs/` | Plantilla del informe |
| `.devcontainer/` | Configuración para abrir el proyecto en GitHub Codespaces |

**Regla de las dos arquitecturas:** la API nunca llama a los consumidores. La única comunicación entre productor y consumidores pasa por el broker. Los consumidores no exponen puertos.

---

## 1. Dónde correrlo

**Opción A — En el PC (Docker Desktop)**
- Docker Desktop abierto y Node.js 20 o superior (`node -v`).
- En Windows, PowerShell o Git Bash sirven: todo lo de pruebas está en Node, no hay scripts de Bash obligatorios.
- Kafka con todos sus contenedores necesita unos 4 GB de RAM libres para Docker.

**Opción B — En GitHub Codespaces (sin instalar nada)**
1. Subir esta carpeta a un repositorio de GitHub.
2. *Code → Codespaces → Create codespace*. Elegir una máquina de **4 núcleos** (el `devcontainer.json` ya la pide).
3. Al abrir, el Codespace ya trae Docker y Node, y ejecuta `npm install` solo.
4. Las consolas se abren desde la pestaña **Ports**: 15672 (RabbitMQ) y 8080 (Kafka UI). El ícono del globo abre cada una en el navegador.
5. El Codespace se apaga tras 30 minutos sin uso: los contenedores se detienen y hay que volver a levantarlos.

---

## 2. Arquitectura RabbitMQ

```bash
npm install                      # una sola vez (raíz del proyecto)
cd rabbitmq
docker compose up -d --build
docker compose ps                # rabbitmq (healthy), orders-api, notification-worker
cd ..
```

- Consola: http://localhost:15672 · usuario `admin` · contraseña `admin` → pestaña *Queues* → cola `pedidos`.
- Prueba manual:
  ```bash
  curl -X POST http://localhost:3000/orders -H "Content-Type: application/json" -d "{\"customerName\":\"Pedro\",\"product\":\"Libro de arquitectura\",\"quantity\":1}"
  ```
- Ejecutar los 10 casos que aplican a RabbitMQ (CP-01, 02, 03, 06, 07, 08, 09, 10, 11, 12):
  ```bash
  node pruebas/run.js rabbitmq all
  node pruebas/run.js rabbitmq CP-09      # o un solo caso
  ```
- Al terminar: `cd rabbitmq && docker compose down` (agregar `-v` para borrar también los mensajes guardados).

## 3. Arquitectura Kafka

Las dos arquitecturas usan el puerto 3000: **apague RabbitMQ antes de levantar Kafka.**

```bash
cd kafka
docker compose up -d --build     # la primera vez Kafka tarda 1–2 minutos en quedar "healthy"
docker compose ps                # kafka, kafka-ui, orders-api, inventory/billing/notification-consumer
cd ..
```

- Kafka UI: http://localhost:8080 → *Topics* → `orders.events` (mensajes, particiones) y *Consumers* (grupos y offsets).
- Prueba manual:
  ```bash
  curl -X POST http://localhost:3000/orders -H "Content-Type: application/json" -d "{\"customerName\":\"Pedro\",\"product\":\"Libro de arquitectura\",\"quantity\":1,\"unitPrice\":85000}"
  ```
- Ejecutar los 10 casos que aplican a Kafka (CP-01, 04, 05, 06, 07, 08, 09, 10, 11, 12):
  ```bash
  node pruebas/run.js kafka all
  ```
- Al terminar: `cd kafka && docker compose down`.

---

## 4. Qué hace el ejecutor de pruebas

Cada caso prepara su precondición (detiene o escala consumidores, reinicia el broker), envía los pedidos, verifica el resultado y escribe `evidencias/<arquitectura>/CP-XX.txt` con:
- los pasos y comandos ejecutados,
- las respuestas HTTP y sus tiempos,
- el estado de la cola (Ready / Unacked / Total) o los offsets y el lag de Kafka,
- las líneas de log de la API, del broker y de los consumidores para ese pedido,
- cada verificación con ✔ o ✘ y el veredicto **CUMPLE / NO CUMPLE**.

Al final deja `evidencias/<arquitectura>/_RESUMEN.txt`.

El ejecutor **no reemplaza las capturas**: la guía pide capturas reales de las consolas. Para tomarlas, ejecute con `--pausa`:

```bash
node pruebas/run.js rabbitmq CP-11 --pausa
```

En cada momento clave el ejecutor imprime `📸 CAPTURA: …` (qué capturar y dónde) y se detiene hasta que presione Enter. Así la cola, los offsets o los logs quedan quietos mientras toma la captura.

## 5. Capturas que pide la guía

**Entorno (ambas arquitecturas):** `docker compose ps` con los contenedores en ejecución y sus puertos; los logs de arranque (`docker compose logs orders-api` y de cada consumidor); la consola de RabbitMQ; Kafka UI con el broker activo.

| Caso | Captura en RabbitMQ | Captura en Kafka |
|---|---|---|
| CP-01 | Postman: POST /orders → 202 | Igual |
| CP-02 | *Queues → pedidos*: Ready = 1 y *Get messages* con el JSON | — |
| CP-03 | Log del worker: RECIBIDO / PROCESADO | — |
| CP-04 | — | Kafka UI → *Topics → orders.events → Messages*: el evento OrderCreated |
| CP-05 | — | Logs de los 3 consumidores y *Consumers*: lag 0 en los 3 grupos |
| CP-06 | Logs de API y worker con el mismo orderId/correlationId | Logs de API, mensaje en Kafka UI y logs de los 3 consumidores |
| CP-07 | Salida del ejecutor (tiempos) + cola con mensajes pendientes | Salida del ejecutor (tiempos) |
| CP-08 | `docker compose ps` (el worker sin puertos) + *Connections* en la consola | `docker compose ps` + *Consumers*: 3 grupos distintos |
| CP-09 | Cola con Ready ≥ 1 y 0 consumidores tras el `kill` | *Consumers → billing-service-group*: lag ≥ 3 |
| CP-10 | *Queues → pedidos*: 2 consumidores + logs de las 2 instancias | *Consumers → inventory-service-group*: 2 miembros con particiones |
| CP-11 | Ready = 3 con el worker apagado, luego Ready = 0 | Lag del grupo apagado y log del `audit-consumer` leyendo desde el inicio |
| CP-12 | Ready = 3 antes y después de `docker compose restart rabbitmq` | Mensajes del tópico antes y después de `docker compose restart kafka` |

## 6. Problemas comunes

| Síntoma | Solución |
|---|---|
| `port is already allocated` en el 3000 | La otra arquitectura sigue arriba: `docker compose down` en su carpeta |
| El ejecutor dice "El laboratorio … está arriba: ✘" | Levantar la arquitectura correcta y esperar a que `docker compose ps` muestre el broker *healthy* |
| Kafka se reinicia o queda *unhealthy* | Falta memoria: cerrar programas, o en Codespaces usar la máquina de 4 núcleos |
| La consola de RabbitMQ muestra 0 aunque haya mensajes | Las estadísticas se actualizan cada 5 s: refrescar |
| `docker compose logs` no muestra nada del worker en el PC | Verificar que esté corriendo: `docker compose ps -a` |
