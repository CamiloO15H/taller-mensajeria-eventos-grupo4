# Reparto del trabajo — Grupo 4

Entrega: **domingo 1 de noviembre de 2026, 11:59 p. m.**, por Tarea en Teams (PDF + enlace al repositorio o ZIP).

| Integrante | Casos a ejecutar y documentar | Sección del informe |
|---|---|---|
| Camilo Ospina Hernández | Montaje de ambos entornos (capturas de contenedores, puertos, consolas y logs de arranque) + Kafka CP-07 a CP-12 | 4 y 6.2 · consolida el PDF final |
| César Augusto Barrientos | RabbitMQ CP-01, CP-02, CP-03, CP-06 | 5.1 |
| Valeria García Ocampo | RabbitMQ CP-07 a CP-12 | 5.2 y 8 (conclusiones) |
| Maria Paulina Vargas Lenis | Kafka CP-01, CP-04, CP-05, CP-06 | 6.1 y 7 (resumen) |

Todos levantan las dos arquitecturas en su equipo (o en Codespaces) y corren la suite completa al menos una vez: el docente evalúa el informe, pero cada uno debe entender lo que entrega.

## Cómo documentar un caso

1. Ejecutar el caso con pausa: `node pruebas/run.js rabbitmq CP-09 --pausa`.
2. En cada `📸 CAPTURA:` tomar la captura que indica (consola, Kafka UI o terminal) y presionar Enter.
3. En el informe, en la tabla del caso:
   - **Resultado obtenido:** copiar los datos clave de `evidencias/<arquitectura>/CP-XX.txt` (códigos HTTP, tiempos, contadores, líneas de log).
   - **Estado:** Cumple / No cumple, según el veredicto del archivo.
   - **Análisis técnico:** revisar el borrador y ajustarlo a lo observado.
4. Pegar las capturas en los recuadros amarillos y borrar el texto de marcador.

## Fechas

| Día | Qué |
|---|---|
| Lunes 26 – martes 27 | Cada uno levanta los dos entornos y corre las suites |
| Miércoles 28 – jueves 29 | Ejecución con `--pausa`, capturas y llenado de su sección |
| Viernes 30 | Camilo consolida; conclusiones (Valeria) y resumen (Paulina) |
| Sábado 31 | Revisión de todos, exportar a PDF |
| Domingo 1 | Entrega en Teams antes de las 11:59 p. m. |
