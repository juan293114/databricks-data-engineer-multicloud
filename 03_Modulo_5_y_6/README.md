# Laboratorio Databricks · Módulos 5 y 6, con Unity Catalog a fondo (≈ 120 min)

Laboratorio práctico en **SQL y PySpark** sobre la misma fuente de los laboratorios anteriores (API de TVMaze), pensado para la **Databricks Free Edition**.
Sigue los slides 17 a 24 de la presentación *"Introducción al curso: del lakehouse al despliegue en producción"* y construye **un solo proyecto**:

```
        Módulo 4 · Unity Catalog gobierna todo: workspace.lab_tvmaze_m56
API TVMaze ─► RAW (JSON por día en un Volumen) ─► BRONZE ─► SILVER ─► GOLD ─► SQL warehouse ─► analistas
        Módulo 5 · a mano → Job (Workflows) → pipeline declarativo          Módulo 6 · Databricks SQL
```

- **Módulo 4 · Unity Catalog** (se retoma del laboratorio anterior y se profundiza): la jerarquía (metastore → catálogo → esquema → tabla, vista, volumen),
  los permisos y su herencia, máscaras y filtros para datos sensibles, almacenamiento, auditoría y linaje.
- **Módulo 5 · Data Pipelines:** el flujo bronze → silver → gold construido de tres formas: con funciones (ETL a mano),
  con un **Job** de cuatro tareas (Workflows) y con un **pipeline declarativo** en SQL con expectativas de calidad (Delta Live Tables).
- **Módulo 6 · Databricks SQL:** el **SQL warehouse** como cómputo de los analistas, el editor SQL, un dashboard y una alerta,
  y la gestión de datos en SQL: crear y modificar tablas, dar permisos con `GRANT` y mantener el rendimiento.
- Al final se comprueba que el Job y el pipeline declarativo producen **la misma tabla gold**, y que el notebook y el warehouse leen **el mismo dato**.

## Cómo está organizado

Un notebook por tema, con la misma estructura de los laboratorios anteriores: objetivo → pasos numerados → "👀 Qué observar" → checkpoint.

| # | Notebook | Módulo | Min | Slides (Intro) | Qué demuestra |
|---|---|---|---|---|---|
| 00 | `00_Setup` | — | — | — | Configuración, funciones del ETL y ayudantes para Jobs, pipelines y el SQL warehouse |
| 01 | `01_UC_Jerarquia_y_Objetos` | 4 | 12 | 17–18 | `current_metastore()`, `CREATE CATALOG` / `SCHEMA` / `TABLE` / `VIEW`, volumen, nombre de tres niveles, comentarios, etiquetas, `information_schema` |
| 02 | `02_UC_Permisos` | 4 | 12 | 17–18 | `GRANT`, `SHOW GRANTS`, `REVOKE`, herencia (`inherited_from`), máscara de columna, filtro de filas |
| 03 | `03_UC_Almacenamiento_Auditoria_Linaje` | 4 | 10 | 17, 19 | Tablas administradas, *storage credential* y *external location*, `DESCRIBE HISTORY`, tablas del sistema, linaje |
| 04 | `04_ETL_Medallion` | 5 | 12 | 20 | Extracción, transformación y carga; bronze, silver y gold; una validación que lanza un error |
| 05 | `05_Workflows` | 5 | 18 | 21 | Job de 4 tareas: dependencias, parámetros, reintentos, horario, alerta; una tarea fallida detiene a las siguientes |
| 06 | `06_Pipeline_Declarativo` | 5 | 18 | 22 | `STREAMING TABLE`, `MATERIALIZED VIEW`, expectativas (`EXPECT`, `DROP ROW`), carga incremental, un archivo con errores |
| 07 | `07_SQL_Warehouse` | 6 | 12 | 23 | El SQL warehouse, una consulta enviada como lo hace una herramienta de BI, el mismo dato en los dos cómputos, editor SQL, dashboard y alerta |
| 08 | `08_SQL_Consultas_y_Gestion_de_Datos` | 6 | 15 | 24 | **Notebook SQL que corre en el warehouse:** consultas, `CREATE TABLE` / `VIEW`, `ALTER TABLE`, `UPDATE`, `GRANT`, `OPTIMIZE`, `ANALYZE` |
| 09 | `09_Todo_Conectado_y_Cierre` | 4, 5, 6 | 10 | 27, 29 | Job y pipeline dan el mismo gold, linaje de punta a punta, quiz, mapa de lo aprendido |
| | **Total** | | **119** | | |

Además hay dos carpetas que usan los notebooks 05 y 06:

| Carpeta | Contenido | Quién la usa |
|---|---|---|
| `tareas/` | `1_ingesta`, `2_transformacion`, `3_validacion`, `4_publicacion`: un notebook pequeño por tarea | El Job del notebook 05 |
| `pipeline/` | `tvmaze_pipeline.sql`: las tres tablas declaradas en SQL | El pipeline del notebook 06 |

### Las tres piezas clave

```python
# Un Job: tareas con dependencias  (notebook 05)
{"task_key": "validacion",
 "depends_on": [{"task_key": "transformacion"}],
 "notebook_task": {"notebook_path": ".../tareas/3_validacion"}}
```

```sql
-- Un pipeline declarativo: tablas con reglas de calidad  (notebook 06)
CREATE OR REFRESH STREAMING TABLE dlt_silver_episodes (
  CONSTRAINT episodio_con_id EXPECT (episode_id IS NOT NULL) ON VIOLATION DROP ROW
) AS SELECT ... FROM STREAM(dlt_bronze_schedule);

-- Databricks SQL: consultar, dar permisos y mantener, todo en el warehouse  (notebook 08)
SELECT network_name, SUM(episodes) AS total FROM gold_network_daily GROUP BY network_name ORDER BY total DESC;
GRANT SELECT ON TABLE gold_network_daily TO `account users`;
OPTIMIZE gold_network_daily;
```

## Cómo importarlo

La carpeta debe conservar su estructura (los notebooks buscan `./00_Setup`, `tareas/` y `pipeline/`).

1. En Databricks: **Workspace → Create → Git folder** y pega la URL de este repositorio.
   (Otra opción: descarga el repositorio como `.zip` y súbelo con **Workspace → ⋮ → Import**.)
2. Abre `03_Modulo_5_y_6/01_UC_Jerarquia_y_Objetos`, conecta el cómputo **Serverless** y ejecuta de arriba hacia abajo.
3. Sigue el orden 01 → 09. Cada notebook indica qué notebooks necesita; si pierdes la sesión, vuelve a correr su primera celda (`%run ./00_Setup`).
4. El notebook **08 es distinto**: es SQL puro y se conecta al **SQL warehouse** (selector de cómputo → **SQL warehouses → Serverless Starter Warehouse**).

Todo se crea en `<catálogo>.lab_tvmaze_m56` (catálogo `workspace` en la Free Edition), más un Job (`lab_tvmaze_m56_workflow`)
y un pipeline (`lab_tvmaze_m56_pipeline`). No toca los esquemas de los laboratorios anteriores.
Todos los notebooks se pueden repetir. Para borrar todo: última celda del notebook 09.

## Diferencias con los slides (Free Edition)

| Slides (Azure) | Free Edition | Cómo se cubre |
|---|---|---|
| Activar Unity Catalog y crear el metastore | Ya vienen listos | `current_metastore()` en el notebook 01 |
| Access connector, storage credential, external location | No hay almacenamiento propio | Listas vacías, el error al usar una ruta sin registrar y el SQL de Azure explicado (notebook 03) |
| Catálogos por entorno (`prod`, `dev`) | Se pueden crear catálogos | `CREATE CATALOG lab_prod` de práctica; el laboratorio vive en `workspace` |
| Grupos (`analistas`, `finanzas`) | Grupo `account users` | `GRANT` / `REVOKE`; la máscara y el filtro usan grupos que no existen para que veas el efecto |
| Clusters para las tareas del Job | Solo cómputo serverless | El Job no define cluster |
| Delta Live Tables | Aparece como *Lakeflow Spark Declarative Pipelines* | Menú **Jobs & Pipelines** |
| SQL warehouses de varios tamaños | Un solo warehouse, tamaño 2X-Small | *Serverless Starter Warehouse* (notebooks 07 y 08) |
| Tablas del sistema (auditoría, linaje) | Disponibles, con retraso (hasta unos 20 minutos) | El linaje se ve al instante en Catalog Explorer; con SQL aparece más tarde |

Límites a tener presentes: cuota diaria de cómputo (los datos son pequeños a propósito), máximo 5 tareas de Job a la vez,
un pipeline activo por tipo de pipeline y salida a internet restringida (la API de TVMaze funcionó al preparar el laboratorio).

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| `%run` no encuentra `./00_Setup` | La carpeta perdió su estructura | Importa la carpeta completa (Git folder o `.zip`) |
| El Job falla con *notebook not found* | Falta la carpeta `tareas/` junto a los notebooks | Igual que el anterior |
| Descarga con 0 archivos | Sin internet o API caída | Vuelve a ejecutar; el Job reintenta la ingesta una vez |
| `Table or view not found` | Falta correr un notebook previo | Revisa "Requiere" al inicio del notebook |
| El email sale como `***` o faltan suscriptores | La máscara o el filtro del notebook 02 siguen puestos | Ejecuta el paso 7 del notebook 02 |
| El pipeline tarda en arrancar | El cómputo serverless se está encendiendo | Espera: la primera ejecución toma unos minutos |
| En el notebook 08, `version_del_warehouse` sale vacía | El notebook está conectado al cómputo *Serverless* | Cambia el cómputo a **SQL warehouses → Serverless Starter Warehouse** |
| El linaje o la auditoría con SQL no muestran lo de hoy | Las tablas del sistema llegan con retraso (hasta unos 20 minutos) | Vuelve a ejecutar la celda más tarde, o mira **Catalog → tabla → Lineage** |
