# Laboratorio Databricks · Módulos 3 y 4 (≈ 90 min)

Laboratorio práctico en **PySpark y SQL** sobre la misma fuente del laboratorio anterior (API de TVMaze), pensado para la **Databricks Free Edition**.
Sigue el orden de los slides *"Databricks Módulos 3 y 4 - Data Science"* y construye **un solo proyecto**:

```
API TVMaze ─► RAW (JSON por día en un Volumen) ─► BRONZE ─► SILVER ─► GOLD
                                   Auto Loader ──┘  CDF + MERGE ┘  CDF + MERGE ┘
                              Unity Catalog gobierna todo: workspace.lab_tvmaze_m34
```

- **Auto Loader por lotes** (`trigger(availableNow=True)`) lleva a `bronze_schedule` solo los archivos nuevos: cada ejecución procesa lo pendiente y se detiene. Lectura y escritura van en dos funciones separadas, y la escritura es un **upsert**: cada lote entra con `MERGE` por `id` (`foreachBatch`), así una corrección actualiza la fila en vez de duplicarla.
- **Change Data Feed** lleva a `silver_episodes` solo los cambios de bronze, y a `gold_network_daily` solo las claves afectadas de silver.
- **Unity Catalog** se recorre al final con lo que el pipeline construyó.

## Cómo está organizado

Un notebook por tema, con la misma estructura del laboratorio anterior: objetivo → pasos numerados → "👀 Qué observar" → checkpoint.

| # | Notebook | Min | Slides | Qué demuestra |
|---|---|---|---|---|
| 00 | `00_Setup` | — | — | Configuración y funciones del pipeline (`run_bronze`, `build_silver`, `build_gold`) |
| 01 | `01_RAW_API_a_Volumen` | 8 | 4, 20 | API → un JSON por día en el Volumen (idempotente) |
| 02 | `02_Bronze_Auto_Loader` | 15 | 20–22 | `cloudFiles`: lectura y escritura separadas, escritura con `foreachBatch` + `MERGE` (upsert), checkpoint, `cloud_files_state`, re-ejecución sin duplicados, una corrección que actualiza, `_rescued_data` |
| 03 | `03_Silver_con_CDF` | 15 | 9–12, 17–19 | `table_changes`, `_change_type`, `MERGE`, `pipeline_state`, un `UPDATE` y un `DELETE` que viajan por el CDF |
| 04 | `04_Gold_con_CDF` | 12 | 19 | Claves afectadas, recálculo parcial, `MERGE`, comprobación contra el cálculo completo |
| 05 | `05_Time_Travel_y_Mantenimiento` | 12 | 6–8, 13–16 | `DESCRIBE HISTORY`, `VERSION AS OF`, `RESTORE`, `OPTIMIZE ZORDER BY`, `VACUUM`, `DeltaTable` |
| 06 | `06_Unity_Catalog` | 14 | 24–32 | Jerarquía, `information_schema`, administrada vs externa, vista, `GRANT` / `SHOW GRANTS` / `REVOKE`, linaje |
| 07 | `07_Pipeline_Completo_y_Cierre` | 10 | 4, 33–34 | El pipeline en cuatro líneas, Job, quiz, mapa de lo aprendido |
| | **Total** | **86** (+ margen) | | |

### Las dos consultas clave

```sql
-- ¿Qué archivos ya leyó Auto Loader?  (notebook 02)
SELECT path, size, discovery_time, commit_time
FROM cloud_files_state('/Volumes/workspace/lab_tvmaze_m34/datos/autoloader/checkpoint');

-- ¿Qué cambió en una tabla Delta?  (notebooks 03 y 04)
SELECT _commit_version, _change_type, count(*) AS filas
FROM table_changes('silver_episodes', 0)
GROUP BY 1, 2 ORDER BY 1, 2;
```

## Cómo importarlo

1. En Databricks: **Workspace → (tu carpeta) → ⋮ → Import** y sube los 8 archivos `.ipynb` **a la misma carpeta**
   (es obligatorio: los notebooks hacen `%run ./00_Setup`).
2. Abre `01_RAW_API_a_Volumen`, conecta el cómputo **Serverless** y ejecuta de arriba hacia abajo.
3. Sigue el orden 01 → 07. Cada notebook indica qué notebooks necesita; si pierdes la sesión, vuelve a correr su primera celda (`%run ./00_Setup`).

Todo se crea en `<catálogo>.lab_tvmaze_m34` (catálogo `workspace` en la Free Edition). No toca `lab_tvmaze` ni los esquemas de la práctica `01_ELT`.
Para empezar de cero: `reset_pipeline()` (borra tablas y la memoria de Auto Loader, conserva RAW) o la última celda del notebook 07 (borra todo).

## Diferencias con los slides (Free Edition)

| Slides (Azure) | Free Edition | Cómo se cubre |
|---|---|---|
| Access connector, storage credential, external location | No hay almacenamiento propio | Explicado en el notebook 06; todo es administrado |
| Catálogos por entorno (`prod`, `dev`) | Un catálogo: `workspace` | Nombres de tres niveles con `workspace.lab_tvmaze_m34` |
| Grupos (`analistas`, `ingenieria`) | Grupo `account users` | `GRANT` / `REVOKE` sobre una vista |
| Tablas del sistema (`system.access`) | Según la cuenta | Consulta con `try`; el linaje se ve en Catalog Explorer |
| Streams continuos | Solo `trigger(availableNow=True)` | Auto Loader corre como lote: procesa lo pendiente y se detiene |

Límites a tener presentes: cuota diaria de cómputo (los datos son pequeños a propósito) y salida a internet restringida (la API de TVMaze funcionó al preparar el laboratorio).

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| `%run` no encuentra `./00_Setup` | Notebooks en carpetas distintas | Súbelos todos a la misma carpeta |
| Descarga con 0 archivos | Sin internet o API caída | Copia los JSON del laboratorio anterior (`/Volumes/workspace/lab_tvmaze/datos/raw/tvmaze/AAAA/MM/DD/tvmaze.json`) a `raw/schedule/schedule_AAAA-MM-DD.json` |
| `Table or view not found` | Falta correr un notebook previo | Revisa "Requiere" al inicio del notebook |
| `cloud_files_state` no encuentra el checkpoint | Auto Loader aún no corrió | Ejecuta el notebook 02 |
| `build_silver()` dice "al día" pero silver no existe | Se borró silver sin reiniciar `pipeline_state` | `reset_pipeline()` y vuelve a correr 02 → 04 |
| La versión 0 ya no se puede leer | Es el efecto esperado de `VACUUM` con retención 0 | Solo pasa sobre la copia `silver_demo` |
