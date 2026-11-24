# Pipeline Medallion · Usos Mifare (Trafpay API) en Databricks

Pipeline incremental de ingeniería de datos en **Databricks** bajo arquitectura **Medallion (RAW → Bronze → Silver → Gold)** para la ingesta, estandarización y agregación de transacciones desde la API y catálogos maestros en **Excel**. Construido sobre **Unity Catalog**, **Delta Lake**, **Auto Loader**, **Change Data Feed (CDF)**, **Liquid Clustering** y **Serverless Compute**.

---

## Arquitectura General

```text
(GetTransacionByID)
        │
        ▼
[Etapa 1 · RAW]      Ingesta_RAW.py       ──▶ /Volumes/<catalog>/trx_mifare/raw_trx_mifare/uses/AAAA/MM/DD/*.json
        │
        ▼  (Task Value: registros_descargados > 0)
[Etapa 2 · Bronze]   Ingesta_BRONCE.py    ──▶ <catalog>.trx_mifare.bronze_uses (Auto Loader + CDF)
        │
        ▼  (Delta CDF + MERGE INTO)
[Etapa 3 · Silver]   Ingesta_SILVER.py    ──▶ <catalog>.trx_mifare.silver_uses (Liquid Clustering + CDF)
        │
        ▼  (Delta CDF + DELETE/INSERT por ventana)
[Etapa 4 · Gold]     Ingesta_GOLD.py      ──▶ <catalog>.trx_mifare.gold_uses_tx + Catálogos Gold

Archivos Excel (.xlsx) ──▶ [Catálogos] Ingesta_CATALOGOS.py ──▶ Bronze / Silver (lines, applications, providers)
```

---

## Estructura del Repositorio

| Archivo | Propósito principal |
| :--- | :--- |
| `000_Setup.py` | Configuración compartida de catálogo, esquema, volumen, rutas, parámetros de API, esquema JSON y funciones auxiliares. |
| `Ingesta_RAW.py` | Extracción incremental desde la API de Trafpay hacia Unity Catalog Volumes y publicación de `taskValues`. |
| `Ingesta_BRONCE.py` | Ingesta incremental de archivos JSON con Auto Loader (`cloudFiles`) hacia `bronze_uses`. |
| `Ingesta_SILVER.py` | Lectura incremental vía CDF, limpieza, normalización de tipos y `MERGE` idempotente en `silver_uses`. |
| `Ingesta_GOLD.py` | Actualización de catálogos Gold, sincronización retroactiva de nombres y agregación incremental en `gold_uses_tx`. |
| `Ingesta_CATALOGOS.py` | Carga de archivos `.xlsx` de catálogos maestros (`applications`, `lines`, `providers`) hacia Bronze y Silver. |

---

## Descripción Detallada por Etapa

### 0. Setup Compartido (`000_Setup.py`)
* **Entorno Unity Catalog:** Define la zona horaria `America/Bogota`, detecta el catálogo activo (`current_catalog()`) y crea automáticamente el esquema `trx_mifare` y el volumen `raw_trx_mifare` si no existen.
* **Rutas y tablas:** Centraliza las rutas de almacenamiento (`/Volumes/<catalog>/trx_mifare/raw_trx_mifare/...`), checkpoints, esquemas y nombres de tablas Delta para transacciones y catálogos en todas las capas.
* **Configuración de API y utilidades:** Declara el endpoint `GetTransacionByID`, `API_PROJECT_ID`, el ID base de respaldo (`API_DEFAULT_TRX_ID`), el esquema explícito `ESQUEMA_USES` (`data.transactions`) y funciones auxiliares como `qualified_name`, `get_max_id`, `read_raw` y `size_mb`.

### 1. Ingesta RAW (`Ingesta_RAW.py`)
* **Consulta incremental:** Obtiene el último ID registrado en `bronze_uses` mediante `get_max_id(BRONZE_USES) + 1` (o usa el ID base si la tabla aún no existe) y consulta la API con una sesión HTTP con reintentos automáticos ante códigos `429`, `500`, `502`, `503` y `504`.
* **Almacenamiento crudo:** Guarda el cuerpo JSON sin modificaciones en `/Volumes/<catalog>/trx_mifare/raw_trx_mifare/uses/AAAA/MM/DD/uses_AAAAMMDD_HHMM.json`.
* **Control de flujo:** Publica la métrica `registros_descargados` mediante `dbutils.jobs.taskValues.set(...)` para que la compuerta condicional del Job determine si debe ejecutar las capas posteriores.

### 2. Capa Bronze (`Ingesta_BRONCE.py`)
* **Auto Loader incremental:** Lee únicamente los archivos nuevos `uses_*.json` desde `RAW_PATH` usando `cloudFiles` (`multiLine = True`) y `trigger(availableNow=True)`.
* **Explotación y auditoría:** Filtra respuestas exitosas (`code == 0`), explota el arreglo `data.transactions` para generar un registro por transacción y añade columnas de trazabilidad (`archivo_origen`, `archivo_modificado_en`, `bronze_ingestado_en`).
* **Change Data Feed:** Persiste los datos en `<catalog>.trx_mifare.bronze_uses` y habilita `delta.enableChangeDataFeed = true`.

### 3. Capa Silver (`Ingesta_SILVER.py`)
* **Consumo incremental (CDF):** Lee los cambios de `bronze_uses` mediante `readChangeFeed = true` y detecta automáticamente si no existe *checkpoint* para ejecutar una carga inicial (*Batch 0*).
* **Calidad y normalización:**
  * Deduplica por `id` dentro de cada micro-lote priorizando la versión de commit más reciente.
  * Parsea fechas (`date` y `fechaCreacion`) de forma tolerante a formatos hacia `TIMESTAMP` y deriva `fecha_trx` (`DATE`).
  * Castea saldos (`purseavalue`, `pursebvalue`) a `DECIMAL(7,2)`, limpia cadenas y alinea los nombres de columna con el modelo de transacciones (`SW_MIFARE_TRX`).
* **Idempotencia y optimización:** Aplica `MERGE INTO` por `trx_id` sobre `<catalog>.trx_mifare.silver_uses`, configurada con **Liquid Clustering (`CLUSTER BY (fecha_trx)`)** y CDF activo.

### 4. Capa Gold (`Ingesta_GOLD.py`)
* **Validación de origen:** Verifica la existencia de `silver_uses` antes de consultar metadatos o iniciar el procesamiento.
* **Catálogos y sincronización retroactiva:** Actualiza las tablas `gold_lines`, `gold_applications` y `gold_providers` desde Silver y ejecuta un `MERGE` retroactivo sobre `gold_uses_tx` para propagar cambios de nombres en registros históricos.
* **Agregación incremental blindada:**
  * Consume cambios de `silver_uses` vía **Delta CDF** (con soporte automático de *Batch 0*).
  * Agrupa por ventana temporal (`Fecha_trx`, `ano_trx`, `mes_trx`, `dia_trx`, `hora_trx`) y dimensiones (`linea`, `empresa`, `aplicacion`, `tipo_uso`), calculando `cant_trx` (`COUNT(*)`) y `total_trx` (`SUM(COALESCE(fee, 0) * 10)`).
  * Usa `LEFT JOIN` y `COALESCE` con los catálogos para no perder transacciones sin correspondencia en catálogo.
  * Recalcula y reemplaza únicamente las ventanas `(Fecha_trx, hora_trx)` afectadas mediante `DELETE` + `INSERT`, evitando duplicados o filas fantasma, sobre una tabla con **Liquid Clustering (`CLUSTER BY (Fecha_trx, hora_trx)`)**.

### 5. Catálogos Maestros (`Ingesta_CATALOGOS.py`)
* **Origen Excel:** Instala `openpyxl` y toma el archivo `.xlsx` más reciente de cada catálogo (`aplications_*.xlsx`, `line_*.xlsx`, `providers_*.xlsx`) en `/Volumes/<catalog>/trx_mifare/raw_trx_mifare/catalogos/`.
* **Capas Bronze y Silver:** Normaliza encabezados a `snake_case`, guarda los datos crudos