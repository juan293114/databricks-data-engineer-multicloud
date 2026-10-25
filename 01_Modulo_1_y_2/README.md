# Laboratorio Databricks · Módulos 1 y 2 (100 min)

Laboratorio práctico en **PySpark** sobre la fuente de la práctica `01_ELT` (API de TVMaze), pensado para la **Databricks Free Edition**.
Sigue el orden de los slides *"Databricks Módulos 1 y 2 - Data Science (60 min)"*.

## Cómo está organizado

Un notebook por tema, todos con la misma estructura: objetivo → pasos numerados → "👀 Qué observar" → checkpoint.

| # | Notebook | Min | Slides | Qué demuestra |
|---|---|---|---|---|
| 00 | `00_Setup` | — | 18 | Configuración común (se llama con `%run`) |
| 01 | `01_Plataforma_y_Notebooks` | 10 | 5–18 | Unity Catalog, magic commands, cómputo, particiones, estado, widgets, secretos |
| 02 | `02_Ingesta_RAW` | 8 | 3, 6, 27 | API → archivos JSON en un Volumen, carga idempotente |
| 03 | `03_DataFrames` | 16 | 20–23 | Distribuido, inmutable, lazy, `explain()`, shuffle, inferir vs esquema explícito |
| 04 | `04_Formatos_CSV_Parquet_Delta` | 23 | 24–26 + mantenimiento Delta | Tamaño, tipos, lectura columnar, `_delta_log`, ACID, time travel, `OPTIMIZE`, `ZORDER BY`, `VACUUM` |
| 05 | `05_Bronze_multiples_archivos` | 10 | 27–28 | Comodines, `_metadata.file_path`, `append` a Delta |
| 06 | `06_Problemas_de_ingesta` | 11 | 29 | Modos de lectura, `mergeSchema`, `MERGE` / carga idempotente |
| 07 | `07_Silver_y_Gold` | 9 | 28 | Medallón con PySpark, reconstrucción desde Bronze |
| 08 | `08_Lakehouse_y_Cierre` | 10 | 3, 6–8, 31 | Tres perfiles/una tabla, gobierno, quiz, retos |
| | **Total** | **97** (+3 de margen) | | |

```
API TVMaze ─► RAW (JSON en Volumen) ─► BRONZE ─► SILVER ─► GOLD
   mód. 2          mód. 2                mód. 5     mód. 7    mód. 7
                       └── formatos (mód. 4) · problemas (mód. 6) · DataFrames (mód. 3)
```

## Cómo importarlo

1. En Databricks: **Workspace → (tu carpeta) → ⋮ → Import** y sube los 9 archivos `.ipynb` **a la misma carpeta**
   (es obligatorio: los módulos hacen `%run ./00_Setup`).
2. Abre `01_Plataforma_y_Notebooks`, conecta el cómputo **Serverless** y ejecuta de arriba hacia abajo.
3. Sigue el orden 01 → 08. Cada módulo indica qué módulos necesita; si pierdes la sesión, solo vuelve a correr su primera celda (`%run ./00_Setup`).

Todo se crea en `<catálogo>.lab_tvmaze` (catálogo `workspace` en la Free Edition). No toca los esquemas `raw`, `bronze`, `silver` ni `gold` de la práctica `01_ELT`.
Para borrar todo: última celda del módulo 08.

## Diferencias con los slides (Free Edition)

| Slides (Azure) | Free Edition | Cómo se cubre |
|---|---|---|
| Crear y configurar clusters | Solo cómputo **serverless** | Tabla comparativa y particiones con `spark_partition_id()` (módulo 1) |
| Autoscaling, auto-terminación, DBU | Los administra Databricks | Explicado, no configurable |
| ADLS Gen2 | Almacenamiento administrado por Databricks | Mismo modelo: Unity Catalog + Volúmenes |
| Secret scopes / Key Vault | Sin interfaz para crearlos | Patrón explicado + `listScopes()` |
| Git folders | Disponible, no se usa aquí | Solo mencionado |

Límites a tener presentes: cuota diaria de cómputo (los datos son pequeños a propósito), sin `cache()`, `sparkContext` ni RDD en serverless, y salida a internet restringida (la API de TVMaze funcionó al preparar el laboratorio).

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| `%run` no encuentra `./00_Setup` | Notebooks en carpetas distintas | Súbelos todos a la misma carpeta |
| Descarga con 0 archivos | Sin internet o API caída | Usa los archivos de `01_ELT` (ver módulo 2) |
| `Table or view not found` | Falta correr un módulo previo | Revisa "Requiere" al inicio del módulo |
| Resultados distintos a los comentados | Cambiaste `dias` o `start_date` | Es normal: los conteos dependen del rango descargado |
