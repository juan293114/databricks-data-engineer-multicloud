# databricks-data-engineer-multicloud
Repositorio para guardar los laboratorios del curso databricks-data-engineer-multicloud

| Carpeta | Módulos del curso | Qué construye |
|---|---|---|
| `01_Modulo_1_y_2` | Fundamentos de Azure Databricks e ingesta de datos | API TVMaze → RAW → Bronze → Silver → Gold con PySpark |
| `02_Modulo_3_y_4` | Delta Lake y Unity Catalog | El mismo flujo con Auto Loader (bronze), Change Data Feed (silver y gold) y gobierno con Unity Catalog |
| `03_Modulo_5_y_6` | Data Pipelines y Databricks SQL (con Unity Catalog a fondo) | Gobierno (jerarquía, permisos, máscaras, linaje), el mismo flujo como Job de cuatro tareas (Workflows) y como pipeline declarativo con expectativas (Delta Live Tables), y su consumo desde el SQL warehouse (consultas, gestión de datos, dashboard) |

Los tres laboratorios corren en la **Databricks Free Edition** (cómputo serverless, catálogo `workspace`).
