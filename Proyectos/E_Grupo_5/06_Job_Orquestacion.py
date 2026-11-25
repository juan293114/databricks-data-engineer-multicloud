# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Orquestación con un Databricks Job
# MAGIC
# MAGIC ```
# MAGIC  Horario: todos los días 6:00 a. m. (Bogotá)
# MAGIC  ┌──────────────┐   ┌──────────┐   ┌──────────┐   ┌────────┐   ┌─────────────────┐
# MAGIC  │ ingesta_raw  │ → │  bronze  │ → │  silver  │ → │  gold  │ → │  mantenimiento  │
# MAGIC  │ API → Volumen│   │AutoLoader│   │   CDF    │   │estrella│   │OPTIMIZE · VACUUM│
# MAGIC  └──────────────┘   └──────────┘   └──────────┘   └────────┘   └─────────────────┘
# MAGIC ```
# MAGIC Cada tarea es un notebook de la carpeta `tareas` y solo empieza cuando la anterior terminó bien.
# MAGIC Si una falla, las siguientes no corren y llega un correo.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()                       # en un notebook ya viene autenticado
ruta_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
PROYECTO_DIR = os.path.dirname(ruta_notebook)                  # la carpeta de este proyecto en el Workspace
if not PROYECTO_DIR.startswith("/Workspace"):
    PROYECTO_DIR = "/Workspace" + PROYECTO_DIR
print(PROYECTO_DIR)
print(sorted(os.listdir(f"{PROYECTO_DIR}/tareas")))

# COMMAND ----------

def tarea(clave, notebook, depende_de=None, reintentos=0):
    t = {"task_key": clave, "notebook_task": {"notebook_path": f"{PROYECTO_DIR}/tareas/{notebook}"}}
    if depende_de:
        t["depends_on"] = [{"task_key": depende_de}]           # DEPENDENCIA: espera a la tarea anterior
    if reintentos:
        t["max_retries"] = reintentos                           # REINTENTO: la API puede fallar un momento
        t["min_retry_interval_millis"] = 60000
    return t


job = {
    "name": JOB_NAME,
    "parameters": [{"name": "dias_atras", "default": "5"}],    # PARÁMETRO: cuántos días pedir a la API
    "tasks": [
        tarea("ingesta_raw",   "1_ingesta_raw", reintentos=2),
        tarea("bronze",        "2_bronze",        "ingesta_raw"),
        tarea("silver",        "3_silver",        "bronze"),
        tarea("gold",          "4_gold",          "silver"),
        tarea("mantenimiento", "5_mantenimiento", "gold"),
    ],
    "schedule": {                                               # CALENDARIO: todos los días a las 6:00
        "quartz_cron_expression": "0 0 6 * * ?",
        "timezone_id": "America/Bogota",
        "pause_status": "UNPAUSED",
    },
    "email_notifications": {"on_failure": [USER]},              # ALERTA: correo si el Job falla
}

# COMMAND ----------

def find_job(name):
    jobs = w.api_client.do("GET", "/api/2.2/jobs/list", query={"name": name}).get("jobs", [])
    return jobs[0]["job_id"] if jobs else None


def create_job(settings):
    """Crea el Job; si ya existe uno con ese nombre, lo actualiza. Devuelve su id."""
    job_id = find_job(settings["name"])
    if job_id:
        w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": job_id, "new_settings": settings})
    else:
        job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=settings)["job_id"]
    print(f"Job listo: {settings['name']} → {w.config.host}/jobs/{job_id}")
    return job_id


def run_job(job_id, **params):
    """Ejecuta el Job, espera a que termine y muestra el estado de cada tarea."""
    run_id = w.api_client.do("POST", "/api/2.2/jobs/run-now",
                             body={"job_id": job_id, "job_parameters": params})["run_id"]
    print(f"Corrida en marcha: {w.config.host}/jobs/{job_id}/runs/{run_id}")
    while True:
        run = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        if run["state"]["life_cycle_state"] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(15)
    for t in sorted(run["tasks"], key=lambda t: t.get("start_time") or 0):
        print(f"  {t['task_key']:<15} {t['state'].get('result_state', t['state']['life_cycle_state'])}")
    print("Resultado del Job:", run["state"].get("result_state", run["state"]["life_cycle_state"]))

# COMMAND ----------

JOB_ID = create_job(job)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejecutarlo (esto es lo que se muestra en la sustentación)

# COMMAND ----------

run_job(JOB_ID)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pipeline_log ORDER BY registrado_en DESC LIMIT 20
