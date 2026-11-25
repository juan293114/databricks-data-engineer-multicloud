# Databricks notebook source
# MAGIC %md
# MAGIC # Orquestador - Pipeline ELT
# MAGIC Ejecuta Raw -> Bronze -> Silver -> Gold.

TIMEOUT_SECONDS = 0

NOTEBOOKS = [
    "/Users/yennijimeno@gmail.com/Python-course-for-data-engineering/PROYECTO/A_Raw/Raw",
    "/Users/yennijimeno@gmail.com/Python-course-for-data-engineering/PROYECTO/B_Bronze/Bronze",
    "/Users/yennijimeno@gmail.com/Python-course-for-data-engineering/PROYECTO/C_Silver/Silver",
    "/Users/yennijimeno@gmail.com/Python-course-for-data-engineering/PROYECTO/D_Gold/Gold",
]

try:
    for notebook_path in NOTEBOOKS:
        print(f"Ejecutando: {notebook_path}")
        dbutils.notebook.run(notebook_path, TIMEOUT_SECONDS)
        print(f"OK: {notebook_path}")

    print("Pipeline ELT ejecutado correctamente")

except Exception as e:
    print(f"Error en la ejecución del Pipeline: {e}")
    raise
