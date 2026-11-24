# Pipeline Medallion · Usos Mifare (Trafpay API) en Databricks

Pipeline incremental en **Databricks** bajo arquitectura **Medallion (RAW → Bronze → Silver → Gold)** sobre **Unity Catalog** y **Delta Lake**, diseñado para ejecutarse cada 30 minutos con **Serverless Compute**.

---

## Arquitectura del Pipeline

```text
API (GetTransacionByID)
        │
        ▼
[01] Ingesta_RAW.py ──▶ /Volumes/<catalog>/trx_mifare/raw_trx_mifare/uses/AAAA/MM/DD/*.json
        │
        ▼  (Task Value: registros_descargados > 0)
[02] Ingesta_BRONCE.py (Auto Loader + availableNow) ──▶ <catalog>.trx_mifare.bronze_uses
        │
        ▼  (Delta Change Data Feed + MERGE INTO)
[03] Ingesta_SILVER.py ──▶ <catalog>.trx_mifare.silver_uses
        │
        ▼  (Delta CDF + Liquid Clustering + DELETE/INSERT por ventana)
[04] Ingesta_GOLD.py ──▶ <catalog>.trx_mifare.gold_uses_tx + Catálogos Gold

Catálogos Excel (.xlsx) ──▶ Ingesta_CATALOGOS.py ──▶ Bronze / Silver / Gold (lines, applications, providers)