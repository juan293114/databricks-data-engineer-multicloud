# Proyecto Final - ELT con Arquitectura Medallón y Databricks Jobs

## Flujo
RAW -> BRONZE -> SILVER -> GOLD -> POWER BI

### Raw
Los CSV originales se mantienen en:
`/Volumes/workspace/default/filestore/tables/raw/`

### Bronze
- Auto Loader (`cloudFiles`)
- Delta Tables
- CDF habilitado
- Checkpoints y schemaLocation separados por dataset

Tablas:
- `workspace.bronze.calendar`
- `workspace.bronze.products`
- `workspace.bronze.customers`
- `workspace.bronze.stores`
- `workspace.bronze.sales`

### Silver
- Consume Bronze mediante Change Data Feed
- Eliminación de duplicados
- Conversión/estandarización de tipos
- Validación de llaves no nulas

Tablas:
- `workspace.silver.calendar`
- `workspace.silver.products`
- `workspace.silver.customers`
- `workspace.silver.stores`
- `workspace.silver.sales`

### Gold
Modelo estrella:
- `workspace.gold.fact_sales`
- `workspace.gold.dim_calendar`
- `workspace.gold.dim_customers`
- `workspace.gold.dim_products`
- `workspace.gold.dim_stores`

## Orden de ejecución
1. `A_Raw/Raw`
2. `B_Bronze/Bronze`
3. `C_Silver/Silver`
4. `D_Gold/Gold`
5. Job del databricks

## Databricks Job
Configurar un Schedule (por ejemplo diario a las 07:00). 

## Power BI
Conectar Power BI a `workspace.gold` y construir una página con KPIs y visuales que respondan preguntas de negocio.
Ejemplos:
- Evolución de las ventas.
- Ventas por producto.
- Ventas por tienda.
- Ventas por fecha.


