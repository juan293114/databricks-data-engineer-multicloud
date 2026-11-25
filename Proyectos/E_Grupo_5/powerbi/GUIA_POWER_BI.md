# Reporte de Power BI: el clima de Colombia

## 1. Conectar Power BI a Databricks

1. En Databricks: **SQL Warehouses → Serverless Starter Warehouse → Connection details**. Copiar **Server hostname** y **HTTP path**.
2. En Databricks: avatar → **Settings → Developer → Access tokens → Generate new token**. Copiarlo, porque solo se muestra una vez.
3. En Power BI Desktop: **Obtener datos → Azure Databricks**.
   - Server hostname y HTTP path: los del paso 1.
   - Modo: **Import**.
   - Autenticación: **Personal Access Token**, el del paso 2.
4. En el navegador: `workspace → proyecto_clima` y marcar **dim_ciudad**, **dim_fecha** y **fact_clima_diario**. Luego **Cargar**.

## 2. Modelo (vista Modelo)

Power BI suele crear las relaciones solo. Si no aparecen, crearlas así:

- `dim_ciudad[ciudad_key]` 1 → * `fact_clima_diario[ciudad_key]`
- `dim_fecha[fecha_key]` 1 → * `fact_clima_diario[fecha_key]`

Ordenar `dim_fecha[nombre_mes]` por `dim_fecha[mes]`: seleccionar la columna, **Herramientas de columnas → Ordenar por columna → mes**.

## 3. Medidas DAX (en la tabla fact_clima_diario → Nueva medida)

```DAX
Lluvia total (mm) = SUM ( fact_clima_diario[precipitacion_mm] )

Días medidos = COUNTROWS ( fact_clima_diario )

Días lluviosos = SUM ( fact_clima_diario[dia_lluvioso] )

% días lluviosos = DIVIDE ( [Días lluviosos], [Días medidos] )

Días de calor extremo = SUM ( fact_clima_diario[dia_calor_extremo] )

Temp. media (°C) = AVERAGE ( fact_clima_diario[temp_media_c] )

Temp. máxima récord (°C) = MAX ( fact_clima_diario[temp_max_c] )
```

Formato: `% días lluviosos` como porcentaje y las temperaturas con 1 decimal.

## 4. La página del reporte

| Visual | Campos | Pregunta que responde |
|---|---|---|
| 4 tarjetas | `Lluvia total (mm)`, `Temp. media (°C)`, `Días de calor extremo`, `Temp. máxima récord (°C)` | Resumen general |
| Barras horizontales | Eje: `dim_ciudad[ciudad]` · Valor: `Lluvia total (mm)` (orden descendente) | ¿Cuál es la ciudad más lluviosa? |
| Columnas | Eje: `dim_fecha[nombre_mes]` · Valor: `% días lluviosos` · Filtro del visual: `dim_ciudad[region]` = Caribe | ¿Cuál es el mejor mes para viajar a la costa? |
| Líneas | Eje: `dim_fecha[fecha]` (jerarquía Año → Mes) · Valor: `Temp. media (°C)` · Leyenda: `dim_ciudad[region]` | ¿Cómo cambia la temperatura en cada región? |
| Barras | Eje: `dim_ciudad[ciudad]` · Valor: `Días de calor extremo` | ¿Dónde hace más calor extremo? |
| Segmentadores | `dim_ciudad[region]`, `dim_fecha[anio]`, `dim_fecha[temporada]` | Filtrar todo el reporte |

Título sugerido: **"¿Llueve o hace calor? El clima de 12 ciudades de Colombia"**.

## 5. Actualizar

El Job carga los datos nuevos todos los días a las 6:00 a. m. En Power BI, el botón **Actualizar** los trae.
