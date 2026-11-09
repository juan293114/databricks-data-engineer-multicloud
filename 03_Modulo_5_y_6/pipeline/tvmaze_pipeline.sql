-- Pipeline declarativo del laboratorio (módulo 5): la programación de TVMaze en tres capas.
-- Cada sentencia DECLARA una tabla. Databricks decide el orden y procesa solo lo nuevo.

-- BRONZE · el dato crudo, tal como llega.
-- STREAM read_files(...) es Auto Loader: en cada ejecución lee solo los archivos nuevos de RAW.
CREATE OR REFRESH STREAMING TABLE dlt_bronze_schedule
COMMENT 'Bronze: episodios tal como llegan de la API TVMaze'
AS SELECT
  *,
  _metadata.file_name AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '${raw_path}',
  format => 'json',
  multiLine => true,
  schema => 'id BIGINT, name STRING, season INT, number INT, airdate STRING, runtime INT,
             rating STRUCT<average: DOUBLE>,
             _embedded STRUCT<show: STRUCT<id: BIGINT, name: STRING, language: STRING,
                                           network: STRUCT<name: STRING>,
                                           webChannel: STRUCT<name: STRING>>>'
);

-- SILVER · limpio, validado y conformado.
-- Las expectativas son las reglas de calidad: dos descartan la fila y una solo avisa.
CREATE OR REFRESH STREAMING TABLE dlt_silver_episodes (
  CONSTRAINT episodio_con_id   EXPECT (episode_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT fecha_valida      EXPECT (airdate IS NOT NULL)    ON VIOLATION DROP ROW,
  CONSTRAINT duracion_positiva EXPECT (runtime_min > 0)
)
COMMENT 'Silver: una fila por episodio, limpia y validada'
AS SELECT
  id                         AS episode_id,
  name                       AS episode_name,
  season,
  number                     AS episode_number,
  try_cast(airdate AS DATE)  AS airdate,
  runtime                    AS runtime_min,
  rating.average             AS episode_rating,
  _embedded.show.id          AS show_id,
  _embedded.show.name        AS show_name,
  _embedded.show.language    AS show_language,
  coalesce(_embedded.show.network.name, _embedded.show.webChannel.name, '(sin red)') AS network_name,
  source_file
FROM STREAM(dlt_bronze_schedule);

-- GOLD · listo para el negocio: episodios por red y día.
CREATE OR REFRESH MATERIALIZED VIEW dlt_gold_network_daily
COMMENT 'Gold: episodios por red y día'
AS SELECT
  airdate,
  network_name,
  count(*)                      AS episodes,
  count(DISTINCT show_id)       AS shows,
  round(avg(episode_rating), 2) AS avg_rating,
  sum(runtime_min)              AS total_minutes
FROM dlt_silver_episodes
GROUP BY airdate, network_name;
