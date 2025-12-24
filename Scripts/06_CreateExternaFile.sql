

CREATE EXTERNAL TABLE silver.stations_parquet
WITH (
    LOCATION = 'silver/flood_ea/stations_v2/',
    DATA_SOURCE = datalake,
    FILE_FORMAT = ff_parquet
)
AS

SELECT
    station_reference,
    station_label,
    lat,
    lon,
    river_name,
    catchment_name,
    town
FROM silver.vw_stations;
