USE env_flood_serverless;
GO
 

IF OBJECT_ID('silver.stations_parquet', 'U') IS NOT NULL
    DROP EXTERNAL TABLE silver.stations_parquet;


CREATE EXTERNAL TABLE silver.stations_parquet
WITH (
    LOCATION = 'flood_ea/stations/',     
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
GO
