USE env_flood_serverless;
GO
 

CREATE EXTERNAL TABLE silver.stations_parquet
WITH (
    LOCATION = 'https://hassanstenvdatalakedev.blob.core.windows.net/datalake/silver/flood_ea/stations/',     
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
