USE env_flood_serverless;
GO

 

CREATE OR ALTER VIEW silver.vw_stations AS
SELECT
    JSON_VALUE(item.value, '$.stationReference') AS station_reference,
    JSON_VALUE(item.value, '$.label')           AS station_label,
    TRY_CAST(JSON_VALUE(item.value, '$.lat')  AS float) AS lat,
    TRY_CAST(JSON_VALUE(item.value, '$.long') AS float) AS lon,
    JSON_VALUE(item.value, '$.riverName')       AS river_name,
    JSON_VALUE(item.value, '$.catchmentName')   AS catchment_name,
    JSON_VALUE(item.value, '$.town')            AS town
FROM OPENROWSET(
        BULK 'raw/flood_ea/ingest_date=*/stations_*.json',
        DATA_SOURCE = 'datalake',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE      = '0x0b',
        ROWTERMINATOR   = '0x0b'
     )
     WITH (json_doc NVARCHAR(MAX)) AS src
CROSS APPLY OPENJSON(src.json_doc, '$.items') AS item;
GO
