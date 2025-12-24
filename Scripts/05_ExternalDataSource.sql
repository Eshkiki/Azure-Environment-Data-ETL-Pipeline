

CREATE EXTERNAL DATA SOURCE datalake
WITH (
    LOCATION = 'https://hassanstenvdatalakedev.blob.core.windows.net/datalake/silver/'
);


SELECT name, location
FROM sys.external_data_sources;

CREATE EXTERNAL FILE FORMAT ff_parquet WITH (FORMAT_TYPE = PARQUET);
