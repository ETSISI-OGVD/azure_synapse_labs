/*
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* EXPLORE AND ANALYZE DATA LAKES WITH SERVERLESS SQL POOL *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

This tutorial uses 
- the New York City (NYC) Taxi data
- the Public Holidays dataset
- the Weather Data dataset


The NYC Taxi dataset includes:

 - Pickup and drop-off dates and times.
 - Pick up and drop-off locations.
 - Trip distances.
 - Itemized fares.
 - Rate types.
 - Payment types.
 - Driver-reported passenger counts.
 
 */


/*
Automatic schema inference 
 
Since data is stored in the Parquet file format, automatic schema inference is available.
You can easily query the data without listing the data types of all columns in the files.
You also can use the virtual column mechanism and the filepath function to filter out a certain subset of files.

Let's first get familiar with the NYC Taxi data by running the following query. 

The OPENROWSET(BULK...) function allows you to access files in Azure Storage. 
[OPENROWSET](develop-openrowset.md) reads content of a remote data source, such as a file, and returns the content as a set of rows.
*/

/*QUERIES 0-2*/

SELECT TOP 100 * FROM
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/puYear=*/puMonth=*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc]


SELECT TOP 100 * FROM
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/holidaydatacontainer/Processed/*.parquet',
        FORMAT='PARQUET'
    ) AS [holidays]


SELECT
    TOP 100 *
FROM  
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/isdweatherdatacontainer/ISDWeather/year=*/month=*/*.parquet',
        FORMAT='PARQUET'
    ) AS [weather]

/*
QUERY3 (Time series): summarize the yearly number of taxi rides by using the following query.
In the Chart view, plot the Column chart with the Category column set to current_year.
*/

SELECT
    YEAR(tpepPickupDateTime) AS current_year,
    COUNT(*) AS rides_per_year
FROM
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/puYear=*/puMonth=*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc]
WHERE nyc.filepath(1) >= '2009' AND nyc.filepath(1) <= '2019'
GROUP BY YEAR(tpepPickupDateTime)
ORDER BY 1 ASC

/*
QUERY4 (Seasonality): query returns the daily number of rides during that year.
In the Chart view, visualize data by plotting the Column chart with the Category column set to current_day and the Legend (series) column set to rides_per_day
*/

SELECT
    CAST([tpepPickupDateTime] AS DATE) AS [current_day],
    COUNT(*) as rides_per_day
FROM
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/puYear=*/puMonth=*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc]
WHERE nyc.filepath(1) = '2016'
GROUP BY CAST([tpepPickupDateTime] AS DATE)
ORDER BY 1 ASC;


/*
From the plot chart, you can see there's a weekly pattern, with Saturdays as the peak day. During Summer months, there are fewer taxi rides because of vacations. Also, notice some significant drops in the number of taxi rides without a clear pattern of when and why they occur.

QUERY5 (Outlier analysis): analyze if the drop in rides correlates with public holidays. Check if there's a correlation by joining the NYC Taxi rides dataset with the Public Holidays dataset:

*/

WITH taxi_rides AS (
SELECT
    CAST([tpepPickupDateTime] AS DATE) AS [current_day],
    COUNT(*) as rides_per_day
FROM
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/puYear=*/puMonth=*/*.parquet',
        FORMAT='PARQUET'
    ) AS [nyc]
WHERE nyc.filepath(1) = '2016'
GROUP BY CAST([tpepPickupDateTime] AS DATE)
),
public_holidays AS (
SELECT
    holidayname as holiday,
    date
FROM
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/holidaydatacontainer/Processed/*.parquet',
        FORMAT='PARQUET'
    ) AS [holidays]
WHERE countryorregion = 'United States' AND YEAR(date) = 2016
),
joined_data AS (
SELECT
    *
FROM taxi_rides t
LEFT OUTER JOIN public_holidays p on t.current_day = p.date
)

SELECT 
    *,
    holiday_rides = 
    CASE   
      WHEN holiday is null THEN 0   
      WHEN holiday is not null THEN rides_per_day
    END   
FROM joined_data
ORDER BY current_day ASC

/*
Highlight the number of taxi rides during public holidays. For that purpose, choose current_day for the Category column and rides_per_day and 
holiday_rides as the Legend (series) columns.
*/

/*
QUERY6 (Outlier analysis): From the plot chart, you can see that during public holidays the number of taxi rides is lower. There's still one unexplained large drop on January 23. 
Let's check the weather in NYC on that day by querying the Weather Data dataset:
*/

SELECT
    AVG(windspeed) AS avg_windspeed,
    MIN(windspeed) AS min_windspeed,
    MAX(windspeed) AS max_windspeed,
    AVG(temperature) AS avg_temperature,
    MIN(temperature) AS min_temperature,
    MAX(temperature) AS max_temperature,
    AVG(sealvlpressure) AS avg_sealvlpressure,
    MIN(sealvlpressure) AS min_sealvlpressure,
    MAX(sealvlpressure) AS max_sealvlpressure,
    AVG(precipdepth) AS avg_precipdepth,
    MIN(precipdepth) AS min_precipdepth,
    MAX(precipdepth) AS max_precipdepth,
    AVG(snowdepth) AS avg_snowdepth,
    MIN(snowdepth) AS min_snowdepth,
    MAX(snowdepth) AS max_snowdepth
FROM
    OPENROWSET(
        BULK 'https://azureopendatastorage.blob.core.windows.net/isdweatherdatacontainer/ISDWeather/year=*/month=*/*.parquet',
        FORMAT='PARQUET'
    ) AS [weather]
WHERE countryorregion = 'US' AND CAST([datetime] AS DATE) = '2016-01-23' AND stationname = 'JOHN F KENNEDY INTERNATIONAL AIRPORT'

/*
The results of the query indicate that the drop in the number of taxi rides occurred because:

There was a blizzard on that day in NYC with heavy snow (~30 cm).
It was cold (temperature was below zero degrees Celsius).
It was windy (~10 m/s).
*/