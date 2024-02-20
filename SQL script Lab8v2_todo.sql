/*
The New York City (NYC) Taxi dataset that includes:

 - Pickup and drop-off dates and times.
 - Pick up and drop-off locations.
 - Trip distances.
 - Itemized fares.
 - Rate types.
 - Payment types.
 - Driver-reported passenger counts.*/


/*
 * * * * * * * * * * * * * * * *
 * Automatic schema inference  *
 * * * * * * * * * * * * * * * *

Since data is stored in the Parquet file format, automatic schema inference is available.
You can easily query the data without listing the data types of all columns in the files.
You also can use the virtual column mechanism and the filepath function to filter out a certain subset of files.

Let's first get familiar with the NYC Taxi data by running the following query: the sum of rides per year.
*/

