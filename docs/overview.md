# Tea

Tea is a Greenplum extension that provides read access to Iceberg tables stored in S3 in Parquet format. Currently, Tea is supported only for Greenplum 6.

## Tea Runtime Environment

* Greenplum. Most of the query is executed here. Tea handles only table scans.
* Iceberg Catalog. Tea uses the catalog solely to resolve the Iceberg metadata file in S3 from a table name. Currently, only HMS is supported.
* S3.
* Redis or Valkey. Used by the Samovar protocol for node coordination (Samovar is required when using External Table and optional when using Foreign Table).

## Tea Usage Modes

### Foreign Table

**Note**. When using Foreign Table, the GPORCA optimizer is not supported (a Greenplum limitation).

* the master queries HMS to find where the metadata is stored;
* reads all Iceberg metadata from S3;
* serializes information about all data files and delete files into a single JSON and sends it to all segments;
* each segment parses the JSON containing all metadata and determines which portion of the data it should process.

### Foreign Table + Samovar

Works similarly to Foreign Table, with the following exceptions:
* metadata is distributed from the master to segments not as JSON, but by writing to a Redis queue;
* segments pull metadata from the queue.

This avoids sending all metadata to every segment and enables dynamic load balancing.

### External Table + Samovar

Due to Greenplum limitations, the master does not participate in query planning.
* a segment is selected and takes on the coordinator role;
* it parses metadata and stores it in Redis;
* all other segments periodically check whether metadata has appeared in Redis;

The main advantage is GPORCA support. The main disadvantage is high load on Redis.

### Comparison

* The External Table + Samovar configuration works with GPORCA, which produces better query plans.
* The Foreign Table configuration is convenient when you do not want to maintain an extra dependency such as Redis.
* The Foreign Table + Samovar configuration
    * places less load on Redis than External Table + Samovar.
    * supports dynamic load balancing during reads, unlike Foreign Table without Samovar.
