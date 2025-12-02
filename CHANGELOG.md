### Tea release 1.76.0, 2025-12-02

### Improvements
* [Samovar] Implement distributed metadata processing for large tables
* [Samovar] Do not block the coordinator (as a writer) during the sync phase

### Tea release 1.75.0, 2025-11-18

### Improvements
* [Teapotless][Samovar] Improve dangling positional delete pruning
* [Samovar] Sleep before processing for large slice numbers
* [Samovar] Make initial synchronization timeouts configurable

### Tea release 1.74.1, 2025-11-10

#### Bug Fix
* Fix reading float4 values in tables with float8 type

### Tea release 1.74.0, 2025-11-05

#### Improvements
* [Samovar] Add jitter
* [Samovar] Static balancing for small tables
* [Samovar] Make initial synchronization configurable

### Tea release 1.73.0, 2025-10-28

#### Improvements
* [Samovar] Handle cancel command when reading metadata
* Make arrow batch size adaptive

### Tea release 1.72.3, 2025-09-29

#### Improvements
* Make arrow batch size configurable

### Tea release 1.72.2, 2025-09-23

#### Improvements
* Set the content type to 'application/gzip' when uploading the artifact

### Tea release 1.72.1, 2025-09-22

### Tea release 1.72.0, 2025-09-18

#### Improvements
* [Teapotless][Samovar] Skip dangling positional delete files

#### Bug Fix
* Fix LIKE function for strings with `\n` character

### Tea release 1.71.2, 2025-09-11

#### Improvements
* Recreate UDF in tea schema

### Tea release 1.71.1, 2025-09-09

#### Improvements
* Fix downgrade/upgrade on gp5

### Tea release 1.71.0, 2025-09-08

#### Improvements
* Implement UDF to get primitive metrics from Iceberg tables

### Tea release 1.70.0, 2025-08-25

#### Improvements
* Improve reading in case filter skips all rows in row group
* [Teapotless][Samovar] Improve metadata reading for queries without filter
* [Teapotless][Samovar] Improve metadata reading for queries with partition filter

#### Bug Fix
* Fix 'prefetch' mode

### Tea release 1.69.0, 2025-08-05

#### Improvements
* Validate profile to tables mapping
* [Samovar] Remove fallback logic when it is redundant
* [Samovar] Improve reading partitioned table with deletes
* [Samovar] Log samovar metrics
* [Samovar] Improve TTL setting for keys

### Tea release 1.68.0, 2025-07-25

#### Improvements
* Log metadata access type
* Return error in case some segments use teapot and some segments use samovar
* Support overriding profile in tea-config.json

### Tea release 1.67.0, 2025-07-10

#### Improvements
* Skip row groups in positional deletes when possible
* Speed up metadata reading for queries without filter
* Support `coalesce` expression in filters

### Tea release 1.66.0, 2025-06-03

#### Improvements
* [Teapot] Improve delete files reading (do not reopen them if possible)
* [Teapotless][Samovar] Support comparison of different temporal types in min/max filter
* Validate schema of config in JSON format

#### Bug Fix
* Fix potential bug if 'prefetch' is enabled

#### Backward Incompatible Change
* Do not support config in ini format anymore

### Tea release 1.65.0, 2025-05-19

#### Improvements
* [Teapotless][Samovar] Improve Iceberg metadata handling (make less copies)

### Tea release 1.64.0, 2025-04-04

#### Improvements
* [Teapotless][Samovar] Apply positional delete files according to referenced_data_file if set
* [Teapotless][Samovar] Apply metadata filter as early as possible
* [Teapot] Improve teapot error messages
* Improve error messages about types incompatibility between greenplum and iceberg

### Tea release 1.63.6, 2025-04-02

#### Bug Fix
* [Teapotless][Samovar] Fix incorrect metadata reading when iceberg manifest file contains at least two entries

### Tea release 1.63.5, 2025-03-27

#### Improvements
* [Samovar] Possibility to choose coordinator from all segments instead of segment 0
* Annotated logs with actual timestamps

### Tea release 1.63.4, 2025-03-25

#### Bug Fix
* [Samovar] Fix `Int32 overflow` when parquet files are larger than 2GB
* [Samovar] Fix `Positional delete stream error`
* [Samovar] Fix `Extra reading from hms and s3`

#### Improvements
* [Samovar] Change default policy in splitting tasks

### Tea release 1.63.3, 2025-03-20

#### Bug Fix
* [Samovar] Fix `Backoff limit exceeded`

### Tea release 1.63.1, 2025-03-14

#### Bug Fix
* [Teapotless] Fix logical type validation for 'day' transform

### Tea release 1.63.0, 2025-03-11

#### Bug Fix
* [Teapot] Fix 'connection attempt timed out before receiving SETTINGS frame'
* [Samovar] Fix exponential backoff
* [Samovar] Abort request if connection was broken while loading task from queue

#### New Feature
* [Teapotless][Samovar] Use partitioning info from iceberg for applying delete files

#### Improvements
* Unify metadata access methods
* [Teapotless][Samovar] Decrease metadata size via encoding filenames
* [Teapotless][Samovar] Decrease metadata memory consumption via discarding unnecessary meta
* [Teapotless][Samovar] Measure iceberg metrics
* [Samovar] Measure samovar metrics
* [Samovar] Make samovar logs more verbose

### Tea release 1.62.1, 2025-02-18

#### Bug Fix
* Fix incorrect missing RowGroup::file_offset handling

### Tea release 1.62.0, 2025-02-13

#### New Feature
* Implement filters on iceberg metadata level
* Support config in JSON format
* Add config validator

### Tea release 1.61.2, 2025-02-06

#### Bug Fix
* Make uuid work without mac address

### Tea release 1.61.1, 2025-02-04

#### Backward Incompatible Change
* Improve endpoints handling in config

### Tea release 1.61.0, 2025-01-31

#### Bug Fix
* Fix missing positional delete file handling
* Fix processing order for PlannedMetadata
* Do not depend on order of AnnotatedDataEntry in Reader::Advance

#### New Feature
* Add flag for enabling postfilter on gp
* Configure access to meta via .cfg file
* Add parameter 'json_max_message_size_on_master'
* Add fallback for HMS

### Improvements
* Make filters more usable
* Balance reading iceberg data entries by split offsets

### Tea release 1.60.1, 2025-01-27

#### Bug Fix
* Handle missing positional delete file

### Tea release 1.60.0, 2025-01-20

#### Backward Incompatible Change
* Enable `use_row_filter` option for FDW by default

#### Bug Fix
* Handle missing column in equality delete file

#### New Feature
* CP1251 substitutor for Json columns
* Support row-group filter for `CASE`

#### Experimental Feature
* Add `filter_ignored_op_exprs` and `filter_ignored_func_exprs` parameteres
* Add `prefetch` parameter

### Tea release 1.59.0, 2024-12-27

#### Backward Incompatible Change
* Drop support for upgrades from Tea before 1.56.2
* Add `CREATE SERVER tea_server ...` to extension SQL scripts

#### Bug Fix
* Fix 'Corrupt snappy compressed data'
* Fix float inconsistency with GP
* Use bankers rounding

#### New Feature

#### Improvement
* Make TeaContext safer
* Make logger thread-safe
* Build partial RowGroupFilter for and/or
* Set default `grpc_max_message_size` to 16MB

### Tea release 1.58.2, 2024-12-18

#### New Feature
* Support `bytea` and `bytea[]`
