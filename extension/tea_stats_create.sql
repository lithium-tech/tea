CREATE TYPE iceberg_stats AS (
  starelid oid,
  staattnum int2,
  stanullfrac float4,
  stawidth int4,
  stadistinct float4
);
