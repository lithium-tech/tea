from pyspark.sql.types import LongType, StructType, StructField

schema = StructType([StructField("a", LongType(), True),
                    StructField("b", LongType(), True)])
df = spark.createDataFrame([], schema)
df.writeTo("demo.gperov.test").create()

data = []
for i in range(10 * 1000):
    data.append((i, i * 2))

df = spark.createDataFrame(data, schema)
df.writeTo("demo.gperov.test").append()

spark.sql("ALTER TABLE demo.gperov.test SET TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')")

spark.sql("DELETE FROM demo.gperov.test WHERE a = 2101")

spark.sql("ALTER TABLE demo.gperov.test CREATE BRANCH `test-branch` RETAIN 7 DAYS WITH SNAPSHOT RETENTION 2 SNAPSHOTS")

spark.sql("SET spark.wap.branch = test-branch")

spark.sql("INSERT INTO demo.gperov.test VALUES (-42, -42)")

spark.sql("ALTER TABLE demo.gperov.test CREATE TAG `EOW-01`")

spark.sql("SET spark.wap.branch = main")

spark.sql("INSERT INTO demo.gperov.test VALUES (-43, -43)")
