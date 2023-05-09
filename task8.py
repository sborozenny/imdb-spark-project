import pyspark.sql.functions as f
from read_write import write
import settings as s
from pyspark.sql import Window


def task8(df):
    res = df.select("primaryTitle", "genres", "averageRating").filter(f.col('genres') != "null")
    window = Window.partitionBy("genres").orderBy(f.col("averageRating").desc())
    res1 = res.withColumn("top", f.row_number().over(window))
    res2 = res1.filter(f.col("top") <= 10).limit(50)
    write(res2, s.result6_dir)
