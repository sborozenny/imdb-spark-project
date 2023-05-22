import pyspark.sql.functions as f
from read_write import write
import settings as s
from pyspark.sql import Window


def task7(df):
    res1 = df.filter(f.col('startYear').isNotNull())
    res2 = res1.withColumn('dec', f.floor(f.col('startYear') / 10) * 10)
    res3 = res2.select("primaryTitle", "dec", "averageRating")
    window = Window.partitionBy("dec").orderBy(f.col("averageRating").desc())
    res4 = res3.withColumn('top', f.row_number().over(window))
    res = res4.filter(f.col('top') <= 10).limit(50)
    #  res.show()
    write(res, s.result7_dir)
