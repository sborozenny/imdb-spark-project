import pyspark.sql.types as t
import pyspark.sql.functions as f
from read_write import write
import settings as s


def task2(df):
    res = df.filter((f.col('birthYear') > 1800) & (f.col('birthYear') < 1900)).select(f.col('primaryName'))
    #  res.show()
    write(res, s.result2_dir)
