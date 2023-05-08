import pyspark.sql.types as t
import pyspark.sql.functions as f
from read_write import write
import settings as s


def task4(df):
    res = df.select(df.primaryName, df.primaryTitle, df.characters).filter(
        f.col('characters') != 'null')
    #  res.show()
    write(res, s.result4_dir)
