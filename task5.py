import pyspark.sql.types as t
import pyspark.sql.functions as f
from read_write import write
import settings as s


def task5(df):
    res = df.groupBy('region').count().orderBy('count', ascending=False).limit(100)
    #  res.show()
    write(res, s.result5_dir)
