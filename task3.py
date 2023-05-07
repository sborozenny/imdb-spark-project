import pyspark.sql.types as t
import pyspark.sql.functions as f
from read_write import write
import settings as s


def task3(df):
    res = df.filter(
        (f.col('titleType') == 'movie') & (f.col('runtimeMinutes') > 120)).select(f.col('primaryTitle'))
    #  res.show()
    write(res, s.result3_dir)
