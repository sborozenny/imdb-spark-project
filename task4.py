import pyspark.sql.types as t
import pyspark.sql.functions as f
from read_write import write
import settings as s


def task4(df):
    res = df.select(f.col('primaryName'), f.col('primaryTitle'), f.col('characters')).filter(
        f.col('characters').isNotNull() & (f.col('characters') != '\\N'))
    #  res.show()
    write(res, s.result4_dir)
