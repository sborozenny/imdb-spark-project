import pyspark.sql.types as t
import pyspark.sql.functions as f
from read_write import write
import settings as s


def task6(df):
    res = df.select(f.col('primaryTitle'), f.col('episodeNumber')) \
        .filter(f.col('episodeNumber') > 0)
    res1 = res.groupBy('primaryTitle').agg({'episodeNumber': 'count'}).orderBy('count(episodeNumber)', ascending=False)
    res2 = res1.limit(50)
    #  res2.show()
    write(res, s.result6_dir)
