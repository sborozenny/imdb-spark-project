import pyspark.sql.types as t
import pyspark.sql.functions as f
from read_write import write
import settings as s


def task1(df):
    res = df.filter(f.col('region') == 'UA').select('title', 'region')
    # res.write.csv('result/res1', header=True, mode='overwrite')
    # res.show()
    write(res, s.result1_dir)
