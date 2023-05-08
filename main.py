import pyspark.sql.types as t
import pyspark.sql.functions as f
import settings as s
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from task1 import task1
from task2 import task2
from task3 import task3
from task4 import task4

conf = SparkConf()
conf.setMaster("local").setAppName("task арр")
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)


def main():
    task1_df = spark.read.options(sep=r'\t').csv(s.title_akas_file_path, header=True, nullValue='null',
                                                 schema=s.schema_title_akas)
    task2_df = spark.read.options(sep=r'\t').csv(s.name_basics_file_path, header=True, nullValue='null',
                                                 schema=s.schema_name_basics)
    task3_df = spark.read.options(sep=r'\t').csv(s.title_basics_file_path, header=True, nullValue='null',
                                                 schema=s.schema_title_basics)
    title_principals_df = spark.read.options(sep=r'\t').csv(s.title_principals_file_path, header=True, nullValue='null',
                                                            schema=s.schema_title_principals)
    title_principals_df = title_principals_df.withColumn('characters',
                                                         f.when(f.col('characters').isin(r'\N', None), None).otherwise(
                                                             f.col('characters')))
    task4_df = title_principals_df.join(task2_df, "nconst", "inner") \
        .join(task3_df, 'tconst', "inner")
    task1(task1_df)
    task2(task2_df)
    task3(task3_df)
    task4(task4_df)


if __name__ == "__main__":
    main()
