import pyspark.sql.types as t
import pyspark.sql.functions as f
import settings as s
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window
from task1 import task1
from task2 import task2
from task3 import task3
from task4 import task4
from task5 import task5
from task6 import task6
from task7 import task7
from task8 import task8

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
    title_episode_df = spark.read.options(sep=r'\t').csv(s.title_episode_file_path, header=True, nullValue='null',
                                                         schema=s.schema_title_episode)
    title_ratings_df = spark.read.options(sep=r'\t').csv(s.title_ratings_file_path, header=True, nullValue='null',
                                                         schema=s.schema_title_ratings)

    task4_1_df = title_principals_df.join(task2_df, "nconst", "inner")
    task4_df = task4_1_df.join(task3_df, 'tconst', "inner")

    task5_1_df = task1_df.filter(f.col('region').isNotNull()).select(f.col('titleId').alias('tconst'), f.col('region'))
    task5_2_df = task3_df.filter(f.col('isAdult') == 1)
    task5_df = task5_1_df.join(task5_2_df, 'tconst', 'inner')

    task6_df = task3_df.join(title_episode_df, task3_df.tconst == title_episode_df.parentTconst, "inner")

    task7_df = task3_df.join(title_ratings_df, "tconst", "inner")

    task8_df = task3_df.join(title_ratings_df, "tconst", "inner")

    task1(task1_df)
    task2(task2_df)
    task3(task3_df)
    task4(task4_df)
    task5(task5_df)
    task6(task6_df)
    task7(task7_df)
    task8(task8_df)


if __name__ == "__main__":
    main()
