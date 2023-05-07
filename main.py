import pyspark.sql.types as t
import pyspark.sql.functions as f
import settings as s
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from task1 import task1
from task2 import task2
from task3 import task3

conf = SparkConf()
conf.setMaster("local").setAppName("task арр")
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)


def main():

    task1_df = spark.read.options(sep=r'\t').csv('imdb-data/title.akas.tsv.gz', header=True, nullValue='null', schema=s.schema_title_akas)
    task2_df = spark.read.options(sep=r'\t').csv("imdb-data/name.basics.tsv.gz", header=True, nullValue='null', schema=s.schema_name_basics)
    task3_df = spark.read.options(sep=r'\t').csv("imdb-data/title.basics.tsv.gz", header=True, nullValue='null', schema=s.schema_title_basics)
    title_principals_df = spark.read.options(sep=r'\t').csv("imdb-data/title.principals.tsv.gz", header=True, nullValue='null', schema=s.schema_title_principals)
    task4_df = title_principals_df.join(task2_df, "nconst", "inner")
    task1(task1_df)
    task2(task2_df)
    task3(task3_df)


if __name__ == "__main__":
    main()
