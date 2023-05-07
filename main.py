import pyspark.sql.types as t
import pyspark.sql.functions as f


def main():
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
    conf = SparkConf()
    conf.setMaster("local").setAppName("task арр")
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc)

    movies_df = spark.read.options(sep=r'\t').csv("imdb-data/name.basics.tsv.gz", header=True)
    titles_df = spark.read.options(sep=r'\t').csv("imdb-data/title.akas.tsv.gz", header=True)
    res = titles_df.filter(f.col('region') == 'UA').select('title', 'region')
    res.write.csv('result/res1', header=True, mode='overwrite')
    #movies_df.show()
    res.show()


if __name__ == "__main__":
    main()
