def main():
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
    conf = SparkConf()
    conf.setMaster("local").setAppName("task арр")
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc)

    movies_df = spark.read.csv("imdb-data/name.basics.tsv.gz")
    movies_df.show()

if __name__ == "__main__":
    main()
