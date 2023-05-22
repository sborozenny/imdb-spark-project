import pyspark.sql.types as t
import pyspark.sql.functions as f

schema_title_akas = t.StructType([t.StructField("titleId", t.StringType(), False),
                                  t.StructField("ordering", t.IntegerType(), False),
                                  t.StructField("title", t.StringType(), True),
                                  t.StructField("region", t.StringType(), True),
                                  t.StructField("language", t.StringType(), True),
                                  t.StructField("types", t.StringType(), True),
                                  t.StructField("attributes", t.StringType(), True),
                                  t.StructField("isOriginalTitle", t.BooleanType(), True)])

schema_title_basics = t.StructType([t.StructField("tconst", t.StringType(), False),
                                    t.StructField("titleType", t.StringType(), True),
                                    t.StructField("primaryTitle", t.StringType(), True),
                                    t.StructField("originalTitle", t.StringType(), True),
                                    t.StructField("isAdult", t.IntegerType(), True),
                                    t.StructField("startYear", t.IntegerType(), False),
                                    t.StructField("endYear", t.IntegerType(), True),
                                    t.StructField("runtimeMinutes", t.IntegerType(), True),
                                    t.StructField("genres", t.StringType(), True)])

schema_title_crew = t.StructType([t.StructField("tconst", t.StringType(), False),
                                  t.StructField("directors", t.StringType(), False),
                                  t.StructField("writers", t.StringType(), False)])

schema_title_episode = t.StructType([t.StructField("tconst", t.StringType(), False),
                                     t.StructField("parentTconst", t.StringType(), False),
                                     t.StructField("seasonNumber", t.IntegerType(), False),
                                     t.StructField("episodeNumber", t.IntegerType(), False)])

schema_title_principals = t.StructType([t.StructField("tconst", t.StringType(), False),
                                        t.StructField("ordering", t.IntegerType(), False),
                                        t.StructField("nconst", t.StringType(), True),
                                        t.StructField("category", t.StringType(), True),
                                        t.StructField("job", t.StringType(), True),
                                        t.StructField("characters", t.StringType(), True)])

schema_title_ratings = t.StructType([t.StructField("tconst", t.StringType(), False),
                                     t.StructField("averageRating", t.IntegerType(), True),
                                     t.StructField("numVotes", t.IntegerType(), True)])

schema_name_basics = t.StructType([t.StructField("nconst", t.StringType(), False),
                                   t.StructField("primaryName", t.StringType(), False),
                                   t.StructField("birthYear", t.StringType(), False),
                                   t.StructField("deathYear", t.StringType(), False),
                                   t.StructField("primaryProfession", t.StringType(), False),
                                   t.StructField("knownForTitles", t.StringType(), False)])

result1_dir = 'result/res1'
result2_dir = 'result/res2'
result3_dir = 'result/res3'
result4_dir = 'result/res4'
result5_dir = 'result/res5'
result6_dir = 'result/res6'
result7_dir = 'result/res7'
result8_dir = 'result/res8'

title_akas_file_path = 'imdb-data/title.akas.tsv.gz'
name_basics_file_path = 'imdb-data/name.basics.tsv.gz'
title_principals_file_path = 'imdb-data/title.principals.tsv.gz'
title_basics_file_path = 'imdb-data/title.akas.tsv.gz'
title_crew_file_path = 'imdb-data/title.crew.tsv.gz'
title_ratings_file_path = 'imdb-data/title.ratings.tsv.gz'
title_episode_file_path = 'imdb-data/title.episode.tsv.gz'

