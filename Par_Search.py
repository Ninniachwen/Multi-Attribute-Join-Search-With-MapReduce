import contextlib
import logging
from datetime import datetime

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

from handlers import DbHandler

MAX_OBJECTS = 20


@contextlib.contextmanager
def log_runtime(action_name: str) -> None:
    """
    Logs the runtime of the action within the context of this contextmanager.

    :param action_name: Name of the action, will be added to the log output.
    :type action_name: str
    :return: Nothing.
    :rtype: None
    """
    start_time = datetime.now()

    yield

    print("{}: {:.4f}".format(
        action_name, (datetime.now() - start_time).total_seconds()
    ))


class JoinScoreParallel:
    # variable: type
    spark: SparkSession
    url: str
    col_names: list
    col_count: int
    read: bool
    write: bool
    job_id: str
    job_setup: str

    def __init__(self, url, col_names, read, write, cores):
        memory = "50g"
        if cores != '128':
            self.spark = SparkSession.builder \
                .master("local[1]") \
                .config("spark.executor.instances", str(1)) \
                .config("spark.executor.cores", str(cores)) \
                .config("spark.sql.shuffle.partitions", str(cores)) \
                .config("spark.driver.memory", memory) \
                .getOrCreate()
        else:
            self.spark = SparkSession.builder \
                .config("spark.driver.memory", memory) \
                .getOrCreate()
        self.url = url
        self.col_names = col_names
        self.col_count = len(col_names)
        self.read = read
        self.write = write
        self.job_setup = f"{self.spark.conf.get('spark.executor.instances', '?')}N_" \
                         f"{self.spark.conf.get('spark.executor.cores', '?')}C_" \
                         f"{self.spark.conf.get('spark.sql.shuffle.partitions')}P_" \
                         f"{self.spark.conf.get('spark.driver.memory', '?')}"
        self.job_id = "col_par_"

    def start(self, rows):
        print("---")
        self.job_id = self.job_id + f"{rows}rows"
        print(f"{self.job_id}")

        with log_runtime('total'):
            read_failed = False
            if self.read:
                logging.info("reading")
                try:
                    df_input_data = self.spark.read.parquet(f"{self.job_id}_col")
                    df_unsorted_data = self.spark.read.parquet(f"{self.job_id}_data")
                except Exception as e:
                    print(e)
                    print("failed to read from disk, load data from DB")
                    read_failed = True

            if not self.read or read_failed:
                with log_runtime("import"):
                    # extract x columns from given csv table
                    list_input_data, df_input_data = self.get_input_data(rows)

                with log_runtime("query"):
                    # query database for all entries
                    df_unsorted_data = self.query_datasource(list_input_data)

            logging.info("starting calculations")
            with log_runtime("join discovery"):
                # join discovery: sort data and reduce to matching rows
                df_matches = self.join_discovery(df_unsorted_data, df_input_data)

            with log_runtime("scoring"):
                # calculate scores
                highscore_tbl, highscore_col = self.calculate_scores(df_matches)
                highscore_tbl.collect()
                highscore_col.collect()

        highscore_tbl.show()
        highscore_col.show()

        if self.write:
            try:
                df_input_data.write.parquet(f"{self.job_id}_col")
                print("written columns")
            except Exception as e:
                print(e)
            try:
                df_unsorted_data.write.parquet(f"{self.job_id}_data")
                print("written data")
            except Exception as e:
                print(e)

        return highscore_tbl, highscore_col

    def get_input_data(self, rows):

        # import data
        url = "https://github.com/BigDaMa/COCOA/raw/master/dataset/movie.csv"
        self.spark = SparkSession.builder.master("local[1]").getOrCreate()
        self.spark.sparkContext.addFile(url)
        df_all_data = self.spark.read.csv("file://" + SparkFiles.get("movie.csv"), header=True, inferSchema=True)

        # trim DataFrame (df) to user defined size, remove duplicates, rows containing nan values and \xa0
        print(rows)
        if rows > 0:
            df_input_data = df_all_data[self.col_names] \
                .limit(rows) \
                .drop_duplicates() \
                .dropna() \
                .replace(u'\xa0', u'')
        else:
            df_input_data = df_all_data[self.col_names] \
                .drop_duplicates() \
                .dropna() \
                .replace(u'\xa0', u'')

        # clean df according to vertica policy (special characters, stopwords, etc.)
        clean_udf = udf(lambda row: DbHandler.clean_argument_for_query(row), StringType())
        for name in self.col_names:
            df_input_data = df_input_data.withColumn(name, clean_udf(col(name)))

        # df to col-wise list of lists
        list_input_data = []
        df_input_data.persist()
        for name in self.col_names:
            list_input_data.append(df_input_data.select(name).rdd.flatMap(lambda x: x).collect())
        df_input_data.unpersist()

        return list_input_data, df_input_data

    def query_datasource(self, list_input_data):
        print(f"querying with {len(list_input_data[0])} rows, this might take a while...")
        db_handler = DbHandler(self.spark)

        schema = StructType([
            StructField('key', StringType(), True),
            StructField('table', IntegerType(), True),
            StructField('column', IntegerType(), True),
            StructField('row', IntegerType(), True)
        ])

        # create empty dataframe with schema
        unsorted_data = self.spark.createDataFrame([], schema)

        # in for loop: append data gained by query
        for col in list_input_data:
            query_string = db_handler.list_to_string(col)  # transform list into query string
            newRow = db_handler.query_many_arguments(query_string)  # query
            unsorted_data = unsorted_data.union(newRow)  # join with other results

        db_handler.disconnect()
        return unsorted_data

    def join_discovery(self, df_unsorted_data, df_input_data):

        # create mappings df (each cell value is mapped a MapKey, to identify its original row)
        df_mappings = df_input_data.select(concat_ws(" ", col(self.col_names[0]), col(self.col_names[1]))
                                           .alias("MapKey"), self.col_names[0], self.col_names[1]) \
            .selectExpr("stack(2,director_name,MapKey,movie_title,MapKey)as (key,MapKey)")
        # df_MapKey.show()
        # +--------------------+--------------------+
        # |              MapKey|                 key|
        # +--------------------+--------------------+
        # |james cameron avatar|       james cameron|
        # |james cameron avatar|              avatar|
        # |gore verbinski pi...|      gore verbinski|
        # |gore verbinski pi...|pirates caribbean...|
        #

        # sort tables an their rows into partitions, so further shuffling is reduced.
        df_partitioned_data = df_unsorted_data.repartitionByRange('table', 'row')
        # print(partitioned_data.show())
        # +--------------------+-----+------+---+
        # |                 key|table|column|row|
        # +--------------------+-----+------+---+
        # |              avatar|  123|     2|  3|
        # |       james cameron|  123|     1|  3|
        # |      gore verbinski|  123|     1|  4|
        # +--------------------+-----+------+---+ partition
        # |pirates caribbean...|  124|     2|  4|
        # +--------------------+-----+------+---+ partition
        # |              avatar|  125|     2|  3|
        # |       james cameron|  125|     1|  3|
        # |      gore verbinski|  125|     1|  4|
        # |pirates caribbean...|  125|     2|  4|
        # +--------------------+-----+------+---+

        # drop rows with 1 entry per table, 1 entry per row in table, to make df smaller before union
        window = Window.partitionBy("table", "row").orderBy("row")
        df_reduced_data = df_partitioned_data \
            .dropDuplicates(["table", "row", "key"]) \
            .withColumn("count", count("table").over(window)) \
            .filter(f"count>={self.col_count}") \
            .drop("count")
        # reduced_data.show()
        # +--------------------+-----+------+---+
        # |                 key|table|column|row|
        # +--------------------+-----+------+---+
        # |              avatar|  123|     2|  3|
        # |       james cameron|  123|     1|  3|
        # |              avatar|  125|     2|  3|
        # |       james cameron|  125|     1|  3|
        # +--------------------+-----+------+---+
        # |      gore verbinski|  125|     1|  4|
        # |pirates caribbean...|  125|     2|  4|
        # +--------------------+-----+------+---+

        # join unsorted_data with MapKey
        df_mapped_data = df_reduced_data.join(df_mappings, ['key'])
        # reduced_data.show()
        # +--------------------+-----+------+---+--------------------------------------------+
        # |                 key|table|column|row|                                      MapKey|
        # +--------------------+-----+------+---+--------------------------------------------+
        # |       james cameron|  125|     1|  3|                        james cameron avatar|
        # |       james cameron|  123|     1|  3|                        james cameron avatar|
        # |              avatar|  125|     2|  3|                        james cameron avatar|
        # |              avatar|  123|     2|  3|                        james cameron avatar|
        # |      gore verbinski|  125|     1|  4| gore verbinski pirates caribbean word's end|
        # |pirates caribbean...|  125|     2|  4| gore verbinski pirates caribbean word's end|
        # |      gore verbinski|  125|     1|  4|          gore verbinski pirates black pearl|
        # |      gore verbinski|  125|     1|  4|                  gore verbinski lone ranger|
        # +--------------------+-----+------+---+--------------------------------------------+

        # reduce data to remove rows with duplicate MapKey
        window = Window.partitionBy("table", "row", "MapKey").orderBy("row")
        df_matches = df_mapped_data \
            .withColumn("count", count("MapKey").over(window)) \
            .filter(f"count>={self.col_count}") \
            .drop("count")

        # matches.show()
        # +--------------------+-----+------+---+--------------------+
        # |                 key|table|column|row|              MapKey|
        # +--------------------+-----+------+---+--------------------+
        # |       james cameron|  128|     2|  3|james cameron avatar|
        # |              avatar|  128|     1|  3|james cameron avatar|
        # |              avatar|  128|     3|  3|james cameron avatar|
        # |              avatar|  127|     2|  3|james cameron avatar|
        # |              avatar|  127|     1|  3|james cameron avatar|
        # |              avatar|  123|     2|  3|james cameron avatar|
        # |       james cameron|  123|     1|  3|james cameron avatar|
        # |              avatar|  125|     2|  3|james cameron avatar|
        # |       james cameron|  125|     1|  3|james cameron avatar|
        # |      gore verbinski|  125|     1|  4|gore verbinski pi...|
        # |pirates caribbean...|  125|     2|  4|gore verbinski pi...|
        # +--------------------+-----+------+---+--------------------+

        return df_matches

    def calculate_scores(self, df_matches):

        score_per_col_all = df_matches.sort("table", "column") \
            .groupBy("table", "column") \
            .agg(count('column')) \
            .sort("count(column)", ascending=False) \
            .withColumnRenamed("count(column)", "join-score")

        window = Window.partitionBy(score_per_col_all['table']).orderBy(score_per_col_all['join-score'].desc())
        score_per_col = score_per_col_all.select(col('*'), row_number().over(window).alias('row_number')) \
            .where(col('row_number') <= self.col_count) \
            .drop(col('row_number')) \
            .orderBy(col("join-score").desc())

        score_per_table = score_per_col.groupBy("table") \
            .agg(sum('join-score')) \
            .withColumnRenamed("sum(join-score)", "join-score") \
            .orderBy(col("join-score").desc())

        highscore_per_col = score_per_col.limit(2 * MAX_OBJECTS)
        highscore_per_tbl = score_per_table.limit(MAX_OBJECTS)

        return highscore_per_tbl, highscore_per_col
