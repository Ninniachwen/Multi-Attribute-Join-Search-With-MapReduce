import os
import re

import vertica_python
from pyspark.sql import SparkSession


class DbHandler:
    """establish connection to DB, handle queries and close connections"""

    spark: SparkSession = None
    cursor: vertica_python.vertica.cursor.Cursor = None
    main_table: str = None

    def __init__(self, spark: SparkSession = None, main_table='main_tokenized'):
        """
        establish connection to database, set member variables
        :param spark: Spark Session
        :param main_table: database to connect to (default main_tokenized)
        """
        self.spark = spark

        conn_info = {
            'port': os.environ.get("VPORT"),
            'host': os.environ.get("VHOST"),
            'user': os.environ.get("VUSER"),
            'password': os.environ.get("VPASSW"),
            'database': 'vdb',
            'session_label': os.environ.get("VSESSIONLABEL"),
            'read_timeout': 60000,
            'unicode_error': 'strict',
            'ssl': False,
            'use_prepared_statements': False
        }

        connection = vertica_python.connect(**conn_info)
        self.cursor = connection.cursor()
        self.main_table = main_table

    # ---query functions--- #

    def execute(self, query):
        """
        execute query on stored database
        :param query: String with complete SQL query
        :return: list of Lines returned by query
        """

        self.cursor.execute(query)
        if self.spark is None:
            fetched = self.cursor.fetchall()
            return fetched
        else:
            schema = ['tokenized', 'tableid', 'colid', 'rowid']
            fetched = self.cursor.fetchall()
            if len(fetched) == 0:
                raise CustomException("no data returned")
            df = self.spark.createDataFrame(fetched, schema)
            return df
        # return self.cursor.fetchall()

    def query_many_arguments(self, arguments):
        """
        queries column 'tokenized' from main_table for the given arguments
        :param arguments: Array of Strings (or String made of array) to find in tokenized
        :return: list of Lines returned by query
        """

        # return self.execute(f"select * from {self.main_table} "
        #                    f"where tokenized IN {arguments} "
        #                    )
        print("QUERY REDUCED TO certain tables")
        return self.execute(f"select * from {self.main_table} "
                            f"where tokenized IN {arguments} "
                            f"and (tableid = 29854416 "
                            f"or tableid = 49739316) ")

    def query_one_argument(self, argument):
        """
        queries column 'tokenized' from main_table for the given argument
        :param argument: string to find in tokenized
        :return: list of Lines returned by query
        """
        try:
            return self.execute(f"select * from {self.main_table} "
                                f"where tokenized like '{argument}'")
        except Exception as e:
            raise CustomException(f"error calling the query: {e}")

    def query_join_discovery(self, key1, key2):
        sql = f"""
                select colX.tableid, colX.tokenized, colX.colid, colX.rowid, colY.tokenized, colY.colid, colY.rowid 
                from( 
                    select tokenized, tableid, colid, rowid 
                    from main_tokenized 
                    where tokenized like '{key1}' 
                    group by tokenized, tableid, colid, rowid 
                )  as colX, ( 
                    select tokenized, tableid, colid, rowid 
                    from main_tokenized 
                    where tokenized like '{key2}' 
                    group by tokenized, tableid, colid, rowid 
                    ) as colY 
                    where colX.tableid = colY.tableid 
                    and colX.rowid = colY.rowid
                """
        try:
            return self.execute(sql)
        except Exception as e:
            raise CustomException(f"error calling the query: {e}")

    # ---helper--- #
    def list_to_string(self, array):
        for i, string in enumerate(array):
            array[i] = self.clean_argument_for_query(string)

        joined_array = "', '".join(array)
        formatted_array = f"('{joined_array}')"
        cleaned_array = formatted_array.replace(", \'\',", ",")  # remove empty string for sql query
        return cleaned_array

    @staticmethod
    def clean_argument_for_query(argument):
        """
        clean arguments for sql query. make lowercase and remove special characters
        :param argument: String to clean
        :return: cleaned string
        """
        arg_low = argument.lower()
        translation_table = dict.fromkeys(map(ord, '!@#$,.-;:_'), ' ')  # source 1
        arg_clean = arg_low.translate(translation_table)
        stopwords = ['a', 'the', 'of', 'on', 'in', 'an', 'and', 'is', 'at', 'are', 'as', 'be', 'but',
                     'by', 'for', 'it', 'no', 'not', 'or', 'such', 'that', 'their', 'there', 'these',
                     'to', 'was', 'with', 'they', 'will', 'v', 've', 'd']  # , 's']
        # cleaned = re.sub('[\W_]+', ' ', text.encode('ascii', 'ignore').decode('ascii'))
        cleaned = re.sub('[\W_]+', ' ', str(arg_clean).encode('ascii', 'ignore').decode('ascii')).lower()
        feature_one = re.sub(' +', ' ', cleaned).strip()
        # feature_one = re.sub(' +', '', cleaned).strip()
        # remove ' #feature_one = re.sub(' +', ' ', text).strip()
        feature_one = feature_one.replace(" s ", "''s ")

        for x in stopwords:
            feature_one = feature_one.replace(' {} '.format(x), ' ')
            if feature_one.startswith('{} '.format(x)):
                feature_one = feature_one[len('{} '.format(x)):]
            if feature_one.endswith(' {}'.format(x)):
                feature_one = feature_one[:-len(' {}'.format(x))]
        return feature_one

    def disconnect(self):
        """disconnects database"""
        if self.cursor:
            self.cursor.close()
        else:
            print("no connection to disconnect")

    def __str__(self):
        """print method"""
        if self.cursor:
            return f"db '{self.main_table}' is connected"
        else:
            return "no database connected"


class CustomException(Exception):
    """custom exception, described when raised"""

    def __init__(self, message="undescribed custom exception"):
        super().__init__(message)
