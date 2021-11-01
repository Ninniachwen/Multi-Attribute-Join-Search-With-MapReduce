import bisect
import contextlib
from datetime import datetime

import pandas as pd

from Object import MatchList
from handlers import DbHandler

MAX_OBJECTS = 30


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


class JoinScoreSequential:
    url: str
    col_names: [str]
    db_handler: DbHandler
    sql: bool
    colwise: bool
    cellwise: bool
    rowwise: bool
    job_id: str

    def __init__(self, url, col_names, col_wise, cell_wise, row_wise, sql_wise):
        self.url = url
        self.col_names = col_names
        self.sql = sql_wise
        self.colwise = col_wise
        self.cellwise = cell_wise
        self.rowwise = row_wise
        if sql_wise:
            self.job_id = "sql_"
        elif self.cellwise:
            self.job_id = "cell_"
        elif self.rowwise:
            self.job_id = "row_"
        else:
            self.job_id = "col_"
            self.colwise = True  # default query type is col-wise

        self.job_id = self.job_id + "seq_"

    def start(self, rows):
        print("---")
        self.job_id = self.job_id + f"{rows}"
        print(self.job_id)
        with log_runtime('all'):

            # extract x columns from given csv table
            with log_runtime("import"):
                input_data = self.get_input_data(rows)

            # query database for all entries
            with log_runtime("query"):
                unsorted_data = self.query_datasource(input_data)

            # sort data and reduce to matching rows
            with log_runtime("join_discovery"):
                matches = self.join_discovery(unsorted_data, input_data)

            with log_runtime("scoring"):
                highscore = self.calculate_scores(matches)

        print("highscore:")
        print("<table_id, score, column scores{colx: col_score, coly:col_score, ...}>")
        for item in highscore:
            print(item)

        return input_data, unsorted_data, matches, highscore

    def get_input_data(self, rows, delimiter=","):
        """
        imports a csv-file from a given url, stores it as Pandas DataFrame and trims it to the desired dimension

        :param rows: number of rows from input data that are used for the search
        :param delimiter: delimiter for the csv file
        :return: input data: those columns of the given csv that should be used for querying. shortened to the number of
                 rows that should be used
        """
        self.db_handler = DbHandler()

        # import data, select search-columns and -rows , remove duplicates
        df_full_table = pd.read_csv(self.url, delimiter=delimiter)
        df_selected = df_full_table[self.col_names]
        if rows > 0:
            df_no_nan = df_selected.truncate(0, rows - 1, copy=False) \
                .drop_duplicates() \
                .dropna()
        else:
            df_no_nan = df_selected.drop_duplicates() \
                .dropna()
        df_no_nan.replace(u'\xa0', u'', regex=True, inplace=True)

        # create appropriate lists for querying with the data
        input_data = []
        if self.colwise:
            for col in self.col_names:
                input_data.append(df_no_nan[col].tolist())
        else:
            input_data = df_no_nan.values.tolist()

        # clean search column strings
        for x, col in enumerate(input_data):
            if type(col) is list:
                for y, cell in enumerate(col):
                    input_data[x][y] = self.db_handler.clean_argument_for_query(cell)
            else:
                input_data[x] = self.db_handler.clean_argument_for_query(col)
        return input_data

    def query_datasource(self, input_data):
        print(f"querying with {len(input_data[0])} rows, this might take a while...")
        db_handler = DbHandler()
        unsorted_data = []

        for i, row in enumerate(input_data):  # or for column in search_columns

            if self.sql:
                unsorted_data.append(db_handler.query_join_discovery(row[0], row[1]))

            elif self.cellwise:
                sorted_data = {}
                for cell in row:
                    sorted_data[cell] = db_handler.query_one_argument(cell)
                unsorted_data.append(sorted_data)
            else:
                row_string = db_handler.list_to_string(row)
                if row_string != "":  # if rowwise query: null value results in empty string, query can be skipped
                    unsorted_data.append(db_handler.query_many_arguments(row_string))

        db_handler.disconnect()
        return unsorted_data

    def join_discovery(self, unsorted_data, input_columns):
        matches = {}
        search_term_dict = {}

        if self.sql:
            for query in unsorted_data:
                self.sort_matches_into_dicts(query, search_term_dict)
            return search_term_dict
        # column wise approach
        elif self.colwise:
            # for col in matches:
            # sort results into a dict
            for col_query in unsorted_data:
                self.sort_into_dicts(col_query, search_term_dict)

            # iterate search_term_dict by search_columns rows
            for x, y in zip(input_columns[0], input_columns[1]):  # iterate rows
                row_dict = {}
                row_dict[x] = search_term_dict.get(x)
                row_dict[y] = search_term_dict.get(y)
                if (row_dict.get(x) is None) or (row_dict.get(y) is None):
                    continue
                self.find_matching_rows(row_dict, matches)  # writes matching entries for each row into 'matches'

        # cell wise approach
        elif self.cellwise:
            # print("calculating matches...")
            for row in unsorted_data:
                self.find_matching_rows(row, matches)

        # row wise approach
        else:
            # print("calculating matches...")
            for row in unsorted_data:
                search_term_dict = {}
                # splitting unsorted matches into as many dict entries as there are search columns.
                self.sort_into_dicts(row, search_term_dict)
                self.find_matching_rows(search_term_dict, matches)

        return matches

    @staticmethod
    def sort_into_dicts(list, search_term_dict, i=0):
        for cell in list:
            if search_term_dict.get(cell[i]) is None:
                search_term_dict[cell[i]] = [cell]
            else:
                search_term_dict[cell[i]].append(cell)

    @staticmethod
    def sort_matches_into_dicts(list, search_term_dict):
        for cell in list:
            if search_term_dict.get(cell[0]) is None:
                search_term_dict[cell[0]] = MatchList(cell)
            else:
                search_term_dict[cell[0]].add_match(cell)
        # match.calculate_col_index()

    @staticmethod
    def find_matching_rows(sorted_row, matches):
        keys = list(sorted_row.keys())
        # if there are results for both keys
        if len(keys) > 1:
            # comparing results of two keys
            for x in sorted_row[keys[0]]:
                for y in sorted_row[keys[1]]:
                    if (x[1] == y[1]) & (x[3] == y[3]):  # if same table and same row, result is relevant
                        if matches.get(x[1]) is None:  # if this table is not already listed, create a new Match_List
                            matches[x[1]] = MatchList(x, y)
                        else:  # if the table is listed already, add new Match to this Match_List
                            matches[x[1]].add_match(x, y)

    @staticmethod
    def calculate_scores(matches):
        # print("calculating scores...")
        high_score = []
        # key = table_id, value = item of class Match
        for key, value in matches.items():
            value.calculate_col_index()
            bisect.insort(high_score, value)
            high_score = high_score[-MAX_OBJECTS:]

        if not high_score:
            print("no matches found")
            return None
        else:
            high_score.reverse()
            return high_score
            # {1234: Match[table, score, relevant cols], 234:...}
