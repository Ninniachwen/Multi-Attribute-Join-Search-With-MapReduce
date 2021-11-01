from collections import Counter
from handlers import CustomException


class MatchList:
    total_matches: int
    score: int
    col_index: Counter

    def __init__(self, match1, match2=None, tableid=0):
        """
        class for collecting matches for a table and calculate scores
        :param match1: first matching row
        :param match2: second matching row
        :param tableid: table id
        """
        self.total_matches = 1
        self.score = 0
        self.col_index = None
        if match2 is None:  # result of sql-matching
            self.table = match1[0]
            self.list = [Match(match1[1:4])]
            self.rel_col = [match1[2]]
            self.insert_no_duplicates(Match(match1[4:7]))
        elif type(match1) is list:  # input is a list
            self.table = match1[1]
            self.list = [Match(match1)]
            self.rel_col = [match1[2]]
            self.insert_no_duplicates(Match(match2))
        elif type(match1) is Match:  # input is of type Match
            self.table = tableid
            self.list = [match1]
            self.rel_col = [match1.col]
            self.insert_no_duplicates(match2)
        else:
            raise CustomException(f"wrong input format for Match")
        if self.table == 0:
            print("no Table given, Table is set to 0")

    def add_match(self, match1, match2=None):
        """
        adds two new matches to the list
        :param match1: first match to add
        :param match2: second match to add
        """
        self.total_matches += 1
        if type(match1) is list:
            self.insert_no_duplicates(Match(match1))
            self.insert_no_duplicates(Match(match2))
        elif type(match1) is Match:
            self.insert_no_duplicates(match1)
            self.insert_no_duplicates(match2)

    def insert_no_duplicates(self, new_match):
        """
        inserts a new match into the list, if it doesn't exist yet
        :param new_match: match to insert
        :return: None
        """
        i = 0
        for item in self.list:
            if item < new_match:
                i = i + 1
            elif item == new_match:
                return
            else:
                self.list.insert(i, new_match)
                self.rel_col.insert(i, new_match.col)
                return
        self.list.append(new_match)
        self.rel_col.append(new_match.col)

    def calculate_col_index(self):
        """
        calculates the join-score per table and column, based on the matches in the list
        """
        self.col_index = Counter(self.rel_col)
        top_two = self.col_index.most_common(2)
        self.score = top_two[0][1] + top_two[1][1]
        self.total_matches = len(self.rel_col)

    def __eq__(self, other):
        return self.score == other.score

    def __ne__(self, other):
        return self.score != other.score

    def __lt__(self, other):
        return self.score < other.score

    def __le__(self, other):
        return self.score <= other.score

    def __gt__(self, other):
        return self.score > other.score

    def __ge__(self, other):
        return self.score >= other.score

    def __str__(self):
        colindex = str(self.col_index)
        colindex = colindex.replace("Counter(", "column scores")
        colindex = colindex.replace(")", "")
        return f"<{self.table}, {self.score}, {colindex}>"

    def __repr__(self):
        return str(self)

    def print_matches(self):
        """
        prints string representations of all matches in the list
        :return: string, representing all matches
        """
        result = []
        for item in self.list:
            result.append(str(item))
        return result


class Match:
    def __init__(self, array):
        """
        transforms array of match into a Match object. storing key, column id and row id
        :param array: array of length 4, [key, table_id, col_id, row_id]
        """
        self.key = array[0]
        self.col = array[2]
        self.row = array[3]

    def __eq__(self, other):
        return (self.key == other.key) & (self.col == other.col) & (self.row == other.row)

    def __lt__(self, other):
        if self.col < other.col:
            return True
        elif self.col == other.col:
            if self.row < other.row:
                return True
            elif self.row == other.row:
                if self.key < other.key:
                    return True
        return False

    def __str__(self):
        return f"<{self.key}, {self.col}, {self.row}>"

    def __repr__(self):
        return str(self)
