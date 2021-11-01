from collections import Counter
from handlers import CustomException


class MatchList:
    total_matches: int
    score: int
    col_index: Counter

    def __init__(self, m1, m2=None, t=0):
        self.total_matches = 1
        self.score = 0
        self.col_index = None
        if m2 is None:  # result of sql-matching
            self.table = m1[0]
            self.list = [Match(m1[1:4])]
            self.rel_col = [m1[2]]
            self.insert_no_duplicates(Match(m1[4:7]))
        elif type(m1) is list:  # input is a list
            self.table = m1[1]
            self.list = [Match(m1)]
            self.rel_col = [m1[2]]
            self.insert_no_duplicates(Match(m2))
        elif type(m1) is Match:  # input is of type Match
            self.table = t
            self.list = [m1]
            self.rel_col = [m1.col]
            self.insert_no_duplicates(m2)
        else:
            raise CustomException(f"wrong input format for Match")
        if self.table == 0:
            print("no Table given, Table is set to 0")

    def add_match(self, e1, e2=None):
        self.total_matches += 1
        if type(e1) is list:
            self.insert_no_duplicates(Match(e1))
            self.insert_no_duplicates(Match(e2))
        elif type(e1) is Match:
            self.insert_no_duplicates(e1)
            self.insert_no_duplicates(e2)

    def insert_no_duplicates(self, new_item):
        i = 0
        for item in self.list:
            if item < new_item:
                i = i + 1
            elif item == new_item:
                return
            else:
                self.list.insert(i, new_item)
                self.rel_col.insert(i, new_item.col)
                return
        self.list.append(new_item)
        self.rel_col.append(new_item.col)

    def calculate_col_index(self):
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
        result = []
        for item in self.list:
            result.append(str(item))
        return result


class Match:
    def __init__(self, array):
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
