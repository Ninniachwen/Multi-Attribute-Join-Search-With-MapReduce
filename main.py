import argparse

from Par_Search import JoinScoreParallel
from Seq_Search import JoinScoreSequential

URL = "https://github.com/BigDaMa/COCOA/raw/master/dataset/movie.csv"
COL_NAMES = ["director_name", "movie_title"]

if __name__ == '__main__':


    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('lines', metavar='lines', nargs='?', default="0",
                        help='an integer for the number of lines to be processed. 0 = all lines are used')
    parser.add_argument('cores', metavar='cores', nargs='?', default="128",
                        help='an integer for the number of cores used')
    parser.add_argument('-col', action='store_true', help="seq only! 1 sql query per column of search data")
    parser.add_argument('-row', action='store_true', help="seq only! 1 sql query per row of search data")
    parser.add_argument('-cell', action='store_true', help="seq only! 1 sql query per cell of search data")
    parser.add_argument('-sql', action='store_true', help="seq only! 1 sql query per row of search data, "
                                                          "join discovery on SQL-level, not python level")
    parser.add_argument('-par', action='store_true', help="parallelized algorithm: Par-Search")
    parser.add_argument('-seq', action='store_true', help="sequential algorithm: Seq-Search")
    parser.add_argument('-read', action='store_true', help="par only! data should be read from local")
    parser.add_argument('-write', action='store_true', help="par only! data should be stored from locally")
    args = parser.parse_args()

    # start Par-Search or Seq-Search
    if args.par:
        join_score = JoinScoreParallel(URL, COL_NAMES, args.read, args.write, args.cores)
    else:
        join_score = JoinScoreSequential(URL, COL_NAMES, args.col, args.cell, args.row, args.sql)

    highscore = join_score.start(int(args.lines))
