import argparse
import datetime
import re
import subprocess as sbp
import sys
from pprint import pprint

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def parse_log(log_dir, start_date, end_date, target_cols,
              spark, num_col=None):
    """parse the log that ``>= start_date`` and ``<= end_date``

    Args:
        log_dir:
            path to the directory that conclude the log of all date
        start_date:
            string in the format of ``'YYYY/MM/DD/HH'``, accept incomplete date
            like ``'YYYY'`` or ``'YYYY/MM'``, ...
        end_date:
            string in the format of ``'YYYY/MM/DD/HH'``, accept incomplete date 
            like ``'YYYY'`` or ``'YYYY/MM'``, ...
        target_cols:
            the columns that need to be counted
        num_col:
            the column that used as weight, if absent every column will be count 
            as 1

    Returns:
         result of statistic 
         ``{<target-column1>:{<tag1>:<frequency>, <tag2>:<frequency>, ... }, <target-column2>:{...}, ...}``
    """
    def path_generator(date, log_dir, debug=False):
        rc = sbp.check_output('hadoop dfs -ls' + ' ' +
                              log_dir + date, shell=True).decode('utf-8')
        if debug:
            print(rc)
        t = re.split(log_dir + '|' + '\n', rc)
        for i in [2 * i for i in range(1, len(t) // 2)]:
            if debug:
                print(t[i])
            yield t[i]

    def agg_log_funs(target_cols, num_col=None, sep=','):
        def seqOp(semi_r, row):
            for t_col in target_cols:
                tags = str(row[t_col]).strip().split(sep)
                num = row[num_col] if num_col and row[num_col] else 1
                for t in tags:
                    semi_r[t_col][t] = num + (semi_r[t_col].get(t) or 0)
            return semi_r

        def comOp(r1, r2):
            for t_col in target_cols:
                for k, v in r1[t_col].items():
                    r2[t_col][k] = v + (r2[t_col].get(k) or 0)
            return r2

        return seqOp, comOp

    target_dates = [""]

    # while len(target_dates) != 0 and not all([d.endswith('gz') for d in target_dates]):
    while len(target_dates) != 0 and not all([len(d) == 13 for d in target_dates]):
        # print(target_dates)
        t_dates = []
        for current_path in target_dates:
            # if current_path.endswith('gz'):
            if len(current_path) == 13:
                t_dates.append(current_path)
                continue
            elif current_path.endswith('_SUCCESS'):  # eliminate special case
                continue
            else:
                for deeper_path in path_generator(current_path, log_dir):
                    if start_date[0:len(deeper_path)] <= deeper_path <= end_date[0:len(deeper_path)]:
                        t_dates.append(deeper_path)
        target_dates = t_dates

    # sum_dict = {<target-column1>:{<tag1>:<frequency>, <tag2>:<frequency>, ... },
    #             <target-column2>:{...},
    #             ...}
    sum_dict = {t_col: {} for t_col in target_cols}
    comb_fun = agg_log_funs(target_cols, num_col)[1]
    for path in target_dates:
        try:
            df_p = spark.sparkContext.textFile(log_dir + path)
            df = spark.read.json(df_p)
        except:
            print("Fail to read/parse file: %s " % (log_dir + path))
            raise
        else:
            assert set(df.columns) >= {*target_cols}
            if num_col:
                assert set(df.columns) >= {num_col}

            print("File '%s' added" % path)
            r = df.rdd.aggregate({t_col: {} for t_col in target_cols},
                                 *agg_log_funs(target_cols, num_col))
            sum_dict = comb_fun(sum_dict, r)
    return sum_dict


def main(start_date, end_date, outfile):
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    log_dir = 'hdfs://some-folder/hourly/'
    target_cols = ['features', 'url', 'user_name', 'user_id', 'client_version']
    num_col = 'query_size'

    output = parse_log(log_dir, start_date, end_date,
                       target_cols, spark, num_col)

    output['date_range'] = {
        'start': start_date,
        'end': end_date
    }

    if output.get('features') is not None:
        if output['features'].get('None') is not None:
            del output['features']['None']

    with open(outfile, 'w') as out:
        pprint(output, stream=out)


def month_start(date: datetime.date):
    return datetime.date(date.year, date.month, 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Summarize the log sequence in the specified date range.'
    )

    required_args = parser.add_argument_group('required arguments')
    parser.add_argument('-start', type=str,
                        help='the start date: YYYY/MM/DD, default to the first day of the current month')
    parser.add_argument(
        '-end', type=str, help='the end date: YYYY/MM/DD, default to today')
    required_args.add_argument('--output', type=str,
                               help='the output file', required=True)
    args = parser.parse_args()

    today = datetime.date.today()
    date_format = "%Y/%m/%d"
    if args.start is None:
        args.start = month_start(today).strftime(date_format)
    if args.end is None:
        args.end = today.strftime(date_format)

    main(args.start, args.end, args.output)
