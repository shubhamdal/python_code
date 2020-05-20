#https://github.pie.apple.com/amp-ae/cloud-analytics-examples/blob/master/scripts/CatalogResetGenerator.scala

from datetime import datetime, timedelta
import os, re, sys
import getpass

from pyspark.sql.functions import udf, col, explode, from_json, row_number, dense_rank, desc, sum, count, size 
from pyspark.sql.functions import lit, from_unixtime, date_format, coalesce, from_utc_timestamp
from pyspark.sql.window import Window
from pyspark.sql.column import Column, _to_java_column

teradata = {
    "schema" : "dev",
    "url" : "jdbc:teradata://edwitscop124.corp.apple.com/CHARSET=UTF16,TYPE=FASTLOAD",
    "driver": "com.teradata.jdbc.TeraDriver",
    # It might be advantageous to increase the batch size to a higher number: Please see
    # https://downloads.teradata.com/connectivity/articles/speed-up-your-jdbcodbc-applications
    "batchsize": 10000
}

common_dataset_urls = {
    "figaro.preprocessed" : {
        "title" : "Pre-Processed (Figaro/Armstrong)",
        "description" : "App Store pre-processed figaro data (from Armstrong)",
        "persistance" : "Scheduled daily",
        "path" : "/group/hdp-amp-adp-de/appstore/figaro-preprocess/daily/final/rptg_local_dt=%s",
    },
    "figaro.preprocessed.extended" : {
        "title" : "App Store Analytics Projection (Figaro/Armstrong)",
        "description" : "App Store extended pre-processed figaro data (from Armstrong)",
        "persistance" : "Scheduled daily (-3 days for data latency)",
        "path" : "/group/hdp-amp-adp-de/appstore/figaro-preprocess/extended/daily/final/rptg_local_dt=%s",
    },
    "figaro.tabsummary" : {
        "title" : " Tab summary data",
        "description" : "App Store pre-processed figaro data (from Armstrong) + Impressions dimension",
        "persistance" : "Forever",
        "path" : "/group/hdp-amp-adp-de/appstore/tab-summary/daily/interim/rptg_local_dt=%s",
    },
    "metadata.storeFrontRef" : {
        "title" : "Storefront Ref",
        "description" : "Storefront reference table",
        "persistance" : "Evergreen",
        "path" : "/group/hdp-amp-adp-de/kafka-two/v3/mz_store_front/20170803",
    },
    "figaro.music.preprocessed" : {
        "title" : "Pre-Processed Music (Figaro/Armstrong)",
        "description" : "Music pre-processed figaro data (from Armstrong)",
        "persistance" : "Scheduled daily",
        "path" : "/group/hdp-amp-adp-de/music/figaro-preprocess/xp_its_music_main/daily/final/local_event_dt=%s",
    },
    "figaro" : {
        "title" : "Figaro/Armstrong",
        "description" : "Full tier 1 figaro data (from Armstrong)",
        "persistance" : "Scheduled daily",
        "path" : "/group/amp_metrics/tier1/figaro_clickstream_events_v2/visitStartDate=%s/visitSpill=false",
    },
}


def announce():
    import __announce__ as announce

def help():
    print """Example usage:
                params = {
                    'dataset' : 'figaro.preprocessed',
                    'dates' : {'start' : '2017-09-20', 'end' : '2017-09-21'},
                }
                date_range = falconlib.build_date_range(params['dates'])
                dates_to_load = falconlib.date_range_to_unix_range(date_range)
                df = falconlib.load_dataset(spark, params['dataset'], dates_to_load)
    """


def list_datasets():
    print "Available datasets:"
    row_line = "+%s+" % ''.join(["-" for i in range(0,217)])[1:]
    print row_line
    print "| {dataset:<30} | {title:<50} | {persistance:<25} | {description:<100} |".format(dataset="Dataset", title="Title", description="Description", persistance="Persistance")
    for k, v in common_dataset_urls.iteritems():
        print row_line
        print "| {dataset:<30} | {title:<50} | {persistance:<25} | {description:<100} |".format(dataset=k, title=v['title'], description=v['description'], persistance=v['persistance'])
    print row_line


def dataset_detail(dataset):
    print common_dataset_urls[dataset]
    print os.system('hdfs dfs -ls %s' % '/'.join(common_dataset_urls[dataset]['path'].split('/')[0:-1]))


"""
Initialization
"""


def initialize(spark, sqlContext):
    global _falcon_credentials
    global _spark
    global _sqlContext
    _spark = spark
    _sqlContext = sqlContext
    _falcon_credentials = {}

"""
General
"""


def load_dataset(spark, name, date):
    d = common_dataset_urls[name]
    print "Loading: ", d
    url = d['path']
    return spark.read.parquet(url % date)


def parse_date(date_string, **kwargs):
    return datetime.strptime('%s 00:00:00'%date_string, '%Y-%m-%d %H:%M:%S')


def build_date_range(params={}):
    start_date = parse_date(params['start'])
    end_date = parse_date(params['end'])
    return [(start_date + timedelta(days=+x)).strftime('%Y-%m-%d') for x in range(0,(end_date - start_date).days + 1)]


def convert_to_local(time, format="YYYY-mm-dd", offset="timezoneOffset"):
    """
    Convert timestamp to local format with formatting
    :param time: name of column containing timestamp in utc
    :param format: format string (default = YYYY-mm-dd)
    :param offset: name of the field containing timezone offset (in minutes) with the local timezone,
           if offset = NA - current time will be formatted without conversionudf-for-time-conversion-fix-42306474
    :return: formatted date column
    """
    tz = spark.conf.get("spark.sql.session.timeZone")
    if (offset != "NA"):
        offset_column = col(offset)
    else:
        offset_column = lit(0)

    MS_IN_MINUTE = 60 * 1000
    formatted_time_in_default_tz = from_unixtime((col(time) - coalesce(offset_column, lit(0)) * MS_IN_MINUTE) / 1000,
                                                 "yyyy-MM-dd hh:mm:ss")
    formatted_time_in_utc = to_utc_timestamp(formatted_time_in_default_tz, tz)
    return date_format(formatted_time_in_utc, format)


def date_range_to_unix_range(date_list):
    return "{%s}" % ','.join(date_list)


def strip_newlines(s):
    if s is not None:
        return s.replace('\n',' ')


def parse_franchise(s):
    if s is not None:
        r = re.findall('([A-Z]+$|[A-Z]{1,}[a-z]+)',s)
        return ' '.join(r)


def list_to_sql_list(python_list):
    return ','.join("'%s'" % i for i in python_list)


def json_string_to_struct(df, column_to_parse, columns_to_return, renamed_parsed_column=False):
    columns_to_return.append('json_string')
    df = df.withColumn('json_string',explode(col(column_to_parse))).select(columns_to_return)
    json_string_schema = _spark.read.json(df.rdd.map(lambda row: row.json_string)).schema
    if renamed_parsed_column is False:
        parsed_column_name = '%s_parsed' % column_to_parse
    else:
        parsed_column_name = renamed_parsed_column
    columns_to_return.pop()
    columns_to_return.append(parsed_column_name)
    df = df.withColumn(parsed_column_name, from_json(col('json_string'), json_string_schema)).select(columns_to_return)
    return df


def left_join(df1, df2, join_column):
    df1 = df1.alias('df1')
    df2 = df2.alias('df2')
    joined_df = df1.join(df2, join_column, 'left')
    return joined_df


def list_hdfs_directory(hdfs_path):
    os.system('hdfs dfs -ls %s' % hdfs_path)


def unique_values(dataFrame, columnName):
    """
    Get a list of the unique values in a column of a dataframe
    """
    return dataFrame.select('%s' % columnName).distinct().rdd.map(lambda r: r[0]).collect()


def temp_path():
    import getpass
    username = getpass.getuser()
    return "/user/%s/temptable" % username


def write_temptable_to_hdfs(object, tablename, fail_if_path_exists=False):
    """
    Overwrite a df to a path in a "temptable" path on hdfs in parquet format
    :param object: dataframe
    :param tablename: folder in hdfs
    :param fail_if_path_exists: overwrite or fail if path exists
    """
    import subprocess
    path = temp_path() + "/" + tablename

    checkCommand = "hdfs dfs -test -e %s" % path
    checkProcess = subprocess.Popen(checkCommand.split(), stdout=subprocess.PIPE)
    checkProcess.communicate()
    path_exists = checkProcess.returncode
    if(fail_if_path_exists and path_exists):
        raise Exception('Path [%s] exists' % path)
    else:
        bashCommand = "hdfs dfs -rmr %s" % path
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        object.write.parquet(path)


def read_temptable_from_hdfs(tablename):
    """
    Read a path in the user's "temptable" directory
    :param tableName:
    :return: dataframe
    """
    path = temp_path() + "/" + tablename
    return _sqlContext.read.parquet(path)


def tv(df, view_name):
    """
    Alias for
    df.createOrReplaceTempView(alias)
    :param df: dataframe
    :param alias: view name
    :return:
    """
    df.createOrReplaceTempView(view_name)


def show(query, rows=10):
    """
    Alias for
    spark.sql("query").show(10, False)
    :param query: Spark SQL query
    """
    _spark.sql(query).show(rows, False)


def show_all(query):
    """
    Alias for
    spark.sql("query").show(99999, False)
    :param query: Spark SQL query
    """
    show(query, rows=99999)


def q(query):
    """
    Alias for
    obj_name = spark.sql("sqlScript")
    :param query: Spark SQL query
    :return: dataframe
    """
    return _spark.sql(query)


def unionAll(*dfs):
    """
    Unions multiple dataframes
    :param dfs: array of dataframes
    :return: dataframe
    """
    from pyspark.sql import DataFrame
    return reduce(DataFrame.unionAll, dfs)


def load_range(input_range, base_path, append_dates=True, filter_statement=None, select_list=None):
    """
    Append data from multiple dates together into one df
    Example:
    falconlib.load_range(['2018-01-01', '2018-01-02'], '/group/hdp-amp-adp-de/appstore/figaro-preprocess/daily/final/rptg_local_dt=')
    :param input_range: range of dates
    :param base_path: base path
    :param append_dates: appends a column called "rptg_local_dt" with the date the hdfs path corresponds to
    :param filter_statement: filter
    :param select_list: columns to select
    :return: dataframe
    """
    from pyspark.sql.functions import lit
    if select_list == None:
        effectiveSelectStatement = '*'
    else:
        effectiveSelectStatement = select_list
    if filter_statement == None:
        effectiveFilterStatement = '1 = 1'
    else:
        effectiveFilterStatement = filter_statement

    dfs = []
    for d in input_range:
        path = base_path + d
        df = _sqlContext.read.parquet(path).filter(effectiveFilterStatement).select(effectiveSelectStatement)
        if append_dates == True:
            df = df.withColumn("rptg_local_dt", lit(d))
        dfs.append(df)

    return unionAll(*dfs)


def yarn_status(*args, **kwargs):
        """
        More human readable yarn status. Please see doc string in 'yarn_status.py' 
        for function of same name for a parameter description. This is just a
        wrapper for making the function accessible through falconlib.
        """
        import yarn_status
        yarn_status.yarn_status(*args, **kwargs)
      

def explode_outer(col):
    _explode_outer = _spark.sparkContext._jvm.org.apache.spark.sql.functions.explode_outer 
    return Column(_explode_outer(_to_java_column(col)))


"""
Stats
"""
def histogram(df, column, column_aggregation, distribution = (1,10), truncate_columns = True, percent_total = True):
	"""
	Returns a dataframe with a histogram.

	histogram(usage_bundle_combos, 'device_count', 'sum', (1,10))
	"""
	buckets = range(distribution[0],distribution[1]+1)
	# Histogram conditions
	conditions = ["when {column} = {bucket} then {bucket}".format(column = column, bucket = bucket) for bucket in buckets]
	conditions.append("else '%s+' end" % str(buckets[-1]+1))
	conditions.insert(0, 'case ')
	histogram = ' '.join(conditions)
	# Histogram ranking conditions
	conditions_ranks = range(distribution[0],distribution[1]+2)
	conditions_rank = ["when histogram = '{conditions_rank}' then {conditions_rank}".format(conditions_rank = conditions_rank) for conditions_rank in conditions_ranks[0:-1]]
	conditions_rank.append("else %s end" % str(conditions_ranks[-1]+1))
	conditions_rank.insert(0, 'case ')
	histogram_sort = ' '.join(conditions_rank)
	# Aggregation
	df2 = df.withColumn('histogram', expr(histogram)).withColumn('histogram_sort', expr(histogram_sort)).groupBy('histogram','histogram_sort').agg({column : column_aggregation})
	if percent_total == True:
		total = df.agg({column : column_aggregation}).collect()[0]["{column_aggregation}({column})".format(column = column, column_aggregation = column_aggregation)]
		df3 = df2.withColumn('%', format_number(col("{column_aggregation}({column})".format(column = column, column_aggregation = column_aggregation))/total, 3)).orderBy('histogram_sort')
	else:
		df3 = df2.orderBy('histogram_sort')
	# Columns to return
	if truncate_columns == False:
		return df3
	else:
		return df3.drop('histogram_sort')

"""
Input/output
"""

def set_teradata_credentials():
    """
    Function 'set_teradata_credentials' prompts for and caches Teradata credentials
    :return:
    """
    _falcon_credentials['teradata_username'] = raw_input("Enter Teradata user: ")
    _falcon_credentials['teradata_password'] = getpass.getpass("Enter Teradata password: ")


def load_teradata_table(tableName, column=None, lowerBound=None, upperBound=None, numPartitions=None, predicates=None):
    """
     Function 'load_teradata_table' loads a Teradata table into a dataframe
    :param tableName:
    :param column: the name of a column of integral type that will be used for partitioning.
    :param lowerBound: the minimum value of `columnName` used to decide partition stride.
    :param upperBound: the maximum value of `columnName` used to decide partition stride.
    :param numPartitions: the number of partitions
    :param predicates: a list of expressions suitable for inclusion in WHERE clauses;
                       each one defines one partition of the DataFrame
    :return: DataFrame
    """
    if not _falcon_credentials:
        _falcon_credentials['teradata_username'] = raw_input("Enter Teradata user: ")
        _falcon_credentials['teradata_password'] = getpass.getpass("Enter Teradata password: ")

    properties = {
        "user": _falcon_credentials['teradata_username'],
        "password": _falcon_credentials['teradata_password'],
        "driver": teradata['driver']
    }
    print "Loading table %s from %s" % (tableName, teradata['url'])
    return _spark.read.jdbc(url=teradata['url'],
                            table=tableName,
                            column=column,
                            lowerBound=lowerBound,
                            upperBound=upperBound,
                            numPartitions=numPartitions,
                            predicates=predicates,
                            properties=properties)


def save_into_teradata_table(input_data_frame, table_name, save_mode):
    """
    Function 'save_into_teradata_table' saves a dataframe to a Teradata table
    :param input_data_frame:
    :param table_name: the fully qualified table name, e.g. "itsp_amr.HDFS_TO_TD_TEST"
    :param save_mode: Specifies the behavior when data or table already exists.
      - `overwrite`: overwrite the existing data.
      - `append`: append the data.
      - `ignore`: ignore the operation (i.e. no-op).
      - `error`: default option, throw an exception at runtime.
    :return: None
    """
    if not _falcon_credentials:
        _falcon_credentials['teradata_username'] = raw_input("Enter Teradata user: ")
        _falcon_credentials['teradata_password'] = getpass.getpass("Enter Teradata password: ")

    print "Writing data frame %s into table %s" % (input_data_frame, table_name)

    # Mode 'append' cannot be used with FASTLOAD - table in TD needs to empty for FASTLOAD to work:
    # https://www.info.teradata.com/HTMLPubs/DB_TTU_16_00/index.html#page/Load_and_Unload_Utilities/B035-2411%E2%80%90116K/2411Ch01.03.2.html
    td_url = teradata['url'].replace(',TYPE=FASTLOAD','') if save_mode == 'append' else teradata['url']

    gateway = _spark.sparkContext._gateway
    from py4j.java_gateway import java_import
    java_import(gateway.jvm, 'com.apple.amp.falcon.teradata_support.HdfsToTeradata')
    hdfs2td = gateway.jvm.HdfsToTeradata(td_url, teradata['driver'], teradata['batchsize'])

    hdfs2td.writeToTeradata( input_data_frame._jdf, table_name,
                             _falcon_credentials['teradata_username'],
                             _falcon_credentials['teradata_password'],
                             save_mode )


"""
HDFS Utilities
"""

def delete_from_hdfs(path):
    print "Warning this will delete %s \n Do you want to proceed? [Y/N]" % path
    input = sys.stdin.read(1).lower()
    if input == 'n':
        print "Skipping delete"
    elif input == 'y':
        print "Deleting %s" % path
        os.system('hdfs dfs -rm -r -skipTrash %s' % path)
    else:
        print "Incorrect input"


"""
Register UDFs
"""
strip_newlines=udf(strip_newlines)
parse_franchise=udf(parse_franchise)

