# Author:  dario zhang
# Python Version:  3.6.5
# Description:  This file is my personal utils for writing pyspark codes, maybe also useful for you.

# Standard library
import sys
if sys.version_info[0] >=3:
unicode = str

# Third-party libraries
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructTyep, StringType, DoubleType, IntegerType, Row
from pyspark import accumulators
from pyspark.accumulators import AccumulatorParam


class AddDictParam(AccumulatorParam):
    """dict accumulator, add value"""
    def zero(self, value=0):
        return dict()
    def addInPlace(self, x, y):
        dict_add(x, y, need_return=False)


class CoverDictParam(AccumulatorParam):
    """dict accumulator, Cover value"""
    def zero(self, value=""):
        return dict()
        
    def addInPlace(self, x, y):
        x.update(y)
        

def dict_add(dict_1, dict_2, need_return=True):
    """add dict value"""
    for k, v in dict_2.items():
        if k in dict_1.keys():
            dict_1[k] += v
        else:
            dict_1[k] = v
    if need_return:
        return dict_1
        
def dict_combine(dict_list):
    """combine"""
    out = {}
    for d in dict_list:
        out.update(d)
    return out
    
def init(app_name=""):
    """init spark"""
    conf = SparkConf().setAppName(app_name)
    sc = SparkContext(conf=conf)
    hsql = SparkSession.builder \
            .config(conf=conf)\
            .enableHiveSupport()\
            .getOrCreate()
    conf.set("spark.driver.maxResultSize", 2)
    return sc, hsql

def get_schema(struct_fields):
    """
        struct_fields:  [StructField("a", StringType(), False),
                         StructField("b", DoubleType(), False), ...]
    """
    schema = StructType(struct_fields)
    return schema
    
def access_dataframe(hive_sql, sql):
    dataframe = hive_sql.sql(sql)
    return dataframe
    
def read_data_from_hive(hsql, sql, map_func=lambda x: x, filter_func=lambda x: True):
    """Using SQL sentence to read data from hive and format it to rdd type"""
    dataframe = access_dataframe(hsql, sql)
    data = dataframe.rdd.map(map_func).filter(filter_func)
    return data

def write_data_to_hive(hsql, sql, table_name, result_rdd, struct_fields, partition=False):
    """Using SQL sentence to write data to hive"""
    schema = get_schema(struct_fields)
    row_rdd = result_rdd.map(lambda p: Row(*p))
    dataframe = hsql.createDataFrame(row_rdd, schema)
    dataframe.registerTempTable(table_name)
    if partition:
        hsql.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        hsql.sql("set hive.exec.dynamic.partition=true")
    hsql.sql(sql)
    
def broadcast_config(sc, kwargs):
    """broadcast"""
    g_config = {key: value for key, value in kwargs.items()}
    g_config_b = sc.broadcast(g_config)
    return g_config_b
    
def any2utf8(text, errors='strict', encoding='utf-8'):
    if isinstance(text, unicode):
        return text.encode('utf-8')
    return unicode(text, encoding, errors=errors).encode('utf-8')
    
def load_single_row_file(sc, hdfs_file):
    lines = set([line.strip() for line in sc.textFile(hdfs_file).collect()])
    return lines
    
def format_list_to_rdd(sc, data):
    rdd = sc.parallelize(data)
    return rdd
