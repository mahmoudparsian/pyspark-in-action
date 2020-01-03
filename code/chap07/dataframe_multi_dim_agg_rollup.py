#!/usr/bin/python
#-----------------------------------------------------
# Create a DataFrame and perform groupBy() and rollup()
# Input: NONE
#------------------------------------------------------
# Input Parameters:
#    NONE
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
# Notes:
# source: https://stackoverflow.com/questions/37975227/what-is-the-difference-between-cube-rollup-and-groupby-operators
#
# DataFrame.groupBy is simply an equivalent of the "GROUP BY"
# clause in standard SQL. In other words
#
# df.groupBy("foo", "bar")
# is equivalent to:
#
# SELECT foo, bar, [agg-expressions] 
#   FROM table 
#     GROUP BY foo, bar
#
# cube is equivalent to CUBE extension to GROUP BY. 
# It takes a list of columns and applies aggregate 
# expressions to all possible combinations of the 
# grouping columns. Lets say you have data like this:
#
# df.show()
#
#  +---+---+
#  |  x|  y|
#  +---+---+
#  |foo|  1|
#  |foo|  2|
#  |bar|  2|
#  |bar|  2|
#  +---+---+
# 
# and you compute cube(x, y) with count as an aggregation:
#
# df.cube("x", "y").count.show()
#
# +----+----+-----+     
# |   x|   y|count|
# +----+----+-----+
# |null|   1|    1|   <- count of records where y = 1
# |null|   2|    3|   <- count of records where y = 2
# | foo|null|    2|   <- count of records where x = foo
# | bar|   2|    2|   <- count of records where x = bar AND y = 2
# | foo|   1|    1|   <- count of records where x = foo AND y = 1
# | foo|   2|    1|   <- count of records where x = foo AND y = 2
# |null|null|    4|   <- total count of records
# | bar|null|    2|   <- count of records where x = bar
# +----+----+-----+
#
# A similar function to cube is rollup which computes hierarchical 
# subtotals from left to right:
#
# df.rollup("x", "y").count.show()
#  +----+----+-----+
#  |   x|   y|count|
#  +----+----+-----+
#  | foo|null|    2|   <- count where x is fixed to foo
#  | bar|   2|    2|   <- count where x is fixed to bar and y is fixed to  2
#  | foo|   1|    1|   ...
#  | foo|   2|    1|   ...
#  |null|null|    4|   <- count where no column is fixed
#  | bar|null|    2|   <- count where x is fixed to bar
#  +----+----+-----+
#
# Just for comparison lets see the result of plain groupBy:
#
# df.groupBy("x", "y").count.show()
#
#  +---+---+-----+
#  |  x|  y|count|
#  +---+---+-----+
#  |foo|  1|    1|   <- this is identical to x = foo AND y = 1 in CUBE or ROLLUP
#  |foo|  2|    1|   <- this is identical to x = foo AND y = 2 in CUBE or ROLLUP
#  |bar|  2|    2|   <- this is identical to x = bar AND y = 2 in CUBE or ROLLUP
#  +---+---+-----+
#
# To summarize:
#
# When using plain GROUP BY every row is included only once 
# in its corresponding summary.
#
# With GROUP BY CUBE(..) every row is included in summary of 
# each combination of levels it represents, wildcards included. 
# Logically, the shown above is equivalent to something like this 
# (assuming we could use NULL placeholders):
#
#   SELECT NULL, NULL, COUNT(*) FROM table
#   UNION ALL
#   SELECT x,    NULL, COUNT(*) FROM table GROUP BY x
#   UNION ALL
#   SELECT NULL, y,    COUNT(*) FROM table GROUP BY y
#   UNION ALL
#   SELECT x,    y,    COUNT(*) FROM table GROUP BY x, y
#
#
# With GROUP BY ROLLUP(...) is similar to CUBE but works 
# hierarchically by filling colums from left to right.
#
#   SELECT NULL, NULL, COUNT(*) FROM table
#   UNION ALL
#   SELECT x,    NULL, COUNT(*) FROM table GROUP BY x
#   UNION ALL
#   SELECT x,    y,    COUNT(*) FROM table GROUP BY x, y
#
#
# ROLLUP and CUBE come from data warehousing extensions so 
# if you want to get a better understanding how this works 
# you can also check documentation of your favorite RDMBS. 
# For example PostgreSQL introduced both in 9.5 and these are 
# relatively well documented.


from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import sum
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import grouping_id

if __name__ == '__main__':

    #if len(sys.argv) != 2:  
    #    print("Usage: dataframe_multi_dim_agg_groupby.py <file>", file=sys.stderr)
    #    exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_multi_dim_agg_groupby")\
        .getOrCreate()


    # DataFrame.groupBy()
    # The following examples show how perform
    # a groupBy for an existing DataFrame. 


    # Let's first create a DataFrame with 3 columns:
    data = [\
              ('Ames', 2006, 100),\
              ('Ames', 2007, 200),\
              ('Ames', 2008, 300),\
              ('Sunnyvale', 2007, 10),\
              ('Sunnyvale', 2008, 20),\
              ('Sunnyvale', 2009, 30),\
              ('Stanford', 2008, 90)\
           ]
    #
    print("data = ", data)
    #
    columns = ("city", "year", "amount")
    print("columns = ", columns)
    
    # create a new DataFrame
    sales = spark.createDataFrame(data , columns)   
    print("sales DataFrame:")
    sales.show() 

    #============================================
    # DataFrame.groupBy(*cols)
    #
    # Description:
    #     Groups the DataFrame using the specified columns, 
    #     so we can run aggregation on them. See GroupedData 
    #     for all the available aggregate functions.
    #
    # groupby() is an alias for groupBy().
    #
    # Parameters:	
    #    cols - list of columns to group by. Each element should 
    #           be a column name (string) or an expression (Column).
    #============================================

    # GROUP BY (city, year)
    # subtotals by (city, year)
    groupby_city_and_year = sales\
       .groupBy('city', 'year')\
       .agg(sum('amount').alias('amount'))
    
    #
    print("# GROUP BY (city, year)")
    groupby_city_and_year.show()
    
          
    # GROUP BY (city)
    # subtotals by (city)
    groupby_city = sales\
       .groupBy('city')\
       .agg(sum('amount').alias('amount'))\
       .select('city', lit(None).alias('year'), 'amount')
    #
    print("# GROUP BY (city)")
    groupby_city.show()

    # apply UNION to groupby_city_and_year and groupby_city
    groupby_with_union = groupby_city_and_year\
      .union(groupby_city)\
      .sort('city', 'year')
    #
    print("# groupby_with_union:")
    groupby_with_union.show()
    

    # Multi-dimensional aggregate operators are semantically 
    # equivalent to union operator (or SQL's UNION ALL) to 
    # combine single grouping queries.
    #
    # DataFrame.rollup(*cols)
    # Description:
    #    Create a multi-dimensional rollup for the current 
    #    DataFrame using the specified columns, so we can run 
    #    aggregation on them.
    #
    # pyspark.sql.functions.grouping_id(*cols)
    # Aggregate function: returns the level of grouping, equals to
    # (grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + … + grouping(cn)
    #
    with_rollup = sales.rollup('city', 'year')\
        .agg(sum('amount').alias('amount'), grouping_id().alias('gflg'))\
        .filter(col('gflg') != 3)
    with_rollup = with_rollup.sort(with_rollup.city.desc_nulls_last(), with_rollup.year.asc_nulls_last())\
        .select('city', 'year', 'amount', 'gflg')
    #
    print("# with_rollup:")
    with_rollup.show()
    with_rollup.printSchema()

    #===================
    # SQL only solution:
    #===================
    sales.createOrReplaceTempView("sales")
    #
    with_grouping_sets = spark.sql("""
       SELECT city, year, SUM(amount) as amount
       FROM sales
       GROUP BY city, year
       GROUPING SETS ((city, year), (city))
       ORDER BY city DESC NULLS LAST, year ASC NULLS LAST
       """)
    # NEW solution
    sales_SQL = spark.sql("""SELECT   city, year, sum(amount) as amount 
                                      ,GROUPING_ID(city, year) GFLG
                               FROM   sales_tbl
                           GROUP BY   ROLLUP(city, year)
                             HAVING   3 != GROUPING_ID(city, year) 
                           ORDER BY   city DESC NULLS LAST, year ASC NULLS LAST
                        """)
    #
    print("# with_grouping_sets:")
    with_grouping_sets.show()  
    print("# sales_SQL:")
    sales_SQL.show()

    # done!
    spark.stop()
