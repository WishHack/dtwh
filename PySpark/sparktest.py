# =============================================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# About this Code:	checking Spark/PySpark
# =============================================================================================
# Version 1: creating the code
# =============================================================================================

from pyspark import SparkContext
sc = SparkContext("local","simple app")

a = [1,4,3,5]
a = sc.parallelize(a)
print a
print a.take(2)