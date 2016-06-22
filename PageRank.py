from pyspark.context import SparkContext
import re, Constants
links = SparkContext().textFile("Simulation.txt").map(lambda x: tuple(re.split(r'\s+', x)[:2])).distinct().filter(lambda x: x[0] != x[1]).cache()
links = links.map(lambda x: (x[1], x[0])).join(links.flatMap(lambda x: (x[0], x[1])).distinct().subtract(links.map(lambda x: x[0])).map(lambda x: (x, None))).map(lambda x: (x[0], x[1][0])).union(links).groupByKey().cache()
ranks = links.map(lambda x: (x[0], Constants.PR_init)).cache()
for _ in range(Constants.iterations): ranks = links.join(ranks).flatMap(lambda x: ((i, x[1][1]/len(x[1][0])) for i in x[1][0])).reduceByKey(lambda x, y: x + y).mapValues(lambda x: x * Constants.d + Constants.c).cache()
print ranks.map(lambda x: x[1]).reduce(lambda x, y: x + y)