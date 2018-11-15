import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

	conf1 = SparkConf().setAppName("Spark Count")
	sc = SparkContext(conf=conf1)
	baseRDD = sc.textFile("file:///home/notroot/lab/data/sample")
	splitRDD = baseRDD.flatMap( lambda line : line.split(" "))
	mappedRDD = splitRDD.map (lambda line: (line,1))
	reducedRDD = mappedRDD.reduceByKey (lambda a,b : a+b)
	rdd2 = reducedRDD.coalesce(1)
	rdd2.saveAsTextFile("file:///home/notroot/lab/data/results1")
	sc.stop()
