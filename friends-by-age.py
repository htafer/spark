

from pyspark import SparkConf, SparkContext

#set up a rdd from reading a file locally with spark

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)        

def parseLine(line):
    data = line.split(",") 
    age = int(data[2])
    friends = int(data[3])
    return age, friends


lines = sc.textFile("/home/taferh/work/spark/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
averageByAge = totalsByAge.mapValues(lambda x: x[0]/x[1]).sortBy(lambda x: x[1], ascending=False)
results = averageByAge.collect()
for result in results:
    print(result)


#lines = sc.textFile("/home/taferh/work/spark/fakefriends.csv")
#rdd = lines.map(parseLine)
#totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
#results = averagesByAge.collect()
#for result in results:
#    print(result)
