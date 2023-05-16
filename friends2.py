from pyspark import SparkConf, SparkContext

# set up a rdd from reading a file locally with spark

conf = SparkConf().setMaster("local").setAppName("Test")
sc = SparkContext(conf=conf)

def parseLine(line):
    data = line.split(",")
    age = int(data[2])
    friends = int(data[3])
    return age, friends
#read file with sc.textFile and map it to parseLine function
lines  = sc.textFile("/home/taferh/work/spark/fakefriends.csv")
rdd = lines.map(parseLine)
#mapValues to add 1 to each value and 1 to each key
totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
avergageByAge =  totalsByAge.mapValues(lambda x: x[0]/x[1]).sortBy(lambda x: x[0]).filter(lambda x: x[1] > 30)
results = avergageByAge.collect()
for result in results:
    print(result)