from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TestWeather")
sc = SparkContext(conf = conf)

def parseLine(line):
    data = line.split(",")
    stationId = data[0]
    entryType = data[2]
    temperature = float(data[3])
    return (stationId, entryType, temperature)


data = sc.textFile("/home/taferh/work/spark/1800.csv")
parsedData = data.map(parseLine)
minTemps = parsedData.filter(lambda x: "TMAX" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = minTemps.collect()
for result in results:
    print(result)