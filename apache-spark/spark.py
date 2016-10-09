import sys
import time
from pyspark import SparkContext
import matplotlib.pyplot as plt

times=[]
x=[i for i in range(1,1001)]
def search(search_corpus,query):
	sc = SparkContext(appName="Inverted Index")
	sc.setLogLevel("WARN")
	start_time=time.time()
	#Building the inverted index
	inverted_index = sc.wholeTextFiles(search_corpus)\
		.flatMap(lambda (title, content): map(lambda word: (word, title), content.split()))\
		.map(lambda (word, title): ((word, title), 1))\
		.reduceByKey(lambda count1, count2: count1 + count2)\
		.map(lambda ((word, title), count): (word, (title, count)))\
		.groupByKey()

	end_time=time.time()

	print "Time to build inverted index is " + str(end_time-start_time)

	for q in query:

		start_time=time.time()
	# # query the result from inverted index
		result_rdd = inverted_index.filter(lambda (word, title_count_list): q)\
			.flatMap(lambda (word, title_count_list): title_count_list)\
			.reduceByKey(lambda count1, count2: count1 + count2)\
			.sortBy((lambda (title, count): count), False)\
			.map(lambda (title, count): title + " has number of hits: " + `count` + " for " +q)

		end_time=time.time()
		print "Time for " + q + " query is " + str(end_time-start_time)
		times.append(end_time-start_time)
	
	#print the result
		for line in result_rdd.collect():
			print line

	plt.plot(x,times, 'ro')
	plt.show()
	sc.stop()
	
if __name__ == "__main__":
	search_corpus=sys.argv[1]
	query=sys.argv[2]
	query = [line.rstrip('\n') for line in open(query)]
	search(search_corpus,query)