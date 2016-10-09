from mpi4py import MPI
import os
import sys
import numpy as np
import nltk
import time
import resource
import pickle
import matplotlib.pyplot as plt
from inverted_index import tokenize_list
from pymongo import MongoClient
from memory_profiler import profile
#mpi is array based
doc_namelist = []
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
M = comm.Get_size()
times = []
memory = []

# {word : { doc_name : [location_list] } }

def update_doclist(dict1, dict2, word1, word2, doclist):
	
	if not dict1 or not dict2: 
			return None
	modlist = []
	for doc in doclist:
		flag = 0
		if doc not in dict1.keys() or doc not in dict2.keys():
			continue
		for location in dict1[doc]:
			if location + len(word1) in dict2[doc]:
				modlist.append(doc)
	return modlist

#@profile
def search(query):
	client = MongoClient()
	db = client['inv_index' + str(rank)]
	
	#collection
	word_col = db.word_col
	
	# our final doclist
	try:
		docdict = word_col.find({"word": query[0]})
	except:
		docdict = []
	doclist = []
	for doc in docdict:
		doclist = doc['word_dict'].keys()

	if not query: return None

	for i, word in enumerate(query):
		if not doclist: break
		if i == 0: continue
		dictcursor1 = None
		dictcursor2 = None
		try:
			dictcursor1 = word_col.find({"word": query[i - 1]})
			dictcursor2 = word_col.find({"word": query[i]})
		except:
			pass
		dict1 = {}
		dict2 = {}
		for doc in dictcursor1:
			dict1 = doc.get('word_dict')
		for doc in dictcursor2:
			dict2 = doc.get('word_dict')
		doclist = update_doclist(dict1, dict2, query[i - 1], word, doclist)
	return doclist

if __name__ == "__main__":

	# uncomment this when need to show demo of one query...

	# phrase = tokenize_list(sys.argv[1])
	# print phrase
	# doc_namelist = search(phrase)
	# if rank == 0:
	# 	for i in range(1, M):
	# 		doc_list = comm.recv(source = i)
	# 		if doc_list is None: pass
	# 		elif doc_namelist is None: doc_namelist = doc_list
	# 		else: doc_namelist = list(set(doc_list + doc_namelist))
		# if doc_namelist is None:
		# 	print '[]'
		# else:
		# 	doc_namelist.sort()
		# 	print doc_namelist
	# else:
	# 	comm.send(doc_namelist, dest = 0)

	##########

	with open(sys.argv[1]) as f:
		content = f.readlines()

	prev_resource = 0
	for query in content:
		try:
			phrase = tokenize_list(query)		
			time_start = time.clock()
			#run your code
			doc_namelist = search(phrase)
			if rank == 0:
				for i in range(1, M):
					doc_list = comm.recv(source = i)
					if doc_list is None: pass
					elif doc_namelist is None: doc_namelist = doc_list
					else: doc_namelist = list(set(doc_list + doc_namelist))
				if doc_namelist is None:
					pass
					#print '[]'
				else:
					doc_namelist.sort()
					#print doc_namelist
			else:
				comm.send(doc_namelist, dest = 0)
			time_elapsed = (time.clock() - time_start)
			curr_resource = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
			if rank == 0:
				times.append(time_elapsed)
				memory.append(curr_resource)
			prev_resource = curr_resource
		except:
			pass
		if rank == 0:
			print query,
			print ' processed '
	if rank == 0:
		plt.plot(times)
		plt.show()
		plt.plot(memory)
		plt.show()

