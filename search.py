from mpi4py import MPI
import os
import sys
import numpy as np
import nltk
import pickle
from inverted_index import tokenize_list
#mpi is array based
doc_namelist = []
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
M = comm.Get_size()

# {word : { doc_name : [location_list] } }
index = {}

def update_doclist(dict1, dict2, word1, word2, doclist):
	if dict1 is None or dict2 is None: 
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

def search(query):
	# our final doclist
	doclist = []
	doclist = index[query[0]].keys() if query[0] in index else None
	for i, word in enumerate(query):
		if doclist is None: break
		if i == 0: continue
		dict1 = index[query[i - 1]] if query[i - 1] in index else None
		dict2 = index[word] if word in index else None
		doclist = update_doclist(dict1, dict2, query[i - 1], word, doclist)
	return doclist

if __name__ == "__main__":
	with open('index' + str(rank), 'rb') as f:
		index = pickle.load(f)
	phrase = tokenize_list(sys.argv[1])
	doc_namelist = search(phrase)
	if rank == 0:
		for i in range(1, M):
			doc_list = comm.recv(source = i)
			if doc_list is None: pass
			elif doc_namelist is None: doc_namelist = doc_list
			else: doc_namelist = list(set(doc_list + doc_namelist))
		doc_namelist.sort()
		print doc_namelist
	else:
		comm.send(doc_namelist, dest = 0)


