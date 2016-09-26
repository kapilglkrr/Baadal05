from mpi4py import MPI
import os
import sys
import numpy as np
import nltk
import math
import pickle
from pymongo import MongoClient
data = []
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
num_nodes = comm.Get_size()
# {word : { doc_name : [location_list] } }

# inverted index.
index = {}

def send_filenames(src):
	files = []
	for root, dirnames, filenames in os.walk(src):
		files = filenames
	files.sort()
	m = int(math.ceil(float(len(files)) / float(num_nodes) ))
	sent = 0
	for i in range(1,  num_nodes):
		comm.send(files[sent: sent + m], dest = i)
		sent += m
	return files[sent:]

# this function returns a list of tokenized and stemmed words of any text
def tokenize_list(doc_text):
    tokens = nltk.tokenize.RegexpTokenizer(r'\w+').tokenize(doc_text)
	# filtered_words = [word for word in tokens if word not in nltk.corpus.stopwords.words('english')]
    tokens = [i.lower() for i in tokens]
    tokens = [i.decode('utf-8', 'ignore').encode("utf-8") for i in tokens]
    porter = nltk.stem.PorterStemmer()
    return [porter.stem(i) for i in tokens]


def store_db():
	client = MongoClient()
	db = client['inv_index' + str(rank)]
	word_col = db.word_col
	for key, value in index.iteritems():
		post = {'word': key, 'word_dict': value}
		word_col.insert_one(post)

# for normal dataset..
# process files and store data in inverted index.
def process_files(src, files):
	for file in files:
		doc_name = src + '/' + file
		words = tokenize_list(open(doc_name).read())
		fptr = 0
		for word in words:
			word_dict = index[word] if word in index else {}
			location_list = word_dict[file] if file in word_dict else []
			location_list.append(fptr)
			word_dict[file] = location_list
			index[word] = word_dict
			fptr += len(word)

	store_db()

# with preprocessing.
# def process_files(src, files):
# 	for file in files:
# 		filename = src + '/' + file
# 		words = tokenize_list(open(filename).read())
# 		# gives the index of the current word.
# 		start = 0
# 		end = 0
# 		doc_name = ""
# 		fptr = 0
# 		for word in words:
# 			if start < 2:
# 				start += 1
# 				continue
# 			if start == 2:
# 				doc_name = word
# 				start += 1
# 				fptr = 0
# 				continue
# 			if start > 2 and start <= 10:
# 				start += 1
# 				continue

# 			if word == 'endofarticl':
# 				end = end + 1
# 				continue
# 			if end != 0:
# 				end = 0
# 				start = 0
# 				continue

# 			word_dict = index[word] if word in index else {}
# 			location_list = word_dict[doc_name] if doc_name in word_dict else []
# 			location_list.append(fptr)
# 			word_dict[doc_name] = location_list
# 			index[word] = word_dict
# 			fptr += len(word)
# 	store_db()



if __name__ == "__main__":
	files = []
	if (len(sys.argv) < 2): 
		print "pl. provide source folder"
		sys.exit(1)
	if rank == 0:
		files = send_filenames(sys.argv[1])
	else:
		files = comm.recv(source = 0)

	process_files(sys.argv[1], files)

