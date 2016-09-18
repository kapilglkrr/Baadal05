from mpi4py import MPI
import os
import sys
import numpy as np
import nltk
import math
import pickle
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
    porter = nltk.stem.PorterStemmer()
    return [porter.stem(i) for i in tokens]

# save/load pickle modules...
'''
def save_obj(obj, name ):
    with open('obj/'+ name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open('obj/' + name + '.pkl', 'rb') as f:
        return pickle.load(f)
'''

# process files and store data in inverted index.
def process_files(src, files):
	for file in files:
		doc_name = src + '/' + file
		words = tokenize_list(open(doc_name).read())
		# if file == 'doc0000':
		# 	print words
		# gives the index of the current word.
		fptr = 0
		for word in words:
			word_dict = index[word] if word in index else {}
			location_list = word_dict[file] if file in word_dict else []
			location_list.append(fptr)
			word_dict[file] = location_list
			index[word] = word_dict
			fptr += len(word)

	with open("index" + str(rank), "wb") as filename:
		pickle.dump(index, filename)


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

