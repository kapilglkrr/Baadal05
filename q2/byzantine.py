from mpi4py import MPI
import os
import sys
import random
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

opinion = {}
message = []
M = [None] * size
M[rank] = bool(random.getrandbits(1))
val = 0
traitor = [1, 3, 5]
# no of traitors
m = int(sys.argv[1])

if __name__ == "__main__":
	
	for i in range(0, size):

		val = comm.bcast(M[i], root = i)
		if i != rank:
			M[i] = val

	if rank == 1 or rank == 3 or rank == 5:
		M = [1] * 10

	for l in range(1, m):
		for i in range(0, size):
			
			# general i, recv msgs from others
			if rank == i:
				for j in range(0, size):
					if rank == j: continue
					lis = comm.recv(source = j)
					opinion[j] = lis
				
				
				for k in range(0, size):
					yes = 0
					no = 0
					for opinions in opinion.values():
						if opinions[k]:
							yes = yes + 1
						else:
							no = no + 1
					if yes > no:
						M[k] = True
					else:
						M[k] = False
			# send M to general i
			else:
				if rank in traitor:
					M = []
					for a in range(0, size):
						M.append(bool(random.getrandbits(1)))
				comm.send(M, dest = i)

	yes = 0
	no = 0
	for i in range(0, size):
		if M[i]:
			yes = yes + 1
		else:
			no = no + 1
	if yes > no:
		print str(rank) + " says True"
	else:
		print str(rank) + " says False"
	
	if rank == 0:
		print traitor


