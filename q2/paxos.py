from mpi4py import MPI
import os
import sys
import random
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


leader = 0
val = 0
balhistory = [-1]
opinions = {}
no_acceptors = 5
if __name__ == '__main__':

	#begincommit
	if rank == 1:
		val = 100
		comm.send(val, dest = leader)

	#1-5: RM; 6-10 Acceptors; fail = 2
	#size = 11
	lis = []

	print "begin commit done ", 
	print rank

	if rank == leader:
		val = comm.recv(source = 1)

		for j in range(1, 6):
			lis = []
			lis.append(val)
			bal = max(balhistory) + 1
			balhistory.append(bal)
			lis.append(bal)
			comm.send(lis, dest = j)		


	if rank in range(1, 6):
		lis = comm.recv(source = leader)
		balhistory.append(lis[1])
		for j in range(6, 11):
			comm.send(lis, dest = j)

	#acceptor
	if rank in range(6, 11):
		for j in range(1, 6):
			opinions[j] = comm.recv(source = j)
			balhistory.append(opinions[j][1])
			
		set(balhistory)
		var = True
		for j in range(1, 6):
			if opinions[j][1] >= max(balhistory):
				var = var and opinions[j][0]

		comm.send(var, dest = leader)

	if rank == leader:
		count = 0
		for j in range(6, 11):
			var = comm.recv(source = j)
			if var:
				count = count + 1

		if count > no_acceptors/2:
			for j in range(1, 6):
				comm.send(True, dest = j)

	if rank in range(1, 6):
		commit = comm.recv(source = leader)
		if commit:
			print "Committing at node " + str(rank)
		else:
			print "Aborting at node ", rank
