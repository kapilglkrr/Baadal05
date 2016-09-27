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

	#0: leader; 1-10: RM; no_fail_tolerable = 4
	#size = 11
	lis = []

	print "begin commit done ", 
	print rank

	if rank == leader:
		val = comm.recv(source = 1)

		#prepare
		for j in range(1, size):	#RMs
			lis = []
			lis.append(val)
			bal = max(balhistory) + 1
			balhistory.append(bal)
			lis.append(bal)
			comm.send(lis, dest = j)		


	#acceptor
	if rank in range(1, size):
		lis = comm.recv(source = leader)
		var = True
		if lis[1] >= max(balhistory):
			balhistory.append(lis[1])
			var = var and lis[0]

		comm.send(var, dest = leader)

	if rank == leader:
		#check majority opinion of RMs
		count = 0
		for j in range(1, size):
			var = comm.recv(source = j)
			if var:
				count = count + 1

		#if majority, then commit
		if count > (size-1)/2:
			#send commit to RMs
			for j in range(1, size):
				comm.send(True, dest = j)

			#leader commits
			print "Committing at node " + str(rank)

		#else, abort
		else:
			for j in range(1, size):
				comm.send(False, dest = j)

			print "Aborting at node ", rank

	#receive commit/abort from leader
	if rank in range(1, size):
		commit = comm.recv(source = leader)
		if commit:
			print "Committing at node " + str(rank)
		else:
			print "Aborting at node ", rank
