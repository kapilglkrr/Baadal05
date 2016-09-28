from mpi4py import MPI
import os
import sys
import time
import random
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
val = False
balhistory = [-1]
bank = {}
message = []
alive = range(0, size)
leader = 0
def leader_failure():
	global leader, size, alive, rank
	if rank == leader:
		exit()
	alive.remove(leader)
	size = size - 1
	leader = alive[(leader + 1) % size]

def node_failure(node_num):
	global rank, alive
	if rank == node_num:
		exit()
	else:
		alive.remove(node_num)

def node_restart(node_num):
	global rank
	if node_num == rank:
		time.sleep(1)

def prepared(var):
	global alive, rank, leader
	if rank in alive and rank != leader:
		comm.send(var, dest = leader)

	commit = False
	if rank == leader:
		#check majority opinion of RMs
		count = 0
		for j in alive:
			if j == leader:
				continue
			var = comm.recv(source = j)
			if var:
				count = count + 1

		#if majority, then commit
		if count > (size-1)/2:
			#leader commits
			#print "Committing at node " + str(rank)
			commit = True

		#else, abort
		else:
			#print "Aborting at node ", rank
			commit = False
	return commit
# userid, debit/depo/view, amount, time, node_num, failure stage, failed node no
if __name__ == "__main__":
	with open(sys.argv[1]) as f:
		content = f.readlines()

	past = int(content[0])
	for i in range(1, past + 1):
		lis = content[i].split()
		bank[lis[0]] = int(lis[1])

	num_queries = int(content[past + 1])
	i = past + 2
	# loop all future queries
	while i < past + 2 + num_queries:
		flag = 0
		balhistory = [-1]
		lis = content[i].split()

		# stage 1.
		#leader fails before card insertion

		if lis[1] == 'f':
			print "Cluster failure. Aborting..."
			comm.Abort()
			
		
		if int(lis[5]) == 1 :
			if int(lis[6]) == leader:
				leader_failure()
			else:
				node_failure(int(lis[6]))

		if int(lis[7]) == 1 :
			node_restart(int(lis[8]))
		

		if rank == int(lis[4]):

			if i + 1 < past + 2 + num_queries:
				lis1 = content[i + 1].split()
				# if debitting and userid and timestamp are same.
				if lis[1] == 'd' and lis1[1] == 'd' and \
					lis1[0] == lis[0] and lis1[3] == lis[3]:
					val = int(lis[2]) + int(lis1[2])
					flag = 1
					if val <= bank[lis[0]]:
						data = val
					else:
						print 'aborting transaction, insufficient balance'
						data = 0
				else:
					data = int(lis[2])
					if data > bank[lis[0]]:
						print 'aborting transaction, insufficient balance'
						data = 0

			else:
				data = int(lis[2])
				if data > bank[lis[0]]:
					print 'aborting transaction, insufficient balance'
					data = 0
			message = []
			message.append(lis[0])
			message.append(lis[1])
			message.append(data)
			message.append(flag)

		if rank == int(lis[4]):
			#begincommit
			comm.send(message, dest = leader)

		#0: leader; 1-10: RM; no_fail_tolerable = 4
		#size = 11
		lis2 = []

		if rank == leader:
			message = comm.recv(source = int(lis[4]))

		# stage 2 failure.
		if int(lis[5]) == 2:
			if int(lis[6]) == leader:
				leader_failure()
				# repeat begin commit.
				if rank == int(lis[4]):
					comm.send(message, dest = leader)				
				if rank == leader:
					message = comm.recv(source = int(lis[4]))
			else:
				node_failure(int(lis[6]))
		if int(lis[7]) == 2 :
			node_restart(int(lis[8]))

		if rank == leader:
			lis2 = []
			lis2.append(message)
			bal = max(balhistory) + 1
			balhistory.append(bal)
			lis2.append(bal)

			#prepare
			for j in alive:	#RMs
				if j == leader:
					continue
				comm.send(lis2, dest = j)

		#acceptor
		var = False
		if rank in alive and rank != leader:
			lis2 = comm.recv(source = leader)
			#each RM chooses randomly whether to commit/abort
			var = True #bool(random.getrandbits(1))	#change???
			if lis2[1] >= max(balhistory):
				balhistory.append(lis2[1])
				#var = var and lis[0][]
			else:
				var = False

		# stage 3 failure
		if int(lis[5]) == 3:
			if int(lis[6]) == leader:
				leader_failure()
				lis2[1] = comm.bcast(lis2[1] + 1, root = leader)
				balhistory.append(lis2[1])
			else:
				node_failure(int(lis[6]))

		if int(lis[7]) == 3 :
			node_restart(int(lis[8]))
		# stage4 failure.
		commit = prepared(var)

		if int(lis[5]) == 4:
			if int(lis[6]) == leader:
				leader_failure()
				commit = prepared(var)
			else:
				node_failure(int(lis[6]))

		if int(lis[7]) == 4 :
			node_restart(int(lis[8]))
		if rank == leader:
			#send commit to RMs
			for j in alive:
				if j == leader:
					continue
				comm.send(commit, dest = j)

		#receive commit/abort from leader
		if rank in alive and rank != leader:
			commit = comm.recv(source = leader)
		
		if commit:
			print "Committing at node " + str(rank)
			if lis2[0][1] == 'd':
				bank[lis2[0][0]] -= int(lis2[0][2])	
			if lis2[0][1] == 'c':
				bank[lis2[0][0]] += int(lis2[0][2])
		else:
			print "Aborting at node ", rank

		if lis2[0][3] == 0: i = i + 1
		else: i = i + 2

	print bank,
	print " " + str(rank)