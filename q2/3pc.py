from mpi4py import MPI
import os
import sys
import random
import time
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

bank = {}
message = []
leader = 0
alive = range(0, size)

def get_status(lis, restart):
	global rank, alive, leader
	# all RM's tell TM agree/disagree to prepare.
	count = 1
	if message[1] != 'v' and message[2] == 0:
		count = 0
	if rank != leader:
		comm.send(count, dest = leader)

	if rank == leader:
		for j in alive:
			if j == leader: continue
			x = comm.recv(source = j)
			
			if x == 1:
				count = count + 1
			else:
				count = 0
	# stage 4 restart.
	if int(lis[8]) == rank and ((int(lis[7]) == 4 and restart == 1) or (int(lis[7]) == 6 and restart == 3)):
		print 'node number ' + str(rank) + ' restarting...'
		time.sleep(1)
	# send no. of agreeing processes
	# so that everyone aborts if they want to
	
	count = comm.bcast(count, root = leader)
	# stage 5 restart.
	if int(lis[8]) == rank and int(lis[7]) == 5 and restart == 1:
		print 'node number ' + str(rank) + ' restarting...'
		time.sleep(1)
	return count

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
		lis = content[i].split()
		# stage 1 restart.
		if int(lis[8]) == rank and int(lis[7]) == 1:
			print 'node number ' + str(rank) + ' restarting...'
			time.sleep(1)
		if rank == int(lis[4]):

			if lis[1] == 'f':
				print "failure in node " + str(rank) + " , aborting..."
				comm.Abort()

			if i + 1 < past + 2 + num_queries:
				lis1 = content[i + 1].split()
				# if debitting and userid and timestamp are same.
				if lis[1] == 'd' and lis1[1] == 'd' and lis1[0] == lis[0] and lis1[3] == lis[3]:
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
			message.append(lis[5])
			comm.send(message, dest = leader)


		# stage 1
		# TM prepared.	
		if rank == leader:
			message = comm.recv(source=MPI.ANY_SOURCE)
			if message[1] == 'v':
				print "balance of user " + message[0] + " " + str(bank[message[0]])
		
		# stage 2 restart.
		if int(lis[8]) == rank and int(lis[7]) == 2:
			print 'node number ' + str(rank) + ' restarting...'
			time.sleep(1)
		# TM prepares all RM's.
		message = comm.bcast(message, root = leader)

		# stage 3 restart.
		if int(lis[8]) == rank and int(lis[7]) == 3:
			print 'node number ' + str(rank) + ' restarting...'
			time.sleep(1)
		count = get_status(lis, 1)
		
		# stage 1 end
		#print message,
		#print str(i) + ' ' + str(rank)
		# TM fails...
		if int(message[4]) == 2:
			alive.remove(leader)
			if rank == leader:
				sys.exit(0)
			# elect new TM.
			leader = alive[(leader + 1) % len(alive)]
			#leader = random.choice(alive)
			# leader = comm.bcast(leader, root = leader)
			count = get_status(lis, 2)
		if count != len(alive) or message[4] == 1:
			print "Aborting at node ",rank
			if message[3] == 0: i = i + 1
			else: i = i + 2
			continue
		
		## pre commit phase
		# send data to all nodes (prepare to commit)
		message = comm.bcast(message, root = leader)
		# recieved ack from RM's
		count = get_status(lis, 3)		
		if count != len(alive) or message[4] == 1:
			print "Aborting at node ",rank
		# commit..
		else:
			print "commiting at node :" ,rank 
			if message[1] == 'd':
				bank[message[0]] -= int(message[2])	
			if message[1] == 'c':
				bank[message[0]] += int(message[2])

		if message[3] == 0: i = i + 1
		else: i = i + 2

	print bank,
	print " " + str(rank)


		



			

