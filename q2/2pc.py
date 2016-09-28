from mpi4py import MPI
import os
import sys
import time
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

bank = {}
message = []

# userid, debit/depo/view, amount, time, node_num, 
# failure stage, failed node no, restart stage, restart node no. 
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

		# stage 1.
		#TM or RM restarts before card insertion
		if int(lis[8]) == rank and int(lis[7]) == 1:
			time.sleep(1)
		if rank == int(lis[4]):

			if lis[1] == 'f':
				print "failure in node " + str(rank) + " , aborting..."
				comm.Abort()
			if i + 1 < past + 2 + num_queries:
				lis1 = content[i + 1].split()
				# if debitting and userid and timestamp are same.
				if lis[1] == 'd' and lis1[0] == lis[0]\
						 and lis1[3] == lis[3]:
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


			comm.send(message, dest = 0)
		
		# prepare TM		
		if rank == 0:
			message = comm.recv(source=MPI.ANY_SOURCE)
			if message[1] == 'v':
				print "balance of user " + message[0] + " " + str(bank[message[0]])
		# stage 2 restart.
		if int(lis[8]) == rank and int(lis[7]) == 2:
			time.sleep(1)

		# TM prepares other RMs.
		message = comm.bcast(message, root = 0)
 
		# stage 3 restart.
		if int(lis[8]) == rank and int(lis[7]) == 3:
			time.sleep(1)

		# all RM's tell TM agree/disagree.
		count = 1
		if message[1] != 'v' and message[2] == 0:
			count = 0
		if rank != 0:
			comm.send(count, dest = 0)

		if rank == 0:
			for j in range(1,size ):
				x = comm.recv(source = j)
				
				if x == 1:
					count = count + 1
				else:
					count = 0

		# stage 4 restart.
		if int(lis[8]) == rank and int(lis[7]) == 4:
			time.sleep(1)

		# TM shares no. of agree-ing RM's with all RM's
		a = comm.bcast(count, root = 0)
		if a == size:
			print "commiting at node :" ,rank 
			if message[1] == 'd':
				bank[message[0]] -= int(message[2])	
			if message[1] == 'c':
				bank[message[0]] += int(message[2])			
		else:
			print "aborting at node :" ,rank

		if message[3] == 0: i = i + 1
		else: i = i + 2

	print bank,
	print " " + str(rank)