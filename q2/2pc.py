from mpi4py import MPI
import os
import sys

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

bank = {}
message = []
if __name__ == "__main__":
	with open(sys.argv[1]) as f:
		content = f.readlines()

	past = int(content[0])
	for i in range(1, past + 1):
		lis = content[i].split()
		bank[lis[0]] = int(lis[1])

	num_queries = int(content[past + 1])
	print "num_queries = " + str(num_queries)
	i = past + 2
	# loop all future queries
	while i < past + 2 + num_queries:
		flag = 0
		lis = content[i].split()
		if rank == int(lis[4]):
			if i + 1 < past + 2 + num_queries:
				lis1 = content[i + 1].split()
				if lis[1] == 'f':
					comm.Abort(errorcode = 420)
				# if debitting and userid and timestamp are same.
				if lis[1] == 'd' and lis1[0] == lis[0] and lis1[3] == lis[3]:
					val = int(lis[2]) + int(lis1[2])
					flag = 1
					if val <= bank[lis[0]]:
						data = val
					else:
						print 'aborting transaction, insufficient balance'
						data = 0
				else:
					data = int(lis[2])

			else:
				data = int(lis[2])
			message = []
			message.append(lis[0])
			message.append(lis[1])
			message.append(data)
			message.append(flag)
			comm.send(message, dest = 0)
							
		if rank == 0:
			message = comm.recv(source=MPI.ANY_SOURCE)
			if message[1] == 'v':
				print "balance of user " + message[0] + " " + str(bank[message[0]])
		message = comm.bcast(message, root = 0)

		if rank != 0:
			comm.send(message, dest = 0) 
		count = 1
		if rank == 0:
			for j in range(1,size ):
				x = comm.recv(source = j)
				
				if x == message:
					count = count + 1
				else:
					count = 0
		a = comm.bcast(count, root = 0)
		if a == size:
			print "commiting at node :" ,rank 
			if message[1] == 'd':
				bank[message[0]] -= int(message[2])	
			if message[0] == 'c':
				bank[message[0]] += int(message[2])
			print str(bank[message[0]])			
		else:
			print "aborting at node :" ,rank

		print str(message[3]) + " rank " + str(rank)
		if message[3] == 0: i = i + 1
		else: i = i + 2
	print "program khatam " + str(rank)