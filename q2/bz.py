from mpi4py import MPI
import os
import sys
import random
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

bank = {}
message = []
opinion = {}
M = [None] * size
M[rank] = bool(random.getrandbits(1))
val = 0
traitor = []
content = []
# userid, debit/depo/view, amount, time, node_num, failure stage
if __name__ == "__main__":

	# read info about traitors
	with open(sys.argv[2]) as f:
		content = f.readlines()
	for lines in content:
		traitor.append(int(lines))

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
		
		M = [None] * size
		M[rank] = bool(random.getrandbits(1))
		val = 0

		flag = 0
		lis = content[i].split()
		#print rank
		if rank == int(lis[4]):

			if lis[1] == 'f':
				print "failure in node " + str(rank) + " , aborting..."
				comm.Abort()
			if i + 1 < past + 2 + num_queries:
				lis1 = content[i + 1].split()
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

		for z in range(0, size):
			val = comm.bcast(M[z], root = z)
			if z != rank:
				M[z] = val

		if rank in traitor:
			M = [1] * 10

		if rank == int(lis[4]):	
			message.append(M)
		
		message = comm.bcast(message, root = int(lis[4]))
		message[-1] = M

		for l in range(1, len(traitor)):
			for x in range(0, size):
				
				# general i, recv msgs from others
				if rank == x:
					for j in range(0, size):
						if rank == j: continue
						lis2 = comm.recv(source = j)
						opinion[j] = lis2[-1]
					
					
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
				# send M to general x
				else:
					if rank in traitor:
						M = []
						for a in range(0, size):
							M.append(bool(random.getrandbits(1)))
						message[-1] = M
					comm.send(message, dest = x)
		yes = 0
		no = 0
		for z in range(0, size):
			if M[z]:
				yes = yes + 1
			else:
				no = no + 1

		msg = []
		if rank == int(lis[4]):
			loopvar = 0
			while loopvar < size:
				if loopvar not in traitor:
					break
			msg.append(yes)
			msg.append(no)

		msg = comm.bcast(msg, root = int(lis[4]))
		yes = msg[0]
		no = msg[1]
		if yes > no:
			print "commiting at node :" ,rank 			
			if message[1] == 'd':
				bank[message[0]] -= int(message[2])	
			if message[1] == 'c':
				bank[message[0]] += int(message[2])
		else:
			print "Aborting at node ",rank

		if message[3] == 0: i = i + 1
		else: i = i + 2
	
	print bank,
	print " " + str(rank)



			