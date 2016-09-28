from mpi4py import MPI
import os
import sys
import time
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
can_fail = int((size - 1) / 3)
alive = range(0, size)
# userid, debit/depo/view, amount, time, node_num, failure stage
if __name__ == "__main__":

	# read info about traitors
	with open(sys.argv[2]) as f:
		content = f.readlines()
	for lines in content:
		traitor.append(int(lines))

	#read store.txt
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
		opinion = {}
		M = [None] * size
		M[rank] = bool(random.getrandbits(1))
		val = 0

		flag = 0
		lis = content[i].split()

		#failure before card insertion
		if lis[1] == 'f':
			if rank == int(lis[4]):
				print "failure in node " + str(rank) + " , aborting..."
				exit()
			if len(alive) < size - can_fail:
				print "greater than" + str(can_fail) + " nodes failed. Exiting gracefully :)"
				comm.Abort()
			alive.remove(int(lis[4]))
			i = i + 1
			continue

		if rank == int(lis[4]):

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

		#stage1 restart.
		if int(lis[8]) == rank and int(lis[7]) == 1:
			print 'node number ' + str(rank) + ' restarting...'
			time.sleep(1)
		alive.sort()
		for z in alive:
			if rank == z:
				for var in alive:
					if var == z: continue
					comm.send(M[z], dest = var)
			else:
				val = comm.recv(source = z)

			#val = comm.bcast(M[z], root = z)
			if z != rank:
				M[z] = val

		if rank in traitor:
			M = [True] * size
		if rank == int(lis[4]):	
			message.append(M)
		
		message = comm.bcast(message, root = int(lis[4]))
		message[-1] = M


		for l in range(0, len(traitor)):
			#stage l restart.
			if int(lis[8]) == rank and int(lis[7]) == l + 2:
				print 'node number ' + str(rank) + ' restarting...'
				time.sleep(1)

			if int(lis[5]) == l + 2:
				if len(alive) < size - can_fail:
					print "greater than " + str(can_fail) + " nodes failed. Exiting gracefully :)"
					comm.Abort()
				if int(lis[6]) == rank:
					print 'node number ' + str(rank) + ' failing...'
					exit()
				alive.remove(int(lis[6]))
			for x in alive:
				
				# general x, recv msgs from others
				if rank == x:
					for j in alive:
						if rank == j: continue
						lis2 = comm.recv(source = j)
						opinion[j] = lis2[-1]
					
					for k in alive:
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
						for a in range(0,size):
							M.append(bool(random.getrandbits(1)))
						message[-1] = M
					comm.send(message, dest = x)
		yes = 0
		no = 0
		for z in alive:
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
			print "commiting at node, rank = " + str(rank) 			
			if message[1] == 'd':
				bank[message[0]] -= int(message[2])	
			if message[1] == 'c':
				bank[message[0]] += int(message[2])
		else:
			print "Aborting at node, rank = " + str(rank)

		if message[3] == 0: i = i + 1
		else: i = i + 2
	
	print bank,
	print " " + str(rank)



			