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
can_fail = int((size - 1) / 2)
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
		barrier = 0
		barrier = comm.bcast(barrier, root = 0)
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
			if len(alive) <= can_fail + 1:
				print "greater than F nodes failed"
				comm.Abort()
			alive.remove(int(lis[4]))
			i = i + 1
			print "rank = " + str(rank) + " continuing..."
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
		print 'i = ' + str(i) + ' rank = ' + str(rank)
		print lis
		alive.sort()
		print alive,
		print rank

		for z in range(0, len(alive)):
			print M[alive[z]]
			val = comm.bcast(M[alive[z]], root = alive[z])
			print 'broadcast ho gaya ' + str(alive[z]) + 'i = ' + str(i)
			print 'val = ' + str(val)
			if alive[z] != rank:
				M[alive[z]] = val
				print 'z = ' + str(alive[z])
				print M,
				print rank

		print 'i = ' + str(i) + ' first broadcast ' + str(rank)
		print lis
		if rank in traitor:
			M = [1] * 10

		if rank == int(lis[4]):	
			message.append(M)
		
		message = comm.bcast(message, root = int(lis[4]))
		print 'i = ' + str(i) + ' second broadcast ' + str(rank)
		message[-1] = M

		for l in traitor:
			for x in alive:
				
				# general i, recv msgs from others
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
						for a in alive:
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
			print "commiting at node, rank =" + str(rank) + ' i = ' + str(i) 			
			if message[1] == 'd':
				bank[message[0]] -= int(message[2])	
			if message[1] == 'c':
				bank[message[0]] += int(message[2])
		else:
			print "Aborting at node, rank =" + str(rank) + ' i = ' + str(i)


		if message[3] == 0: i = i + 1
		else: i = i + 2
	
	print bank,
	print " " + str(rank)



			