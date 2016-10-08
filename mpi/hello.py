from mpi4py import MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
num_nodes = comm.Get_size()

print rank