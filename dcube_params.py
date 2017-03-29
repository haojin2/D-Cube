# The port to use
PGPORT = 15000

# Flag to control bucket type, 1 for bucket by day, 0 for bucket by hour
BUCKET_FLAG = 1

# List of columns for DARPA TCP DUMP data
columns = ['src', 'dest', 'bucket']

# Flag for whether to binarize the count, 1 for binarize, 0 for not
BINARY_FLAG = 1
