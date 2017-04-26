import sys

def main(argv):
    if len(argv) < 3:
        print "python reduce.py <target d> <filename> <outname>"
        return
    dimension = int(argv[1])
    filename = argv[2]
    outname = argv[3]
    with open(filename, 'r') as input_file:
        with open(outname, 'w') as output_file:
            for line in input_file:
                entries = line.rstrip().split(',')[0:dimension]
                output_file.write("%s\r\n" % ','.join(entries))

if __name__ == '__main__':
    main(sys.argv)