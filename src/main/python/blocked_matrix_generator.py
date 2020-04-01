import random
import sys
import time
import argparse


parser = argparse.ArgumentParser(description='generate blocked matrix')
parser.add_argument('numRows',  metavar='numRows', type=int, default=1000000, help='number of rows')
parser.add_argument('numCols', metavar='numCols', type=int, default=1000, help='number of columns')
parser.add_argument('blockRowSize', metavar='blockRowSize', type=int, default=1000, help='number of rows in a block')
parser.add_argument('blockColSize', metavar='blockColSize', type=int, default=1000, help='number of columns in a block')
parser.add_argument('fileName', metavar='fileName', type=str, default='data.csv', help='name of output file')
args = parser.parse_args()

#num of rows
dimX = args.numRows
#num of cols
dimY = args.numCols
blockRowSize = args.blockRowSize
blockColSize = args.blockColSize
fileName = args.fileName


assert dimX%blockRowSize == 0 and dimY%blockColSize == 0

blockRowNum = dimX / blockRowSize
blockColNum = dimY / blockColSize


blocks = open(fileName, "w")

for i in xrange(dimX/blockRowSize):
    for j in xrange(dimY/blockColSize):
        #block row_id
        blocks.write(str(i))
        blocks.write(" ")
        #block col_id
        blocks.write(str(j))
        blocks.write(" ")
        #block data
        #Step1: create block data in memory
        block = []
        ii = 0
        jj = 0
        for ii in xrange(blockRowSize):
             row = []
             for jj in xrange(blockColSize):
                 row.append(random.random())
             block.append(row)
        #Step2: write block data to file
        ii = 0
        jj = 0
        for ii in xrange(blockRowSize):
            for jj in xrange(blockColSize):
                 blocks.write(str(block[ii][jj]))
                 blocks.write(" ")
        blocks.write("\n")
        print("written block <" + str(i) + ", " + str(j) +">\n")
blocks.close()
