import random
import argparse

parser = argparse.ArgumentParser(description='generate pagerank data')
parser.add_argument('numPages', metavar='numPages', type=int, default=1000000,
                    help='number of pages')
parser.add_argument('maxLinks', metavar='maxLinks', type=int, default=1000,
                    help='maximum number of links')
parser.add_argument('fileName', metavar='fileName', type=str,
                    default='data.csv', help='name of output file')
args = parser.parse_args()

# num of pages
numPages = args.numPages
# maximum num of links
maxLinks = args.maxLinks
fileName = args.fileName

assert numPages > maxLinks > 0

print(str(numPages) + " with max " + str(maxLinks) + "\n")

pages = open(fileName, "w")

for i in range(numPages):
    numLinks = random.randint(0, maxLinks)
    links = random.sample(range(0, numPages - 1), numLinks)
    for j in links:
        # page id
        pages.write(str(i))
        pages.write(" ")
        # link id
        pages.write(str(j))
        pages.write("\n")

pages.close()
