import sys

from scipy.sparse import random
from scipy import stats
import numpy as np


class CustomRandomState(np.random.RandomState):
    def randint(self, k, **kwargs):
        i = np.random.randint(k)
        return i - i % 2


np.random.seed(12345)
rs = CustomRandomState()
rvs = stats.poisson(25, loc=10).rvs


def main():
    args = sys.argv
    r1 = int(args[1])
    c1 = int(args[2])
    r2 = int(args[3])
    c2 = int(args[4])

    sl = random(r1, c1, density=1.0, random_state=rs, data_rvs=rvs)

    with open("left_matrix.txt", "w") as text_file:
        for x in range(sl.row.size):
            text_file.write(
                str(sl.row[x]) + ',' + str(sl.col[x]) + ',' + str(
                    sl.data[x]) + "\n")

    sr = random(r2, c2, density=1.0, random_state=rs, data_rvs=rvs)

    with open("right_matrix.txt", "w") as text_file:
        for x in range(sr.row.size):
            text_file.write(
                str(sr.row[x]) + ',' + str(sr.col[x]) + ',' + str(
                    sr.data[x]) + "\n")


if __name__ == "__main__":
    main()
