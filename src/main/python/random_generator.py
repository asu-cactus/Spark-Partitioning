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
    density_frac = float(args[5])

    with open("left_matrix.txt", "w") as text_file:
        for row_index in range(0, r1):
            sl = random(1, c1, density=density_frac, random_state=rs, data_rvs=rvs)
            for x in range(sl.row.size):
                text_file.write(
                    str(row_index) + ',' + str(sl.col[x]) + ',' + str(
                        sl.data[x]) + "\n")

    with open("right_matrix.txt", "w") as text_file:
        for col_index in range(0, c2):
            sr = random(r2, 1, density=density_frac, random_state=rs, data_rvs=rvs)
            for x in range(sr.row.size):
                text_file.write(
                    str(sr.row[x]) + ',' + str(col_index) + ',' + str(
                        sr.data[x]) + "\n")


if __name__ == "__main__":
    main()
