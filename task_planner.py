from multiprocessing.pool import Pool
from multiprocessing import Process
import proc_runner
import multiprocessing
import snap_op as snap
import sys
import io
import os
import time


bucket = 'sixsq.eoproc'

products = ['S2A_OPER_PRD_MSIL1C_PDMC_20151230T202002_R008_V20151230T105153_20151230T105153.SAFE',
            'S2A_MSIL1C_20170202T090201_N0204_R007_T35SNA_20170202T090155.SAFE',
            'S2A_MSIL1C_20170617T012701_N0205_R074_T54SUF_20170617T013216.SAFE']


meta_file_dict = {'S2A_MTD': 'MTD_MSIL1C.xml'}


# Import your proces or paste it here
def MyProc(meta, params):
    return("About to process %s with parameters %s" % (str(meta), str(params)))


def main(jobs):
    print "%d cpu available" % multiprocessing.cpu_count()

    for job in jobs:
        prod, proc_func, tasks = job
        proc_runner.main(proc_func, [prod, tasks])


if __name__ == '__main__':

    task1 = {
        'bands': ['B04', 'B07'],
        'params': ['ndvi']
    }
    task2 = {
        'bands': ['B05', 'B04'],
        'params': ['ndi45']
    }
    task3 = {
        'bands': ['B03', 'B07'],
        'params': ['gndvi']
    }

    tasks0 = [task1, task2, task3]
    tasks1 = [task1, task2]

    job0 = [products[0], snap.main, tasks0]
    job1 = [products[1], snap.main, tasks1]
    #main([job0, job1])
    main([job0])
