import multiprocessing
import sys

import proc_runner
import snap_op as snap
from log import get_logger

logger = get_logger()

products = [
    'S2A_MSIL1C_20170202T090201_N0204_R007_T35SNA_20170202T090155.SAFE',
    'S2A_MSIL1C_20170617T012701_N0205_R074_T54SUF_20170617T013216.SAFE'
]

# Import your proces or paste it here
def MyProc(meta, params):
    return "About to process %s with parameters %s" % (str(meta), str(params))


def main(jobs, indices_expr, s3conf):
    logger.info("%d cpu available" % multiprocessing.cpu_count())

    for job in jobs:
        prod, proc_func, tasks = job
        map_arg = {'product': prod,
                   'tasks': tasks,
                   'indices_expr': indices_expr}
        proc_runner.main(proc_func, map_arg, s3conf)


if __name__ == '__main__':
    indices_expr = {'ndvi': '(B7 + B4) != 0 ? (B7 - B4) / (B7 + B4) : -2',
                    'ndi45': '(B5 + B4) != 0 ? (B5 - B4) / (B5 + B4) : -2',
                    'gndvi': '(B7 + B3) != 0 ? (B7 - B3) / (B7 + B3) : -2'}
    task1 = {
        'bands': ['B04', 'B07'],
        'index': 'ndvi'
    }
    task2 = {
        'bands': ['B05', 'B04'],
        'index': 'ndi45'
    }
    task3 = {
        'bands': ['B03', 'B07'],
        'index': 'gndvi'
    }

    job1 = [products[0], snap.main, [task1]]
    job2 = [products[0], snap.main, [task1, task2]]
    job3 = [products[0], snap.main, [task1, task2, task3]]
    endpoint_url = sys.argv[1]
    bucket_id = sys.argv[2]
    s3conf = {'endpoint_url': endpoint_url,
              'bucket_id': bucket_id}
    main([job1], indices_expr, s3conf)
    logger.info('success.')
