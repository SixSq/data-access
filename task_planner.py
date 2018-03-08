import sys
import multiprocessing

import proc_runner
import snap_op as snap
from log import get_logger
import NoDaemonProcess as ndp
from multiprocessing.pool import Pool

logger = get_logger()

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

tasks_map = {'ndvi': task1,
             'ndi45': task2,
             'gndvi': task3}


# def main2(jobs, indices_expr, s3conf):
#     logger.info("%d cpu available" % multiprocessing.cpu_count())
#
#     for job in jobs:
#         prod, proc_func, tasks = job
#         map_arg = {'product': prod,
#                    'tasks': tasks,
#                    'indices_expr': indices_expr}
#         logger.info('will run... %s', map_arg)
#         proc_runner.main(proc_func, map_arg, s3conf)

def main(jobs, indices_expr, s3conf):
    logger.info("%d cpu available" % multiprocessing.cpu_count())

    nbproc = len(jobs)
    pool = ndp.MyPool(processes=nbproc)
    # pool = Pool(processes=nbproc)

    for job in jobs:
        prod, proc_func, tasks = job
        map_arg = {'product': prod,
                   'tasks': tasks,
                   'indices_expr': indices_expr}
        logger.info('work on job: %s', map_arg)
        pool.apply_async(proc_runner.main,
                         args=(proc_func, map_arg, s3conf),
                         callback=lambda x: logger.info("SUCCESS job: %s", x))
    pool.close()
    pool.join()


def _check_args():
    if len(sys.argv) < 5:
        usage = """required args: <s3_endpoint_url> <s3_bucket> <prod,..> <index,..>
prod - e.g. S2A_MSIL1C_20170202T090201_N0204_R007_T35SNA_20170202T090155.SAFE
index - any of or all ndvi,ndi45,gndvi"""
        print(usage)
        raise SystemExit(1)


def _get_s3_coords():
    endpoint_url = sys.argv[1]
    bucket_id = sys.argv[2]
    return {'endpoint_url': endpoint_url,
            'bucket_id': bucket_id}


def _build_jobs(processor):
    products = sys.argv[3].split(',')
    tasks_req = sys.argv[4].split(',')
    tasks = []
    for t in tasks_req:
        tasks.append(tasks_map[t])
    return [[prod, processor, tasks] for prod in products]


if __name__ == '__main__':
    _check_args()

    main(_build_jobs(snap.main),
         indices_expr,
         _get_s3_coords())

    logger.info('success.')
