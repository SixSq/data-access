import multiprocessing
import threading
import time
from functools import partial
from multiprocessing import Process
from random import randint

import NoDaemonProcess as ndp
import Shared
import product_downloader as prdl
import product_meta as pm
from log import get_logger
logger = get_logger('proc-runner')

''' Library  of communicating processes over a shared object
    index:  list of objects
'''


class download_decorator(object):

    def __init__(self, target):
        self.target = target

    def __call__(self, *args):
        """
        :param args: {'product': '', 'tasks': [{'bands': [], 'index': ''},], 'indices_expr': {'index': 'expr',}}
        :return:
        """
        self.product = args[0]
        task = args[1]
        self.bands = task['bands']
        self.index = task['index']
        self.indices_expr = args[2]
        self.s3conf = args[3]
        whoaim("a process assigned to object %s" % self.bands)
        self.register()
        while not all(Shared.shared.dict[k] for k in self.bands):
            logger.debug('keys found for @%s, %s - ' % multiprocessing.current_process().name +
                  ', '.join(k for k in self.bands if Shared.shared.dict[k]))
            rdm_sleep(1)
        return partial(self.target, index=self.index, indices_expr=self.indices_expr)
        # return self.target(pm.get_meta_from_prod(self.product), self.params)

    def run_download_manager(self):

        def create_process(bands_loc, metadata_loc):
            whoaim("the download manager process for metadata and bands.")
            object_list = [k for k in Shared.shared.dict.keys() if k in bands_loc.keys()]
            logger.info("Bands selected: " + str(object_list))
            meta = threading.Thread(target=prdl.get_product_metadata,
                                    args=(metadata_loc, self.s3conf))

            bands = threading.Thread(target=prdl.get_product_data,
                                     args=(bands_loc, self.s3conf, object_list))
            meta.start()
            meta.join()  # Can be optimized
            bands.start()
            bands.join()

        def download_manager():
            bands_loc, metadata_loc = prdl.init(self.s3conf, self.product)
            create_process(bands_loc, metadata_loc)

        downlad_manager_daemon = Process(target=download_manager)
        downlad_manager_daemon.daemon = False
        downlad_manager_daemon.start()
        downlad_manager_daemon.join()

    def register(self):
        valid_index = [key for key in self.bands if key not in Shared.shared.dict]
        for v in valid_index:
            Shared.shared.write(v, False)
        logger.info("Objects: %s registered in shared object" % ','.join(self.bands))
        rdm_sleep()
        Shared.shared.dict["nbproc"] += -1
        if Shared.shared.dict["nbproc"] == 0:
            Shared.shared.write('Init', False)
            self.run_download_manager()


def rdm_sleep(offset=0):
    time.sleep(.001 * randint(10, 100) + offset)


def whoaim(id):
    logger.info("I'm running on CPU #%s and I am %s" % (multiprocessing.current_process().name, id))


def main(proc_func, args, s3conf):
    """
    :param proc_func: processing function
    :param args: {'product': '', 'tasks': [{'bands': [], 'index': ''},], 'indices_expr': {'index': 'expr',}}
    :return:
    """
    nbproc = len(args['tasks'])
    Shared.shared.write("nbproc", nbproc)
    pool = ndp.MyPool(nbproc)
    prod_endpoint = pm.get_meta_from_prod(args['product'])

    def executor(_proc_func):
        _proc_func(prod_endpoint)
        return "ok"

    for task in args['tasks']:
        pool.apply_async(
            download_decorator(proc_func),
            args=(args['product'], task, args['indices_expr'], s3conf),
            callback=executor)
    pool.close()
    pool.join()
