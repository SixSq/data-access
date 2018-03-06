from functools import partial
from multiprocessing import Process
import multiprocessing
import threading

import NoDaemonProcess as ndp
import Shared
import product_downloader as prdl
import product_meta as pm
from log import get_logger
from utils import rdm_sleep

logger = get_logger()
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
        self.index_expr = args[2]
        self.s3conf = args[3]
        whoaim("a process assigned to object %s" % self.bands)
        self._register_and_download_bands()

        return partial(self.target, index=self.index, index_expr=self.index_expr)
        # return self.target(pm.get_meta_from_prod(self.product), self.params)

    def _register_and_download_bands(self):
        valid_index = [key for key in self.bands if key not in Shared.shared.dict]
        for v in valid_index:
            Shared.shared.write(v, False)
        logger.info('Registered in shared object: %s' % ','.join(self.bands))
        logger.info('The shared object: %s' % Shared.shared.dict)
        rdm_sleep()

        Shared.shared.dict["nbproc"] += -1
        if Shared.shared.dict["nbproc"] == 0:
            Shared.shared.write('Init', False)
            self._run_download_manager()

        # barrier until bands are downloaded
        while not all(Shared.shared.dict[k] for k in self.bands):
            logger.info(Shared.shared.dict)
            logger.info('Keys found for @%s, %s', multiprocessing.current_process().name,
                        [k for k in self.bands if Shared.shared.dict[k]])
            rdm_sleep(1)
        logger.debug('The shared object after barrier: %s' % Shared.shared.dict)


    def _run_download_manager(self):

        def create_download_threads(bands_loc, metadata_loc):
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
            create_download_threads(bands_loc, metadata_loc)

        downlad_manager_daemon = Process(target=download_manager)
        downlad_manager_daemon.daemon = True
        downlad_manager_daemon.start()
        downlad_manager_daemon.join()


def whoaim(id):
    logger.info("I'm running on CPU #%s and I am %s" % (multiprocessing.current_process(), id))


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

    def proc_func_runner(_proc_func):
        return _proc_func(prod_endpoint)

    res = []
    for task in args['tasks']:
        logger.info('Starting async daemon for task: %s' % task)
        res.append(pool.apply_async(
            download_decorator(proc_func),
            args=(args['product'], task, args['indices_expr'][task['index']], s3conf),
            callback=proc_func_runner))

    pool.close()
    pool.join()
