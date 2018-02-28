import logging
from utils import config_get

LOG_LEVEL = logging.INFO
LOG_HANDLER = 'file'
LOG_FILE = 'eo-data-access.log'

FORMAT_FIELD_SEP = ' '
FORMAT = '%(asctime)s{0}%(threadName)s{0}%(name)s{0}%(levelname)s{0}%(message)s'.format(FORMAT_FIELD_SEP)
FORMAT_DATE = '%Y-%m-%dT%H:%M:%SZ'


def get_logger(name=__name__, log_level=None, log_handler=None):
    if log_level is None:
        try:
            log_level = logging.getLevelName(config_get('log_level').strip())
        except Exception:
            log_level = LOG_LEVEL
    if log_handler is None:
        try:
            log_handler = config_get('log_handler').strip()
        except Exception:
            log_handler = LOG_HANDLER
    if log_handler == 'console':
        logging.basicConfig(format=FORMAT, datefmt=FORMAT_DATE, level=log_level)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        logger = logging.getLogger(name)
        logger.addHandler(ch)
    else:
        logging.basicConfig(filename=LOG_FILE, format=FORMAT,
                            datefmt=FORMAT_DATE, level=log_level)
        logger = logging.getLogger(name)
    return logger


class Logger(object):
    def __init__(self):
        self.log = get_logger('%s.%s' % (__name__, self.__class__.__name__))
