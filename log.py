from __future__ import print_function
from utils import config_get
import inspect
import logging
import multiprocessing
import multiprocessing_logging
import os
import sys

LOG_LEVEL = logging.INFO
LOG_HANDLER = 'file'
LOG_FILE = 'eo-data-access.log'

FORMAT_FIELD_SEP = ' : '
FORMAT = \
    '%(asctime)s{0}' \
    '%(relativeCreated)6d{0}' \
    '%(levelname)s{0}' \
    '%(pathname)s:' \
    '%(lineno)03d{0}' \
    '%(funcName)s{0}' \
    '%(threadName)s{0}' \
    '%(processName)s ' \
    '>>> ' \
    '%(message)s'.format(FORMAT_FIELD_SEP)
FORMAT_DATE = '%Y-%m-%dT%H:%M:%SZ'


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '[%s->%s] %s' % (os.getppid(), os.getpid(), msg), kwargs


def _mod_name():
    frm = inspect.stack()[2]
    mod = inspect.getmodule(frm[0])
    return mod.__name__


def _get_logger_defaults(log_level, log_handler):
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
    return log_handler, log_level


# default logger
def get_logger_default(log_level=None, log_handler=None):
    log_handler, log_level = _get_logger_defaults(log_level, log_handler)
    if log_handler == 'console':
        logging.basicConfig(format=FORMAT, datefmt=FORMAT_DATE, level=log_level)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        fmt = logging.Formatter(fmt=FORMAT)
        ch.setFormatter(fmt)
        logger = logging.getLogger()
        logger.addHandler(ch)
    else:
        logging.basicConfig(filename=LOG_FILE, format=FORMAT,
                            datefmt=FORMAT_DATE, level=log_level)
        logger = logging.getLogger()
    return CustomAdapter(logger, {})


# multiprocessing_logging logger
def get_logger_multproc_logging(log_level=None, log_handler=None):
    log_handler, log_level = _get_logger_defaults(log_level, log_handler)
    if log_handler == 'console':
        logging.basicConfig(format=FORMAT, datefmt=FORMAT_DATE, level=log_level)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        fmt = logging.Formatter(fmt=FORMAT)
        ch.setFormatter(fmt)
        logger = logging.getLogger()
        if not len(logger.handlers):
            logger.addHandler(ch)
            logger.setLevel(logging.DEBUG)
        multiprocessing_logging.install_mp_handler()
    else:
        logging.basicConfig(filename=LOG_FILE, format=FORMAT,
                            datefmt=FORMAT_DATE, level=log_level)
        logger = logging.getLogger()
        multiprocessing_logging.install_mp_handler()
    return CustomAdapter(logger, {})


# multiprocessing native logger
def get_logger_multproc(log_level=None, log_handler=None):
    log_handler, log_level = _get_logger_defaults(log_level, log_handler)
    if log_handler == 'console':
        logger = multiprocessing.get_logger()
        logger.setLevel(log_level)
        ch = logging.StreamHandler(stream=sys.stdout)
        fmt = logging.Formatter(fmt=FORMAT)
        ch.setFormatter(fmt)
        logger.addHandler(ch)
    else:
        logger = multiprocessing.get_logger()
        logger.setLevel(log_level)
        fh = logging.FileHandler(LOG_FILE)
        fmt = logging.Formatter(fmt=FORMAT)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return CustomAdapter(logger, {})


# trivial print logger
def get_logger_print():
    class logger(object):
        def info(self, msg, *args):
            if args:
                print(msg % args, file=sys.stdout)
            else:
                print(msg, *args, file=sys.stdout)

        debug = info
        warn = info

    return logger()


def get_logger(*args, **kwargs):
    return get_logger_multproc_logging(**kwargs)


class Logger(object):
    def __init__(self):
        self.log = get_logger()
        # self.log = get_logger('%s.%s' % (_mod_name(), self.__class__.__name__))
