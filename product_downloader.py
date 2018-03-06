"""Download in parallel EO product to S3.

boto provides a 'multipart' mode which chunks objects and proceeds
the upload/download using threading. In addition we can parallelize the download
of multiple objects by using threading again.

usage: The init function returns the metadata filename list and a dictionary of
the bands' filename indexed with their accronym (B1, B2, B3 ...). Theses variables
allows you to download the metadata and the required bands via the functions
'get_product_metadata' and 'get_product_data'.
"""

from functools import partial
from multiprocessing.pool import ThreadPool
import errno
import io
import logging
import os
import time
import xml.etree.ElementTree as ET

from utils import config_get, rdm_sleep
import boto3
from boto3.s3.transfer import TransferConfig
log_level = logging.getLevelName(config_get('log_level').strip())
boto3.set_stream_logger(name='botocore', level=log_level)
logging.getLogger("botocore.vendored.requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)

import Shared
import product_meta as pm

from log import get_logger
logger = get_logger()

# Multipart mode paramaters
GB = 1024 ** 3
config = TransferConfig(multipart_threshold=0.03 * GB,
                        max_concurrency=20,
                        use_threads=True)

''' Takes the absolute path of a file and create locally the unexisting
 directories.'''


def _create_dir(abs_path):
    path = '/'.join(abs_path.split("/")[:-1])
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def _download_obj(obj, s3conf):
    """Takes a single object's filename and downloads it locally.
    :param obj:
    :param s3conf:
    :return:
    """
    _create_dir(obj)
    s3 = boto3.resource('s3', endpoint_url=s3conf['endpoint_url'])
    try:
        t0 = time.time()
        logger.debug('%s - start object download' % obj)
        s3.Bucket(s3conf['bucket_id']).download_file(obj, obj, Config=config)
        logger.debug('%s - finish object download. Time took: %0.3f' % (obj, time.time() - t0))
    except OSError as ex:
        msg = "Failed to download %s from %s." % (obj, s3conf['bucket_id'])
        logger.error(msg)
        raise Exception('%s %s' % (msg, 'Error: %s' % ex))
    return obj


def _get_product_keys(s3conf, f=""):
    """Lists the objects of an entire bucket or one of its directories.

    :param s3conf:
    :param f:
    :return:
    """
    s3 = boto3.resource('s3', endpoint_url=s3conf['endpoint_url'])
    bucket = s3.Bucket(s3conf['bucket_id'])
    objects = list(bucket.objects.filter(Prefix=f + '/'))
    return map(lambda x: x.Object().key, list(objects))


def _locate_bands(product, meta, file_keys, s3conf):
    """From the product's info containted in the 'xml' tree we can extract the
    bands's filename.

    inputs
        product: string of the product's name
        file_keys: product objects' list
        s3conf: string of the bucket's name

    output
        bands: dictionary containing the product bands filename's
        indexed by their accronym.
    """

    metadata_file = meta
    logger.info("Determine bands' location from " + metadata_file)
    s3 = boto3.resource('s3', endpoint_url=s3conf['endpoint_url'])
    obj = s3.Object(s3conf['bucket_id'], metadata_file)
    data = io.BytesIO()
    # Since we use xml file only once we retrieve it as
    obj.download_fileobj(data)
    data.seek(0)  # a file-like object.
    root = ET.parse(data).getroot()
    bands = {}
    for child in root[0][0][-1][0][0]:
        band = child.text
        bands[band.split('_')[-1]] = ''.join([f for f in file_keys if f.find(band) != -1])
    return bands


def get_product_metadata(keys, s3conf):
    """Takes an objects list and downloads it in parallel.

    :param keys:
    :param s3conf:
    :return:
    """
    pool = ThreadPool(processes=len(keys))
    _get_obj = partial(_download_obj, s3conf=s3conf)
    t0 = time.time()
    logger.info("Metadata: starting download.")
    pool.map(_get_obj, keys)
    Shared.shared.write('meta', True)
    logger.info("Metadata: finished downloading. Time took: %0.3f" % (time.time() - t0))


def get_product_data(bands_dict, s3conf, targets=None):
    """Takes the bands dict and downloads the selected ones in parallel.

    :param bands_dict:
    :param s3conf:
    :param targets:
    :return:
    """

    def value2key(value):
        return bands_dict.keys()[bands_dict.values().index(value)]

    def callback(band):
        band_key = value2key(band)
        Shared.shared.write(band_key, True)
        logger.info("%s downloaded." % band_key)

    if targets:
        bands = [bands_dict[i] for i in targets]
    else:
        bands = bands_dict.values()

    def obj_downloader(band, bucket_id):
        t0 = time.time()
        bname = value2key(band)
        logger.info('%s - start object download' % bname)
        obj = _download_obj(band, bucket_id)
        logger.info('%s - finish object download. Time took: %0.3f' % (bname, time.time() - t0))
        return obj

    pool = ThreadPool(processes=len(bands))
    logger.info("Product data: starting download of %s" % str(bands))
    res = []
    for band in bands:
        res.append(pool.apply_async(obj_downloader, args=(band, s3conf), callback=callback))

    pool.close()
    pool.join()


def _locate_metadata(files, bands):
    """Removes the bands file form whole product objects' list

    :param files:
    :param bands:
    :return:
    """
    return [f for f in files if f not in bands]


def init(s3conf, product):
    """Returns the metadata filename list and a dictionary of
    the bands' filename indexed with their acronym (B1, B2, B3 ...).  This
    allows to download the metadata and the required bands via the functions
    'get_product_metadata' and 'get_product_data'.

    :param s3conf:
    :param product:
    :return:
    """
    product_file_list = _get_product_keys(s3conf, product)
    bands_index = _locate_bands(product, pm.get_meta_from_prod(product), product_file_list, s3conf)
    metadata_loc = _locate_metadata(product_file_list, bands_index.values())
    return bands_index, metadata_loc
