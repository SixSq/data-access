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

from utils import config_get
import boto3
from boto3.s3.transfer import TransferConfig
log_level = logging.getLevelName(config_get('log_level').strip())
boto3.set_stream_logger(name='botocore', level=log_level)
logging.getLogger("botocore.vendored.requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)

import Shared
import product_meta as pm

from log import get_logger
logger = get_logger('product-downloader')

endpoint_url = config_get('endpoint_url')

# Multipart mode paramaters
GB = 1024 ** 3
config = TransferConfig(multipart_threshold=0.03 * GB,
                        max_concurrency=20,
                        use_threads=True)  # max_concurency

''' Takes the absolute path of a file and create locally the unexisting
 directories.'''


def create_dir(abs_path):
    path = '/'.join(abs_path.split("/")[:-1])
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def _download_obj(obj, bucket_id):
    """Takes a single object's filename and downloads it locally.
    :param obj:
    :param bucket_id:
    :return:
    """
    create_dir(obj)
    s3 = boto3.resource('s3', endpoint_url=endpoint_url)
    try:
        t0 = time.time()
        logger.debug('%s - start object download' % obj)
        s3.Bucket(bucket_id).download_file(obj, obj, Config=config)
        logger.debug('%s - finish object download. Time took: %0.3f' % (obj, time.time() - t0))
    except OSError as ex:
        msg = "Failed to download %s from %s." % (obj, bucket_id)
        logger.info(msg)
        raise Exception('%s %s' % (msg, 'Error: %s' % ex))
    return obj


def _get_product_keys(bucket_id, f=""):
    """Lists the objects of an entire bucket or one of its directories.

    :param bucket_id:
    :param f:
    :return:
    """
    s3 = boto3.resource('s3', endpoint_url=endpoint_url)
    bucket = s3.Bucket(bucket_id)
    objects = list(bucket.objects.filter(Prefix=f + '/'))
    return map(lambda x: x.Object().key, list(objects))


def _locate_bands(product, meta, file_keys, bucket_id):
    """From the product's info containted in the 'xml' tree we can extract the
    bands's filename.

    inputs
        product: string of the product's name
        file_keys: product objects' list
        bucket_id: string of the bucket's name

    output
        bands: dictionary containing the product bands filename's
        indexed by their accronym.
    """

    metadata_file = meta
    logger.info("Determine bands' location from " + metadata_file)
    s3 = boto3.resource('s3', endpoint_url=endpoint_url)
    obj = s3.Object(bucket_id, metadata_file)
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


def get_product_metadata(keys, bucket_id):
    """Takes an objects list and downloads it in parallel.

    :param keys:
    :param bucket_id:
    :return:
    """
    pool = ThreadPool(processes=len(keys))
    _get_obj = partial(_download_obj, bucket_id=bucket_id)
    t0 = time.time()
    logger.info("Metadata: starting download.")
    pool.map(_get_obj, keys)
    Shared.shared.write('meta', True)
    logger.info("Metadata: finished downloading. Time took: %0.3f" % (time.time() - t0))


def get_product_data(bands_dict, bucket_id, targets=None):
    """Takes the bands dict and downloads the selected ones in parallel.

    :param bands_dict:
    :param bucket_id:
    :param targets:
    :return:
    """

    def value2key(value):
        return bands_dict.keys()[bands_dict.values().index(value)]

    def callback(band):
        band_key = value2key(band)
        logger.info("%s downloaded." % band_key)
        Shared.shared.write(band_key, True)

    if targets:
        bands = [bands_dict[i] for i in targets]
    else:
        bands = bands_dict.values()

    pool = ThreadPool(processes=len(bands))
    logger.info("Product data: starting download of %s" % str(bands))
    res = []
    for band in bands:
        res.append(pool.apply_async(_download_obj, args=(band, bucket_id), callback=callback))
    # logger.info('get_product_data: results list -> %s' % res)

    pool.close()
    pool.join()


def _locate_metadata(files, bands):
    """Removes the bands file form whole product objects' list

    :param files:
    :param bands:
    :return:
    """
    return [f for f in files if f not in bands]


def init(bucket_id, product):
    """Returns the metadata filename list and a dictionary of
    the bands' filename indexed with their acronym (B1, B2, B3 ...).  This
    allows to download the metadata and the required bands via the functions
    'get_product_metadata' and 'get_product_data'.

    :param bucket_id:
    :param product:
    :return:
    """
    product_file_list = _get_product_keys(bucket_id, product)
    bands_index = _locate_bands(
        product, pm.get_meta_from_prod(product), product_file_list, bucket_id)
    metadata_loc = _locate_metadata(product_file_list, bands_index.values())
    return bands_index, metadata_loc
