from boto3.s3.transfer import TransferConfig
from functools import partial
from multiprocessing.pool import ThreadPool
from boto3.s3.transfer import TransferConfig
from multiprocessing import Process
import xml.etree.ElementTree as ET
import product_meta as pm
import boto3
import threading
import errno
import time
import Shared
import os
import io

'''Download in parallel EO product to S3.

Boto provides a 'multipart' mode which chunk objects and proceed
the upload/download using threading. In addition we can parallelize the download
of multiple objects by using threading again.

usage: The init function returns the metadata filename list and a dictionary of
the bands' filename indexed with their accronym (B1, B2, B3 ...). Theses variables
allows you to download the metadata and the required bands via the functions
'get_product_metadata' and 'get_product_data'.

'''

# Multipart mode paramaters
GB = 1024 ** 3
config = TransferConfig(multipart_threshold=0.03 * GB,
                        max_concurrency=20,
                        use_threads=True)  # max_concurency

''' Takes the absolute path of a file and create locally the unexisting
 directories.'''


def create_dir(abs_path):
    path = ('/').join(abs_path.split("/")[:-1])
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


# Takes a SINGLE object's filename and donwload it locally.
def get_obj(obj, bucket_id):
    create_dir(obj)
    s3 = boto3.resource('s3')
    try:
        rep = s3.Bucket(bucket_id).download_file(obj, obj, Config=config)
    except OSError:
        print("Failled to download "
              + key
              + " from "
              + bucket_id)
    return obj


# List the objects of an entire bucket or one of its directories.
def get_product_keys(bucket_id, f=""):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_id)
    objects = list(bucket.objects.filter(Prefix=f + '/'))
    return map(lambda x: x.Object().key, list(objects))


def locate_bands(product, meta, file_keys, bucket_id):
    '''From the product's info containted in the 'xml' tree we can extract the
    bands's filename.

    inputs
        product: string of the product's name
        file_keys: product objects' list
        bucket_id: string of the bucket's name

    output
        bands: dictionary containing the product bands filename's
        indexed by their accronym.
    '''

    metadata_file = meta
    print "Determine bands' location from " + metadata_file
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_id, metadata_file)
    data = io.BytesIO()
    # Since we use xml file only once we retrieve it as
    obj.download_fileobj(data)
    data.seek(0)               # a file-like object.
    root = ET.parse(data).getroot()
    bands = {}
    for child in root[0][0][-1][0][0]:
        band = child.text
        bands[band.split(
            '_')[-1]] = ''.join([f for f in file_keys if f.find(band) != -1])
    return bands


# Takes an objects list and donwload it in parallel.
def get_product_metadata(keys, bucket_id):
    pool = ThreadPool(processes=len(keys))
    _get_obj = partial(get_obj, bucket_id=bucket_id)
    print "Download of the  metadata is started."
    pool.map(_get_obj, keys)
    Shared.shared.write('meta', True)
    print "Metadata ready."


# Takes the bands dict and donwload the selected ones in parallel.
def get_product_data(bands_dict, bucket_id, targets=None):
    def value2key(value):
        return bands_dict.keys()[bands_dict.values().index(value)]

    def cb(band):
        band_key = value2key(band)
        print("%s downloaded." % band_key)
        Shared.shared.write(band_key, True)

    if targets:
        bands = [bands_dict[i] for i in targets]
    else:
        bands = bands_dict.values()

    pool = ThreadPool(processes=len(bands))
    print "Download of %s is starting" % str(bands)
    res = [pool.apply_async(get_obj,
                            args=(band, bucket_id),
                            callback=cb) for band in bands]
    pool.close()
    pool.join()


# removes the bands file form whole product objects' list
def locate_metadata(files, bands):
    return [f for f in files if f not in bands]


# Explained in the top description
def init(bucket_id, product):
    product_file_list = get_product_keys(bucket_id, product)
    bands_index = locate_bands(
        product, pm.get_meta_from_prod(product), product_file_list, bucket_id)
    metadata_loc = locate_metadata(
        product_file_list, bands_index.values())
    return bands_index, metadata_loc
