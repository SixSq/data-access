from functools import wraps
import os
import sys
import time

sys.path.append(os.path.expanduser('~/.snap/snap-python'))
import snappy
from snappy import ProductIO

jpy = snappy.jpy
from snappy import GPF
from snappy import Rectangle
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy

from log import get_logger

logger = get_logger()

''' Read, resample, subset, and compute the vegetation indices of SENTINEL-2
    products'''


def _log_product_info(product):
    width = product.getSceneRasterWidth()
    height = product.getSceneRasterHeight()
    name = product.getName()
    description = product.getDescription()
    band_names = product.getBandNames()

    logger.info("Product:     %s, %s" % (name, description))
    logger.info("Raster size: %d x %d pixels" % (width, height))
    logger.info("Start time:  " + str(product.getStartTime()))
    logger.info("End time:    " + str(product.getEndTime()))
    logger.info("Bands:       %s" % (list(band_names)))


def start_stop(msg):
    def func_decor(func):
        # FIXME: %(funcName)s in the logger still prints the name of the wrapper function.
        @wraps(func)
        def func_wrap(*args, **kwargs):
            t0 = time.time()
            logger.info('>>> Start: %s', msg)
            res = func(*args, **kwargs)
            logger.info('>>> Finish: %s. Time took: %.3f', msg, time.time() - t0)
            return res
        func_wrap.__name__ = func.__name__
        return func_wrap

    return func_decor


@start_stop('read product')
def read_product(fn):
    product = ProductIO.readProduct(os.getcwd() + '/' + fn)
    _log_product_info(product)
    return product


@start_stop('write product')
def write_product(product, veg_index):
    fn = 'snappy_bmaths_output_%s_%s.dim' % (veg_index, product.getName())
    fmt = 'BEAM-DIMAP'
    ProductIO.writeProduct(product, fn, fmt)


@start_stop('re-sampling')
def resample(product, params):
    _log_product_info(product)
    HashMap = jpy.get_type('java.util.HashMap')
    parameters = HashMap()
    parameters.put('targetResolution', params)
    return GPF.createProduct('Resample', parameters, product)


@start_stop('sub-setting')
def subset(product):
    _log_product_info(product)
    SubsetOp = jpy.get_type('org.esa.snap.core.gpf.common.SubsetOp')
    #    WKTReader = jpy.get_type('com.vividsolutions.jts.io.WKTReader')
    #    wkt = 'POLYGON ((27.350865857300093 36.824908050376905,
    #		     27.76637805803395 36.82295594263548,
    #	 	     27.76444424458719 36.628100558767244,
    #                     27.349980428973755 36.63003894847389,
    #                    27.350865857300093 36.824908050376905))'
    #    geometry = WKTReader().read(wkt)
    op = SubsetOp()
    op.setSourceProduct(product)
    op.setRegion(Rectangle(0, 500, 500, 500))
    sub_product = op.getTargetProduct()
    return sub_product


def save_array(band):
    w = band.getRasterWidth()
    h = band.getRasterHeight()
    band_data = numpy.zeros(w * h, numpy.float32)
    band.readPixels(0, 0, w, h, band_data)
    band_data.shape = h, w
    width = 12
    height = 12
    fig = plt.figure(figsize=(width, height))
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    im = plt.imshow(band_data)
    pos = fig.add_axes([0.93, 0.1, 0.02, 0.35])  # Set colorbar position in fig
    fig.colorbar(im, cax=pos)  # Create the colorbar
    plt.savefig(band.getName() + '.jpg')


@start_stop('compute vegetation index')
def compute_vegetation_index(product, index, index_expr):
    logger.info("vegetation index to compute: %s" % index)
    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
    HashMap = jpy.get_type('java.util.HashMap')
    BandDescriptor = jpy.get_type('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor')
    targetBand = BandDescriptor()
    targetBand.name = index
    targetBand.type = 'float32'
    targetBand.expression = index_expr
    targetBands = jpy.array('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor', 1)
    targetBands[0] = targetBand
    parameters = HashMap()
    parameters.put('targetBands', targetBands)
    logger.info("Start to compute expression:" + index_expr)
    result = GPF.createProduct('BandMaths', parameters, product)
    logger.info('Expression computed: ' + index_expr)
    logger.info('Result: %s' % result)
    return result


@start_stop(__name__)
def _main(product_fn_xml, veg_index, index_expr):
    """
    :param product_fn_xml: path to product's metadata xml
    :param veg_index: vegetation index to compute
    :param index_expr: expression to compute
    :return:
    """
    logger.info('snap_op main - product: %s', product_fn_xml)
    logger.info('snap_op main - vegetation index: %s', veg_index)
    logger.info('snap_op main - expression: %s', index_expr)
    product = read_product(product_fn_xml)
    product = resample(product, 60)
    product = subset(product)
    result = compute_vegetation_index(product, veg_index, index_expr)
    logger.info('Final result computed.')
    _log_product_info(result)
    write_product(result, veg_index)


def main(product_fn_xml, index, index_expr):
    "For calling from multi-threaded environment."
    _main(product_fn_xml, index, index_expr)


if __name__ == '__main__':
    products = ['S2A_MSIL1C_20170202T090201_N0204_R007_T35SNA_20170202T090155.SAFE',
                'S2A_MSIL1C_20170617T012701_N0205_R074_T54SUF_20170617T013216.SAFE']

    meta = 'MTD_MSIL1C.xml'
    _main(products[0] + '/' + meta, 'ndvi', '(B7 + B4) != 0 ? (B7 - B4) / (B7 + B4) : -2')
