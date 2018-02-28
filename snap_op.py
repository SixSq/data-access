import os
import sys
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
logger = get_logger('snap-op')

''' Read, resample, subset, and compute the vegetation indices of SENTINEL-2
    products'''

indices_expr = {'ndvi': '(B7 + B4) != 0 ? (B7 - B4) / (B7 + B4) : -2',
                'ndi45': '(B5 + B4) != 0 ? (B5 - B4) / (B5 + B4) : -2',
                'gndvi': '(B7 + B3) != 0 ? (B7 - B3) / (B7 + B3) : -2'}


def _log_product_info(product):
    logger.info("Product: %s, %d x %d pixels" %
                (product.getName(), product.getSceneRasterWidth(),
                 product.getSceneRasterHeight()))
    logger.info("Bands:   %s" % list(product.getBandNames()))


def read_product(f):
    product = ProductIO.readProduct(os.getcwd() + '/' + f)
    _log_product_info(product)
    return product


def resample(product, params):
    logger.info(">>> Re-sampling...")
    _log_product_info(product)
    HashMap = jpy.get_type('java.util.HashMap')
    parameters = HashMap()
    parameters.put('targetResolution', params)
    result = GPF.createProduct('Resample', parameters, product)
    logger.info(">>> Re-sampling... done.")
    return result


def subset(product):
    logger.info(">>> Subsetting...")
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
    logger.info(">>> Subsetting... done.")
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


def compute_vegetation_index(product, index):
    logger.info(">>> Compute vegetation index...")
    index = ''.join(index)
    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
    HashMap = jpy.get_type('java.util.HashMap')
    BandDescriptor = jpy.get_type('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor')
    targetBand = BandDescriptor()
    targetBand.name = index
    targetBand.type = 'float32'
    targetBand.expression = indices_expr[index]
    targetBands = jpy.array('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor', 1)
    targetBands[0] = targetBand
    parameters = HashMap()
    parameters.put('targetBands', targetBands)
    logger.info("Start to compute:" + indices_expr[index])
    result = GPF.createProduct('BandMaths', parameters, product)
    logger.info('Expression computed: ' + indices_expr[index])
    logger.info('Result: %s' % result.getBand(index))
    logger.info(">>> Compute vegetation index... done.")
    return result.getBand(index)


def main(product, params):
    product = read_product(product)
    product = resample(product, 60)
    compute_vegetation_index(product, params)


if __name__ == '__main__':
    products = ['S2A_OPER_PRD_MSIL1C_PDMC_20151230T202002_R008_V20151230T105153_20151230T105153.SAFE',
                'S2A_MSIL1C_20170202T090201_N0204_R007_T35SNA_20170202T090155.SAFE',
                'S2A_MSIL1C_20170617T012701_N0205_R074_T54SUF_20170617T013216.SAFE']

    meta = 'S2A_OPER_MTD_SAFL1C_PDMC_20151230T202002_R008_V20151230T105153_20151230T105153.xml'
    main(products[0] + '/' + meta, 'ndvi')
