''' Draft function returning the metadata '.xml' filename from the product name '''


def get_meta_from_prod(p):
    if p == "S2A_OPER_PRD_MSIL1C_PDMC_20151230T202002_R008_V20151230T105153_20151230T105153.SAFE":
        bar = p + '/' + "S2A_OPER_MTD_SAFL1C_PDMC_20151230T202002_R008_V20151230T105153_20151230T105153"
        foo = ".xml"
    else:
        foo = p.split('_')[1] + '.xml'
        bar = p + '/MTD_'
    return (bar + foo)
