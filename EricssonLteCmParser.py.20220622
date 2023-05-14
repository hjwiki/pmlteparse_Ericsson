#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author:haojian
# File Name: EricssonLteCmParser.py
# Created Time: 2022/6/1 10:50:57


import os,sys
import logging
from logging.handlers import RotatingFileHandler
import xml.etree.ElementTree as ET
from xml.parsers import expat
import math
import glob
import tarfile
import gzip
import datetime

#assert ('linux' in sys.platform), '该代码只能在 Linux 下执行'

#handler = RotatingFileHandler('EricssonLteCmParser.log',maxBytes = 100*1024*1024,backupCount = 3)
handler = logging.FileHandler('EricssonLteCmParser.py.log')
#handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
console = logging.StreamHandler()
#console.setLevel(logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
logger.addHandler(handler)
#logger.addHandler(console)



def deal_with_file(xmltext,out):
    #tree = ET.parse(fname)
    #root = tree.getroot()
    root=ET.fromstring(xmltext)
    configData=root.find('{configData.xsd}configData')
    SubNetwork=configData.find('{genericNrm.xsd}SubNetwork')
    MeContext=SubNetwork.find('{genericNrm.xsd}MeContext')
    ManagedElement=MeContext.find('{genericNrm.xsd}ManagedElement')
    VsDataContainer=[i for i in list(ManagedElement) if i.tag=='{genericNrm.xsd}VsDataContainer' and i.find('{genericNrm.xsd}attributes').find('{genericNrm.xsd}vsDataType').text=='vsDataENodeBFunction' and i.find('{genericNrm.xsd}attributes').find('{genericNrm.xsd}vsDataFormatVersion').text=='EricssonSpecificAttributes'][0]
    attributes=VsDataContainer.find('{genericNrm.xsd}attributes')
    vsDataENodeBFunction=attributes.find('{EricssonSpecificAttributes.xsd}vsDataENodeBFunction')
    eNBId=vsDataENodeBFunction.find('{EricssonSpecificAttributes.xsd}eNBId').text
    for VsDataContainer in [i for i in list(VsDataContainer) if i.tag=='{genericNrm.xsd}VsDataContainer' and i.find('{genericNrm.xsd}attributes').find('{genericNrm.xsd}vsDataType').text=='vsDataEUtranCellFDD' and i.find('{genericNrm.xsd}attributes').find('{genericNrm.xsd}vsDataFormatVersion').text=='EricssonSpecificAttributes']:
        EUtranCellFDD=VsDataContainer.attrib['id']
        attributes=VsDataContainer.find('{genericNrm.xsd}attributes')
        vsDataEUtranCellFDD=attributes.find('{EricssonSpecificAttributes.xsd}vsDataEUtranCellFDD')
        cellId=vsDataEUtranCellFDD.find('{EricssonSpecificAttributes.xsd}cellId').text
        mncList=[i.find('{EricssonSpecificAttributes.xsd}mnc').text for i in list(vsDataEUtranCellFDD) if i.tag=='{EricssonSpecificAttributes.xsd}additionalPlmnList']
        share='是' if '11' in mncList else '否'
        out.append(EUtranCellFDD+','+'127.'+eNBId+'.'+cellId+','+share)

def deal_with_tar(tarName):
    out=list()
    with tarfile.open(tarName,'r') as tar:
        for gz in tar.getmembers()[:]:
            logger.info(gz.name)
            #deal_with_file(gzip.decompress(tar.extractfile(gz).read()),out)
            deal_with_file(tar.extractfile(gz).read(),out)
    return out

if __name__ == '__main__':
    os.chdir(sys.path[0])
    tarName=glob.glob('/data/esbftp/cm/4G/ERICSSON/OMC1/CM/202*/CM_4G*.tar.gz')[-1]
    out=deal_with_tar(tarName)
    csvName='cell%s.csv'%(datetime.datetime.now().strftime("%Y%m%d_%H"))
    if os.path.isfile(csvName):os.remove(csvName)
    with open(csvName+'.temp','w') as f:
        f.write('\n'.join(out))
    os.rename(csvName+'.temp',csvName)

