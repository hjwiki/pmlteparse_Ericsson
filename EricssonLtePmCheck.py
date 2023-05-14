#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author:haojian
# File Name: EricssonLtePmParser.py
# Created Time: 2022/3/29 11:16:49
# 每次解析一个小时的数据
# 文件路径/data/esbftp/pm/4G/ERICSSON/OMC1/PM/20220531/PM_A20220531103000_900_4GNSA.tar.gz
#2022-6-15 增加指标


import os,sys
import logging
from logging.handlers import RotatingFileHandler
import xml.etree.ElementTree as ET
from xml.parsers import expat
import math
import glob
import datetime
import tarfile
import gzip

os.chdir(sys.path[0])
#assert ('linux' in sys.platform), '该代码只能在 Linux 下执行'
if 'linux' in sys.platform:
    logpath='../log/'
    cmpath='/data/output/cm/ericsson/4g/EricssonLteCm_*.cs*'
    outpath='/data/output/pm/ericsson/4g/'
else:
    #用于本地测试
    logpath='./'
    cmpath='./EricssonLteCm_*.cs*'
    outpath='./'

#handler = RotatingFileHandler('EricssonLtePmParser.log',maxBytes = 100*1024*1024,backupCount = 3)
handler = logging.FileHandler(logpath+'EricssonLtePmParser_%s.log'%datetime.datetime.now().strftime('%Y%m%d'))
#handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
console = logging.StreamHandler()
#console.setLevel(logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
logger.addHandler(handler)
if 'linux' not in sys.platform:
    logger.addHandler(console)

#解析时延
hour_delay=2



def deal_with_kpi(cell,dd,starttime,interval):
    a_sdate=starttime      #分析时间  类型:timestamp,
    a_eci=cell      #对象编号  类型:varchar,
    a_drop_duration=dd.get('pmCellDowntimeAuto',0)+dd.get('pmCellDowntimeMan',0)      #LTE小区退服时长(s)  类型:numeric,
    a_total_duration=interval      #小区统计时长(s)  类型:numeric,
    a_cell_available_ratio='' if interval==0 else 1-(dd.get('pmCellDowntimeAuto',0)+dd.get('pmCellDowntimeMan',0))/interval     #LTE小区可用率(%)  类型:numeric,
    a_noise='' if ((dd.get('pmRadioRecInterferencePwr_0',0)+dd.get('pmRadioRecInterferencePwr_1',0)+dd.get('pmRadioRecInterferencePwr_2',0)+dd.get('pmRadioRecInterferencePwr_3',0)+dd.get('pmRadioRecInterferencePwr_4',0)+dd.get('pmRadioRecInterferencePwr_5',0)+dd.get('pmRadioRecInterferencePwr_6',0)+dd.get('pmRadioRecInterferencePwr_7',0)+dd.get('pmRadioRecInterferencePwr_8',0)+dd.get('pmRadioRecInterferencePwr_9',0)+dd.get('pmRadioRecInterferencePwr_10',0)+dd.get('pmRadioRecInterferencePwr_11',0)+dd.get('pmRadioRecInterferencePwr_12',0)+dd.get('pmRadioRecInterferencePwr_13',0)+dd.get('pmRadioRecInterferencePwr_14',0)+dd.get('pmRadioRecInterferencePwr_15',0)))==0 else 10*math.log10(((pow(10,(-121/10.0))*(dd.get('pmRadioRecInterferencePwr_0',0)))+(pow(10,(-120.5/10.0))*(dd.get('pmRadioRecInterferencePwr_1',0)))+(pow(10,(-119.5/10.0))*(dd.get('pmRadioRecInterferencePwr_2',0)))+(pow(10,(-118.5/10.0))*(dd.get('pmRadioRecInterferencePwr_3',0)))+(pow(10,(-117.5/10.0))*(dd.get('pmRadioRecInterferencePwr_4',0)))+(pow(10,(-116.5/10.0))*(dd.get('pmRadioRecInterferencePwr_5',0)))+(pow(10,(-115.5/10.0))*(dd.get('pmRadioRecInterferencePwr_6',0)))+(pow(10,(-114.5/10.0))*(dd.get('pmRadioRecInterferencePwr_7',0)))+(pow(10,(-113.5/10.0))*(dd.get('pmRadioRecInterferencePwr_8',0)))+(pow(10,(-112.5/10.0))*(dd.get('pmRadioRecInterferencePwr_9',0)))+(pow(10,(-110/10.0))*(dd.get('pmRadioRecInterferencePwr_10',0)))+(pow(10,(-106/10.0))*(dd.get('pmRadioRecInterferencePwr_11',0)))+(pow(10,(-102/10.0))*(dd.get('pmRadioRecInterferencePwr_12',0)))+(pow(10,(-98/10.0))*(dd.get('pmRadioRecInterferencePwr_13',0)))+(pow(10,(-94/10.0))*(dd.get('pmRadioRecInterferencePwr_14',0)))+(pow(10,(-90/10.0))*(dd.get('pmRadioRecInterferencePwr_15',0))))/((dd.get('pmRadioRecInterferencePwr_0',0)+dd.get('pmRadioRecInterferencePwr_1',0)+dd.get('pmRadioRecInterferencePwr_2',0)+dd.get('pmRadioRecInterferencePwr_3',0)+dd.get('pmRadioRecInterferencePwr_4',0)+dd.get('pmRadioRecInterferencePwr_5',0)+dd.get('pmRadioRecInterferencePwr_6',0)+dd.get('pmRadioRecInterferencePwr_7',0)+dd.get('pmRadioRecInterferencePwr_8',0)+dd.get('pmRadioRecInterferencePwr_9',0)+dd.get('pmRadioRecInterferencePwr_10',0)+dd.get('pmRadioRecInterferencePwr_11',0)+dd.get('pmRadioRecInterferencePwr_12',0)+dd.get('pmRadioRecInterferencePwr_13',0)+dd.get('pmRadioRecInterferencePwr_14',0)+dd.get('pmRadioRecInterferencePwr_15',0))))      #平均每PRB干扰噪声功率(dBm)  类型:numeric,
    a_cqi0=dd.get('pmRadioUeRepCqiDistr_0',0)      #CQI 0数量(个)  类型:numeric,
    a_cqi1=dd.get('pmRadioUeRepCqiDistr_1',0)      #CQI 1数量(个)  类型:numeric,
    a_cqi2=dd.get('pmRadioUeRepCqiDistr_2',0)      #CQI 2数量(个)  类型:numeric,
    a_cqi3=dd.get('pmRadioUeRepCqiDistr_3',0)      #CQI 3数量(个)  类型:numeric,
    a_cqi4=dd.get('pmRadioUeRepCqiDistr_4',0)      #CQI 4数量(个)  类型:numeric,
    a_cqi5=dd.get('pmRadioUeRepCqiDistr_5',0)      #CQI 5数量(个)  类型:numeric,
    a_cqi6=dd.get('pmRadioUeRepCqiDistr_6',0)      #CQI 6数量(个)  类型:numeric,
    a_cqi7=dd.get('pmRadioUeRepCqiDistr_7',0)      #CQI 7数量(个)  类型:numeric,
    a_cqi8=dd.get('pmRadioUeRepCqiDistr_8',0)      #CQI 8数量(个)  类型:numeric,
    a_cqi9=dd.get('pmRadioUeRepCqiDistr_9',0)      #CQI 9数量(个)  类型:numeric,
    a_cqi10=dd.get('pmRadioUeRepCqiDistr_10',0)      #CQI 10数量(个)  类型:numeric,
    a_cqi11=dd.get('pmRadioUeRepCqiDistr_11',0)      #CQI 11数量(个)  类型:numeric,
    a_cqi12=dd.get('pmRadioUeRepCqiDistr_12',0)      #CQI 12数量(个)  类型:numeric,
    a_cqi13=dd.get('pmRadioUeRepCqiDistr_13',0)      #CQI 13数量(个)  类型:numeric,
    a_cqi14=dd.get('pmRadioUeRepCqiDistr_14',0)      #CQI 14数量(个)  类型:numeric,
    a_cqi15=dd.get('pmRadioUeRepCqiDistr_15',0)      #CQI 15数量(个)  类型:numeric,
    a_cqi_avg='' if (dd.get('pmRadioUeRepCqiDistr_0',0)+dd.get('pmRadioUeRepCqiDistr_1',0)+dd.get('pmRadioUeRepCqiDistr_2',0)+dd.get('pmRadioUeRepCqiDistr_3',0)+dd.get('pmRadioUeRepCqiDistr_4',0)+dd.get('pmRadioUeRepCqiDistr_5',0)+dd.get('pmRadioUeRepCqiDistr_6',0)+dd.get('pmRadioUeRepCqiDistr_7',0)+dd.get('pmRadioUeRepCqiDistr_8',0)+dd.get('pmRadioUeRepCqiDistr_9',0)+dd.get('pmRadioUeRepCqiDistr_10',0)+dd.get('pmRadioUeRepCqiDistr_11',0)+dd.get('pmRadioUeRepCqiDistr_12',0)+dd.get('pmRadioUeRepCqiDistr_13',0)+dd.get('pmRadioUeRepCqiDistr_14',0)+dd.get('pmRadioUeRepCqiDistr_15',0))==0 else  (0*dd.get('pmRadioUeRepCqiDistr_0',0)+1*dd.get('pmRadioUeRepCqiDistr_1',0)+2*dd.get('pmRadioUeRepCqiDistr_2',0)+3*dd.get('pmRadioUeRepCqiDistr_3',0)+4*dd.get('pmRadioUeRepCqiDistr_4',0)+5*dd.get('pmRadioUeRepCqiDistr_5',0)+6*dd.get('pmRadioUeRepCqiDistr_6',0)+7*dd.get('pmRadioUeRepCqiDistr_7',0)+8*dd.get('pmRadioUeRepCqiDistr_8',0)+9*dd.get('pmRadioUeRepCqiDistr_9',0)+10*dd.get('pmRadioUeRepCqiDistr_10',0)+11*dd.get('pmRadioUeRepCqiDistr_11',0)+12*dd.get('pmRadioUeRepCqiDistr_12',0)+13*dd.get('pmRadioUeRepCqiDistr_13',0)+14*dd.get('pmRadioUeRepCqiDistr_14',0)+15*dd.get('pmRadioUeRepCqiDistr_15',0))/(dd.get('pmRadioUeRepCqiDistr_0',0)+dd.get('pmRadioUeRepCqiDistr_1',0)+dd.get('pmRadioUeRepCqiDistr_2',0)+dd.get('pmRadioUeRepCqiDistr_3',0)+dd.get('pmRadioUeRepCqiDistr_4',0)+dd.get('pmRadioUeRepCqiDistr_5',0)+dd.get('pmRadioUeRepCqiDistr_6',0)+dd.get('pmRadioUeRepCqiDistr_7',0)+dd.get('pmRadioUeRepCqiDistr_8',0)+dd.get('pmRadioUeRepCqiDistr_9',0)+dd.get('pmRadioUeRepCqiDistr_10',0)+dd.get('pmRadioUeRepCqiDistr_11',0)+dd.get('pmRadioUeRepCqiDistr_12',0)+dd.get('pmRadioUeRepCqiDistr_13',0)+dd.get('pmRadioUeRepCqiDistr_14',0)+dd.get('pmRadioUeRepCqiDistr_15',0))      #平均CQI  类型:numeric,
    a_cqi_ge7='' if (dd.get('pmRadioUeRepCqiDistr_0',0)+dd.get('pmRadioUeRepCqiDistr_1',0)+dd.get('pmRadioUeRepCqiDistr_2',0)+dd.get('pmRadioUeRepCqiDistr_3',0)+dd.get('pmRadioUeRepCqiDistr_4',0)+dd.get('pmRadioUeRepCqiDistr_5',0)+dd.get('pmRadioUeRepCqiDistr_6',0)+dd.get('pmRadioUeRepCqiDistr_7',0)+dd.get('pmRadioUeRepCqiDistr_8',0)+dd.get('pmRadioUeRepCqiDistr_9',0)+dd.get('pmRadioUeRepCqiDistr_10',0)+dd.get('pmRadioUeRepCqiDistr_11',0)+dd.get('pmRadioUeRepCqiDistr_12',0)+dd.get('pmRadioUeRepCqiDistr_13',0)+dd.get('pmRadioUeRepCqiDistr_14',0)+dd.get('pmRadioUeRepCqiDistr_15',0))==0 else  (dd.get('pmRadioUeRepCqiDistr_7',0)+dd.get('pmRadioUeRepCqiDistr_8',0)+dd.get('pmRadioUeRepCqiDistr_9',0)+dd.get('pmRadioUeRepCqiDistr_10',0)+dd.get('pmRadioUeRepCqiDistr_11',0)+dd.get('pmRadioUeRepCqiDistr_12',0)+dd.get('pmRadioUeRepCqiDistr_13',0)+dd.get('pmRadioUeRepCqiDistr_14',0)+dd.get('pmRadioUeRepCqiDistr_15',0))/(dd.get('pmRadioUeRepCqiDistr_0',0)+dd.get('pmRadioUeRepCqiDistr_1',0)+dd.get('pmRadioUeRepCqiDistr_2',0)+dd.get('pmRadioUeRepCqiDistr_3',0)+dd.get('pmRadioUeRepCqiDistr_4',0)+dd.get('pmRadioUeRepCqiDistr_5',0)+dd.get('pmRadioUeRepCqiDistr_6',0)+dd.get('pmRadioUeRepCqiDistr_7',0)+dd.get('pmRadioUeRepCqiDistr_8',0)+dd.get('pmRadioUeRepCqiDistr_9',0)+dd.get('pmRadioUeRepCqiDistr_10',0)+dd.get('pmRadioUeRepCqiDistr_11',0)+dd.get('pmRadioUeRepCqiDistr_12',0)+dd.get('pmRadioUeRepCqiDistr_13',0)+dd.get('pmRadioUeRepCqiDistr_14',0)+dd.get('pmRadioUeRepCqiDistr_15',0))      #CQI>=7占比  类型:numeric,
    a_cqi_le7='' if (dd.get('pmRadioUeRepCqiDistr_0',0)+dd.get('pmRadioUeRepCqiDistr_1',0)+dd.get('pmRadioUeRepCqiDistr_2',0)+dd.get('pmRadioUeRepCqiDistr_3',0)+dd.get('pmRadioUeRepCqiDistr_4',0)+dd.get('pmRadioUeRepCqiDistr_5',0)+dd.get('pmRadioUeRepCqiDistr_6',0)+dd.get('pmRadioUeRepCqiDistr_7',0)+dd.get('pmRadioUeRepCqiDistr_8',0)+dd.get('pmRadioUeRepCqiDistr_9',0)+dd.get('pmRadioUeRepCqiDistr_10',0)+dd.get('pmRadioUeRepCqiDistr_11',0)+dd.get('pmRadioUeRepCqiDistr_12',0)+dd.get('pmRadioUeRepCqiDistr_13',0)+dd.get('pmRadioUeRepCqiDistr_14',0)+dd.get('pmRadioUeRepCqiDistr_15',0))==0 else (dd.get('pmRadioUeRepCqiDistr_0',0)+dd.get('pmRadioUeRepCqiDistr_1',0)+dd.get('pmRadioUeRepCqiDistr_2',0)+dd.get('pmRadioUeRepCqiDistr_3',0)+dd.get('pmRadioUeRepCqiDistr_4',0)+dd.get('pmRadioUeRepCqiDistr_5',0)+dd.get('pmRadioUeRepCqiDistr_6',0))/(dd.get('pmRadioUeRepCqiDistr_0',0)+dd.get('pmRadioUeRepCqiDistr_1',0)+dd.get('pmRadioUeRepCqiDistr_2',0)+dd.get('pmRadioUeRepCqiDistr_3',0)+dd.get('pmRadioUeRepCqiDistr_4',0)+dd.get('pmRadioUeRepCqiDistr_5',0)+dd.get('pmRadioUeRepCqiDistr_6',0)+dd.get('pmRadioUeRepCqiDistr_7',0)+dd.get('pmRadioUeRepCqiDistr_8',0)+dd.get('pmRadioUeRepCqiDistr_9',0)+dd.get('pmRadioUeRepCqiDistr_10',0)+dd.get('pmRadioUeRepCqiDistr_11',0)+dd.get('pmRadioUeRepCqiDistr_12',0)+dd.get('pmRadioUeRepCqiDistr_13',0)+dd.get('pmRadioUeRepCqiDistr_14',0)+dd.get('pmRadioUeRepCqiDistr_15',0))      #质差CQI<7样本点占比  类型:numeric,
    a_ul_pdcp_package_drop=dd.get('pmPdcpPktLostUl',0)      #上行PDCP SDU丢包数(个)  类型:numeric,
    a_ul_pdcp_package_total=dd.get('pmPdcpPktLostUl',0)+dd.get('pmPdcpPktReceivedUl',0)      #上行PDCP SDU包总数(个)  类型:numeric,
    a_dl_pdcp_package_drop=dd.get('pmPdcpPktDiscDlPelrUu',0)      #下行PDCP SDU丢包数(个)  类型:numeric,
    a_dl_pdcp_package_total=dd.get('pmPdcpPktReceivedDl',0)      #下行PDCP SDU包总数(个)  类型:numeric,
    a_ul_pdcp_package_drop_ratio='' if (dd.get('pmPdcpPktLostUl',0)+dd.get('pmPdcpPktReceivedUl',0))==0 else dd.get('pmPdcpPktLostUl',0)/(dd.get('pmPdcpPktLostUl',0)+dd.get('pmPdcpPktReceivedUl',0))      #上行PDCP SDU丢包率(%)  类型:numeric,
    a_ul_pdcp_package_drop_ratio_qci1=(dd.get('pmPdcpPktLostUlQci_1',0)/(dd.get('pmPdcpPktReceivedUlQci_1',0)+dd.get('pmPdcpPktLostUlQci_1',0)+0.000001))      #上行PDCP SDU丢包率(QCI1)(%)  类型:numeric,
    a_dl_pdcp_package_drop_ratio_qci1=(dd.get('pmPdcpPktDiscDlPelrUuQci_1',0)/(dd.get('pmPdcpPktReceivedDlQci_1',0)+0.00001))      #下行PDCP SDU丢包率(QCI1)(%)  类型:numeric,
    a_dl_pdcp_package_discard_ratio='' if dd.get('pmPdcpPktReceivedDl',0)==0 else dd.get('pmPdcpPktDiscDlPelr',0)/dd.get('pmPdcpPktReceivedDl',0)      #下行PDCP SDU弃包率(%)  类型:numeric,
    a_ul_speed_mbps='' if dd.get('pmUeThpTimeUl',0)==0 else dd.get('pmUeThpVolUl',0)/dd.get('pmUeThpTimeUl',0)      #小区级上行单用户平均感知速率(Mbps)  类型:numeric,
    a_dl_speed_mbps='' if dd.get('pmUeThpTimeDl',0)==0 else (dd.get('pmPdcpVolDlDrb',0)-dd.get('pmPdcpVolDlDrbLastTTI',0))/dd.get('pmUeThpTimeDl',0)      #小区级下行单用户平均感知速率(Mbps)  类型:numeric,
    a_rrc_req=dd['pmRrcConnEstabAtt']-dd['pmRrcConnEstabAttReatt']      #RRC建立请求次数(次)  类型:numeric,
    a_rrc_suc=dd['pmRrcConnEstabSucc']      #RRC建立成功次数 (次)  类型:numeric,
    a_rrc_congest=dd['pmRrcConnEstabFailLic']+dd.get('pmRrcConnEstabFailLackOfResources',0)      #RRC连接建立拥塞次数(次)  类型:numeric,
    a_rrc_avg='' if dd.get('pmRrcConnLevSamp',0)==0 else dd['pmRrcConnLevSum']/dd.get('pmRrcConnLevSamp',0)      #RRC连接平均数(个)  类型:numeric,
    a_rrc_max=dd.get('pmRrcConnMax',0)      #RRC连接最大数(个)  类型:numeric,
    a_erab_req=dd['pmErabEstabAttInit']+dd['pmErabEstabAttAdded']      #E-RAB建立请求次数(次)  类型:numeric,
    a_erab_req_qci3=dd.get('pmErabEstabAttInitQci_3',0)+dd.get('pmErabEstabAttAddedQci_3',0)      #E-RAB建立请求次数(QCI3)(次)  类型:numeric,
    a_erab_req_qci4=dd.get('pmErabEstabAttInitQci_4',0)+dd.get('pmErabEstabAttAddedQci_4',0)      #E-RAB建立请求次数(QCI4)(次)  类型:numeric,
    a_erab_req_qci6=dd.get('pmErabEstabAttInitQci_6',0)+dd.get('pmErabEstabAttAddedQci_6',0)      #E-RAB建立请求次数(QCI6)(次)  类型:numeric,
    a_erab_req_qci7=dd.get('pmErabEstabAttInitQci_7',0)+dd.get('pmErabEstabAttAddedQci_7',0)      #E-RAB建立请求次数(QCI7)(次)  类型:numeric,
    a_erab_req_qci8=dd.get('pmErabEstabAttInitQci_8',0)+dd.get('pmErabEstabAttAddedQci_8',0)      #E-RAB建立请求次数(QCI8)(次)  类型:numeric,
    a_erab_req_qci9=dd.get('pmErabEstabAttInitQci_9',0)+dd.get('pmErabEstabAttAddedQci_9',0)      #E-RAB建立请求次数(QCI9)(次)  类型:numeric,
    a_erab_suc=dd['pmErabEstabSuccInit']+dd['pmErabEstabSuccAdded']      #E-RAB建立成功次数(次)  类型:numeric,
    a_erab_suc_qci3=dd.get('pmErabEstabSuccInitQci_3',0)+dd.get('pmErabEstabSuccAddedQci_3',0)      #E-RAB建立成功次数(QCI3)(次)  类型:numeric,
    a_erab_suc_qci4=dd.get('pmErabEstabSuccInitQci_4',0)+dd.get('pmErabEstabSuccAddedQci_4',0)      #E-RAB建立成功次数(QCI4)(次)  类型:numeric,
    a_erab_suc_qci6=dd.get('pmErabEstabSuccInitQci_6',0)+dd.get('pmErabEstabSuccAddedQci_6',0)      #E-RAB建立成功次数(QCI6)(次)  类型:numeric,
    a_erab_suc_qci7=dd.get('pmErabEstabSuccInitQci_7',0)+dd.get('pmErabEstabSuccAddedQci_7',0)      #E-RAB建立成功次数(QCI7)(次)  类型:numeric,
    a_erab_suc_qci8=dd.get('pmErabEstabSuccInitQci_8',0)+dd.get('pmErabEstabSuccAddedQci_8',0)      #E-RAB建立成功次数(QCI8)(次)  类型:numeric,
    a_erab_suc_qci9=dd.get('pmErabEstabSuccInitQci_9',0)+dd.get('pmErabEstabSuccAddedQci_9',0)      #E-RAB建立成功次数(QCI9)(次)  类型:numeric,
    a_erab_congest=dd.get('pmErabEstabFailAddedRnlS1Cause25',0)+dd.get('pmErabEstabFailInitRnlS1Cause25',0)+dd.get('pmErabEstabFailAddedTransS1Cause0',0)+dd.get('pmErabEstabFailInitTransS1Cause0',0)      #E-RAB建立拥塞次数(次)  类型:numeric,
    a_s1_signaling_att=dd['pmS1SigConnEstabAtt']      #S1信令连接建立尝试次数(次)  类型:numeric,
    a_s1_signaling_suc=dd['pmS1SigConnEstabSucc']      #S1信令连接建立成功次数(次)  类型:numeric,
    a_s1_signaling_suc_r='' if dd['pmS1SigConnEstabAtt']==0 else dd['pmS1SigConnEstabSucc']/dd['pmS1SigConnEstabAtt']      #S1信令连接建立成功率(%)  类型:numeric,
    a_erab_suc_r='' if (dd['pmErabEstabAttInit']+dd['pmErabEstabAttAdded'])==0 else (dd['pmErabEstabSuccInit']+dd['pmErabEstabSuccAdded'])/(dd['pmErabEstabAttInit']+dd['pmErabEstabAttAdded'])      #E-RAB建立成功率(%)  类型:numeric,
    a_erab_suc_r_qci1='' if (dd.get('pmErabEstabAttInitQci_1',0)+dd.get('pmErabEstabAttAddedQci_1',0))==0 else (dd.get('pmErabEstabSuccInitQci_1',0)+dd.get('pmErabEstabSuccAddedQci_1',0))/(dd.get('pmErabEstabAttInitQci_1',0)+dd.get('pmErabEstabAttAddedQci_1',0))      #E-RAB建立成功率(QCI1)(%)  类型:numeric,
    a_erab_suc_r_qci2='' if (dd.get('pmErabEstabAttInitQci_2',0)+dd.get('pmErabEstabAttAddedQci_2',0))==0 else (dd.get('pmErabEstabSuccInitQci_2',0)+dd.get('pmErabEstabSuccAddedQci_2',0))/(dd.get('pmErabEstabAttInitQci_2',0)+dd.get('pmErabEstabAttAddedQci_2',0))      #E-RAB建立成功率(QCI2)(%)  类型:numeric,
    a_erab_suc_r_qci3='' if (dd.get('pmErabEstabAttInitQci_3',0)+dd.get('pmErabEstabAttAddedQci_3',0))==0 else (dd.get('pmErabEstabSuccInitQci_3',0)+dd.get('pmErabEstabSuccAddedQci_3',0))/(dd.get('pmErabEstabAttInitQci_3',0)+dd.get('pmErabEstabAttAddedQci_3',0))      #E-RAB建立成功率(QCI3)(%)  类型:numeric,
    a_erab_suc_r_qci4='' if (dd.get('pmErabEstabAttInitQci_4',0)+dd.get('pmErabEstabAttAddedQci_4',0))==0 else (dd.get('pmErabEstabSuccInitQci_4',0)+dd.get('pmErabEstabSuccAddedQci_4',0))/(dd.get('pmErabEstabAttInitQci_4',0)+dd.get('pmErabEstabAttAddedQci_4',0))      #E-RAB建立成功率(QCI4)(%)  类型:numeric,
    a_erab_suc_r_qci5='' if (dd.get('pmErabEstabAttInitQci_5',0)+dd.get('pmErabEstabAttAddedQci_5',0))==0 else (dd.get('pmErabEstabSuccInitQci_5',0)+dd.get('pmErabEstabSuccAddedQci_5',0))/(dd.get('pmErabEstabAttInitQci_5',0)+dd.get('pmErabEstabAttAddedQci_5',0))      #E-RAB建立成功率(QCI5)(%)  类型:numeric,
    a_radio_conn_suc_r='' if (dd['pmRrcConnEstabAtt']-dd['pmRrcConnEstabAttReatt'])*(dd['pmErabEstabAttInit']+dd['pmErabEstabAttAdded'])==0 else (dd['pmRrcConnEstabSucc']/(dd['pmRrcConnEstabAtt']-dd['pmRrcConnEstabAttReatt']))*((dd['pmErabEstabSuccInit']+dd['pmErabEstabSuccAdded'])/(dd['pmErabEstabAttInit']+dd['pmErabEstabAttAdded']))      #无线接通率(%)  类型:numeric,
    a_erab_abnormal_release=dd['pmErabRelAbnormalEnbAct']+dd['pmErabRelAbnormalMmeAct']      #E-RAB异常释放次数(次)  类型:numeric,
    a_uecontext_abnormal_release=dd['pmUeCtxtRelAbnormalEnbAct']      #UE上下文异常释放次数(次)  类型:numeric,
    a_uecontext_release=dd.get('pmUeCtxtRelNormalEnb',0)+dd.get('pmUeCtxtRelMme',0)+dd.get('pmUeCtxtRelAbnormalEnb',0)      #UE上下文正常释放次数(次)  类型:numeric,
    a_lte_drop_r='' if (dd.get('pmErabRelAbnormalEnb',0)+dd.get('pmErabRelNormalEnb',0)+dd.get('pmErabRelMme',0))==0 else (dd['pmErabRelAbnormalEnbAct']+dd['pmErabRelAbnormalMmeAct'])/(dd.get('pmErabRelAbnormalEnb',0)+dd.get('pmErabRelNormalEnb',0)+dd.get('pmErabRelMme',0))      #LTE业务掉线率(%)  类型:numeric,
    a_lte_drop=dd.get('pmErabRelAbnormalEnb',0)+dd.get('pmErabRelNormalEnb',0)+dd.get('pmErabRelMme',0)      #LTE业务释放次数(次)  类型:numeric,
    a_ho_pingpang=dd.get('pmHoOscIntraF',0)+dd.get('pmHoOscInterF',0)      #乒乓切换次数  类型:numeric,
    a_ho_out_suc_r='' if (dd.get('pmHoPrepAttLteIntraF',0)+dd.get('pmHoPrepAttLteInterF',0))*(dd.get('pmHoExeAttLteIntraF',0)+dd.get('pmHoExeAttLteInterF',0))==0 else ((dd.get('pmHoPrepSuccLteIntraF',0)+dd.get('pmHoPrepSuccLteInterF',0))/(dd.get('pmHoPrepAttLteIntraF',0)+dd.get('pmHoPrepAttLteInterF',0)))*((dd.get('pmHoExeSuccLteIntraF',0)+dd.get('pmHoExeSuccLteInterF',0))/(dd.get('pmHoExeAttLteIntraF',0)+dd.get('pmHoExeAttLteInterF',0)))      #切换出成功率(%)  类型:numeric,
    a_ul_tra_mb=dd['pmPdcpVolUlDrb']/8000      #空口上行业务流量(MByte)  类型:numeric,
    a_dl_tra_mb=dd['pmPdcpVolDlDrb']/8000      #空口下行业务流量(MByte)  类型:numeric,
    a_total_tra_mb=(dd['pmPdcpVolUlDrb']+dd['pmPdcpVolDlDrb'])/8000      #空口总业务量(MByte)  类型:numeric,
    a_rrc_fail_license=dd['pmRrcConnEstabFailLic']      #由于RRCLicense受限导致RRC连接失败的次数  类型:numeric,
    a_rrc_conn_suc=dd['pmRrcConnEstabSucc']      #RRC连接建立成功次数  类型:numeric,
    a_ul_prb_utilization='' if dd.get('pmPrbAvailUl',0)==0 else (dd.get('pmPrbUsedUlDtch',0)+dd.get('pmPrbUsedUlSrb',0))/(dd.get('pmPrbAvailUl',0))      #上行PRB平均利用率(%)  类型:numeric,
    a_dl_prb_utilization='' if dd.get('pmPrbUsedDlFirstTrans',0)*dd.get('pmPrbAvailDl',0)==0 else ((dd.get('pmPrbUsedDlBcch',0)+dd.get('pmPrbUsedDlDtch',0)+dd.get('pmPrbUsedDlPcch',0)+dd.get('pmPrbUsedDlSrbFirstTrans',0))*(1+dd.get('pmPrbUsedDlReTrans',0)/dd.get('pmPrbUsedDlFirstTrans',0)))/dd.get('pmPrbAvailDl',0)      #下行PRB平均利用率(%)  类型:numeric,
    a_rrc_congest_license_r='' if (dd.get('pmRrcConnEstabAtt',0)-dd.get('pmRrcConnEstabAttReatt',0))==0 else dd.get('pmRrcConnEstabFailLic',0)/(dd.get('pmRrcConnEstabAtt',0)-dd.get('pmRrcConnEstabAttReatt',0))      #由于RRCLicense受限导致的RRC连接拥塞率  类型:numeric,
    a_double_link_r=0 if (dd.get('pmRadioTxRankDistr_1',0)+dd.get('pmRadioTxRankDistr_2',0))==0 else dd.get('pmRadioTxRankDistr_2',0)/(dd.get('pmRadioTxRankDistr_1',0)+dd.get('pmRadioTxRankDistr_2',0))      #双流占比  类型:numeric,
    a_radio_resource_utilization=''      #无线资源利用率(%)  不支持  类型:numeric,
    a_sgnb_add_req=''      #辅站（SgNB）添加请求次数（eNB侧）不支持  类型:numeric,
    a_sgnb_add_suc=''      #辅站（SgNB）添加成功次数（eNB侧）不支持  类型:numeric,
    a_sgnb_add_suc_r=''      #辅站（SgNB）添加成功率（eNB侧）不支持  类型:numeric,
    a_dl_16qam_utilization='' if (dd.get('pmMacHarqDlAck64qam',0)+dd.get('pmMacHarqDlAck16qam',0)+dd.get('pmMacHarqDlAckQpsk',0))==0 else dd.get('pmMacHarqDlAck16qam',0)/(dd.get('pmMacHarqDlAck64qam',0)+dd.get('pmMacHarqDlAck16qam',0)+dd.get('pmMacHarqDlAckQpsk',0))      #下行16QAM使用比例(%)  类型:numeric,
    a_dl_64qam_utilization='' if (dd.get('pmMacHarqDlAck64qam',0)+dd.get('pmMacHarqDlAck16qam',0)+dd.get('pmMacHarqDlAckQpsk',0))==0 else dd.get('pmMacHarqDlAck64qam',0)/(dd.get('pmMacHarqDlAck64qam',0)+dd.get('pmMacHarqDlAck16qam',0)+dd.get('pmMacHarqDlAckQpsk',0))      #下行64QAM使用比例(%)  类型:numeric,
    a_erab_congest_radio=dd.get('pmErabEstabFailInitRnlS1Cause25',0)+dd.get('pmErabEstabFailAddedRnlS1Cause25',0)      #无线资源受限导致的E-RAB建立拥塞次数(次)  类型:numeric,
    a_erab_congest_trans=dd.get('pmErabEstabFailAddedTransS1Cause0',0)+dd.get('pmErabEstabFailInitTransS1Cause0',0)      #传输资源拥塞导致的E-RAB建立拥塞次数(次)  类型:numeric,
    a_erab_fail_ue=dd.get('pmErabEstabFailInitRnlS1Cause26',0)+dd.get('pmErabEstabFailAddedRnlS1Cause26',0)      #E-RAB建立失败次数（UE无响应）(次)  类型:numeric,
    a_erab_fail_core=''      #E-RAB建立失败次数（核心网问题）(次) 不支持 类型:numeric,
    a_erab_fail_trans=''      #E-RAB建立失败次数（传输层问题）(次) 不支持 类型:numeric,
    a_erab_fail_radio=''      #E-RAB建立失败次数（无线层问题）(次) 不支持 类型:numeric,
    a_erab_fail_resource=''      #E-RAB建立失败次数（无线资源不足）(次) 不支持 类型:numeric,
    a_rrc_release_csfb=dd.get('pmUeCtxtRelCsfbGsm',0)+dd.get('pmUeCtxtRelCsfbWcdma',0)+dd.get('pmUeCtxtRelCsfbGsmEm',0)+dd.get('pmUeCtxtRelCsfbWcdmaEm',0)      #CSFB触发的RRC连接释放次数(次)  类型:numeric,
    a_dl_high_msc_utilization=''      #下行高阶调制占比 不支持 类型:numeric,
    a_ul_high_msc_utilization=''      #上行高阶调制占比 不支持 类型:numeric,
    a_csfb_suc_r= 0 if (dd.get('pmUeCtxtEstabAttCsfb',0)+dd.get('pmUeCtxtModAttCsfb',0))==0 else (dd.get('pmUeCtxtRelCsfbGsm',0)+dd.get('pmUeCtxtRelCsfbWcdma',0)+dd.get('pmUeCtxtRelCsfbGsmEm',0)+dd.get('pmUeCtxtRelCsfbWcdmaEm',0))/(dd.get('pmUeCtxtEstabAttCsfb',0)+dd.get('pmUeCtxtModAttCsfb',0))      #CSFB回落成功率  类型:numeric,
    a_s1_ho_out_req=dd.get('pmHoPrepOutS1AttInterEnb','')      #eNB间S1切换出请求次数(次)  类型:numeric,
    a_s1_ho_out_suc=dd.get('pmHoExecOutS1SuccInterEnb','')      #eNB间S1切换出成功次数(次)  类型:numeric,
    a_x2_ho_out_req=dd.get('pmHoPrepOutX2AttInterEnb','')      #eNB间X2切换出请求次数(次)  类型:numeric,
    a_x2_ho_out_suc=dd.get('pmHoExecOutX2SuccInterEnb','')      #eNB间X2切换出成功次数(次)  类型:numeric,
    a_erab_req_qci1=dd.get('pmErabEstabAttInitQci_1',0)+dd.get('pmErabEstabAttAddedQci_1',0)      #E-RAB建立请求次数(QCI1)(次)  类型:numeric,
    a_erab_req_qci5=dd.get('pmErabEstabAttInitQci_5',0)+dd.get('pmErabEstabAttAddedQci_5',0)      #E-RAB建立请求次数(QCI5)(次)  类型:numeric,
    a_erab_suc_qci1=dd.get('pmErabEstabSuccInitQci_1',0)+dd.get('pmErabEstabSuccAddedQci_1',0)      #E-RAB建立成功次数(QCI1)(次)  类型:numeric,
    a_erab_suc_qci5=dd.get('pmErabEstabSuccInitQci_5',0)+dd.get('pmErabEstabSuccAddedQci_5',0)      #E-RAB建立成功次数(QCI5)(次)  类型:numeric,
    a_erab_normal_qci1=dd.get('pmErabRelNormalEnbQci_1',0)+dd.get('pmErabRelMmeQci_1',0)+dd.get('pmErabRelAbnormalEnbQci_1',0)      #E-RAB正常释放次数(QCI1)(次)  类型:numeric,
    a_erab_abnormal_qci1=dd.get('pmErabRelAbnormalEnbActQci_1',0)+dd.get('pmErabRelAbnormalMmeActQci_1',0)      #E-RAB异常释放次数(QCI1)(次)  类型:numeric,
    a_lte_drop_r_qci1='' if ((dd.get('pmErabRelAbnormalEnbQci_1',0)+dd.get('pmErabRelNormalEnbQci_1',0)+dd.get('pmErabRelMmeQci_1',0)))==0 else (dd.get('pmErabRelAbnormalEnbActQci_1',0)+dd.get('pmErabRelAbnormalMmeActQci_1',0))/((dd.get('pmErabRelAbnormalEnbQci_1',0)+dd.get('pmErabRelNormalEnbQci_1',0)+dd.get('pmErabRelMmeQci_1',0)))      #LTE业务掉线率(QCI1)(%)  类型:numeric,
    a_lru_blind=dd.get('pmUeCtxtRelSCWcdma',0)      #LTE-UTRAN系统间重定向请求次数(盲重定向)(次)  类型:numeric,
    a_lru_not_blind=dd.get('pmUeCtxtRelSCWcdma',0)      #LTE-UTRAN系统间重定向请求次数(非盲重定向)(次)  类型:numeric
    a_succoutintraenb=''      #eNB内切换出成功次数 无法只在pm统计 类型:numeric,
    a_attoutintraenb=''      #eNB内切换出请求次数 无法只在pm统计 类型:numeric,
    a_rru_puschprbassn=(dd.get('pmPrbUsedUlDtch',0)+dd.get('pmPrbUsedUlSrb',0))/interval/1000      #上行PUSCH_PRB占用数  类型:numeric,
    a_rru_puschprbtot=dd.get('pmPrbAvailUl',0)/interval/1000      #上行PUSCH_PRB可用数  类型:numeric,
    a_rru_pdschprbassn='' if dd.get('pmPrbUsedDlFirstTrans',0)*interval==0 else (dd.get('pmPrbUsedDlDtch',0)+ dd.get('pmPrbUsedDlBcch',0)+ dd.get('pmPrbUsedDlPcch',0)+dd.get('pmPrbUsedDlSrbFirstTrans',0)*(1+dd.get('pmPrbUsedDlReTrans',0)/dd.get('pmPrbUsedDlFirstTrans',0)))/interval/1000      #下行PUSCH_PRB占用数  类型:numeric,
    a_rru_pdschprbtot=dd.get('pmPrbAvailDl',0)/interval/1000      #下行PUSCH_PRB可用数  类型:numeric,
    a_effectiveconnmean='' if dd.get('pmRrcConnLevSamp',0)==0 else dd['pmRrcConnLevSum']/dd.get('pmRrcConnLevSamp',0)      #有效RRC连接平均数  类型:numeric,
    a_effectiveconnmax=dd.get('pmRrcConnMax',0)      #有效RRC连接最大数  类型:numeric,
    a_pdcch_signal_occupy_ratio='' if (dd.get('pmPdcchCceUtil_0',0)+dd.get('pmPdcchCceUtil_1',0)+dd.get('pmPdcchCceUtil_2',0)+dd.get('pmPdcchCceUtil_3',0)+dd.get('pmPdcchCceUtil_4',0)+dd.get('pmPdcchCceUtil_5',0)+dd.get('pmPdcchCceUtil_6',0)+dd.get('pmPdcchCceUtil_7',0)+dd.get('pmPdcchCceUtil_8',0)+dd.get('pmPdcchCceUtil_9',0)+dd.get('pmPdcchCceUtil_10',0)+dd.get('pmPdcchCceUtil_11',0)+dd.get('pmPdcchCceUtil_12',0)+dd.get('pmPdcchCceUtil_13',0)+dd.get('pmPdcchCceUtil_14',0)+dd.get('pmPdcchCceUtil_15',0)+dd.get('pmPdcchCceUtil_16',0)+dd.get('pmPdcchCceUtil_17',0)+dd.get('pmPdcchCceUtil_18',0)+dd.get('pmPdcchCceUtil_19',0))==0 else  ((dd.get('pmPdcchCceUtil_0',0))*2.5+(dd.get('pmPdcchCceUtil_1',0))*7.5+(dd.get('pmPdcchCceUtil_2',0))*12.5+(dd.get('pmPdcchCceUtil_3',0))*17.5+(dd.get('pmPdcchCceUtil_4',0))*22.5+(dd.get('pmPdcchCceUtil_5',0))*27.5+(dd.get('pmPdcchCceUtil_6',0))*32.5+(dd.get('pmPdcchCceUtil_7',0))*37.5+(dd.get('pmPdcchCceUtil_8',0))*42.5+(dd.get('pmPdcchCceUtil_9',0))*47.5+(dd.get('pmPdcchCceUtil_10',0))*52.5+(dd.get('pmPdcchCceUtil_11',0))*57.5+(dd.get('pmPdcchCceUtil_12',0))*62.5+(dd.get('pmPdcchCceUtil_13',0))*67.5+(dd.get('pmPdcchCceUtil_14',0))*72.5+(dd.get('pmPdcchCceUtil_15',0))*77.5+(dd.get('pmPdcchCceUtil_16',0))*82.5+(dd.get('pmPdcchCceUtil_17',0))*87.5+(dd.get('pmPdcchCceUtil_18',0))*92.5+(dd.get('pmPdcchCceUtil_19',0))*97.5)/(dd.get('pmPdcchCceUtil_0',0)+dd.get('pmPdcchCceUtil_1',0)+dd.get('pmPdcchCceUtil_2',0)+dd.get('pmPdcchCceUtil_3',0)+dd.get('pmPdcchCceUtil_4',0)+dd.get('pmPdcchCceUtil_5',0)+dd.get('pmPdcchCceUtil_6',0)+dd.get('pmPdcchCceUtil_7',0)+dd.get('pmPdcchCceUtil_8',0)+dd.get('pmPdcchCceUtil_9',0)+dd.get('pmPdcchCceUtil_10',0)+dd.get('pmPdcchCceUtil_11',0)+dd.get('pmPdcchCceUtil_12',0)+dd.get('pmPdcchCceUtil_13',0)+dd.get('pmPdcchCceUtil_14',0)+dd.get('pmPdcchCceUtil_15',0)+dd.get('pmPdcchCceUtil_16',0)+dd.get('pmPdcchCceUtil_17',0)+dd.get('pmPdcchCceUtil_18',0)+dd.get('pmPdcchCceUtil_19',0))      #PDCCH信道CCE占用率(%)(下行利用率PDCCH)  类型:numeric,
    a_rru_pdcchcceutil=(dd.get('pmPdcchCceUtil_0',0))*2.5+(dd.get('pmPdcchCceUtil_1',0))*7.5+(dd.get('pmPdcchCceUtil_2',0))*12.5+(dd.get('pmPdcchCceUtil_3',0))*17.5+(dd.get('pmPdcchCceUtil_4',0))*22.5+(dd.get('pmPdcchCceUtil_5',0))*27.5+(dd.get('pmPdcchCceUtil_6',0))*32.5+(dd.get('pmPdcchCceUtil_7',0))*37.5+(dd.get('pmPdcchCceUtil_8',0))*42.5+(dd.get('pmPdcchCceUtil_9',0))*47.5+(dd.get('pmPdcchCceUtil_10',0))*52.5+(dd.get('pmPdcchCceUtil_11',0))*57.5+(dd.get('pmPdcchCceUtil_12',0))*62.5+(dd.get('pmPdcchCceUtil_13',0))*67.5+(dd.get('pmPdcchCceUtil_14',0))*72.5+(dd.get('pmPdcchCceUtil_15',0))*77.5+(dd.get('pmPdcchCceUtil_16',0))*82.5+(dd.get('pmPdcchCceUtil_17',0))*87.5+(dd.get('pmPdcchCceUtil_18',0))*92.5+(dd.get('pmPdcchCceUtil_19',0))*97.5      #PDCCH信道CCE占用个数  类型:numeric,
    a_rru_pdcchcceavail=(dd.get('pmPdcchCceUtil_0',0)+dd.get('pmPdcchCceUtil_1',0)+dd.get('pmPdcchCceUtil_2',0)+dd.get('pmPdcchCceUtil_3',0)+dd.get('pmPdcchCceUtil_4',0)+dd.get('pmPdcchCceUtil_5',0)+dd.get('pmPdcchCceUtil_6',0)+dd.get('pmPdcchCceUtil_7',0)+dd.get('pmPdcchCceUtil_8',0)+dd.get('pmPdcchCceUtil_9',0)+dd.get('pmPdcchCceUtil_10',0)+dd.get('pmPdcchCceUtil_11',0)+dd.get('pmPdcchCceUtil_12',0)+dd.get('pmPdcchCceUtil_13',0)+dd.get('pmPdcchCceUtil_14',0)+dd.get('pmPdcchCceUtil_15',0)+dd.get('pmPdcchCceUtil_16',0)+dd.get('pmPdcchCceUtil_17',0)+dd.get('pmPdcchCceUtil_18',0)+dd.get('pmPdcchCceUtil_19',0))      #PDCCH信道CCE可用个数  类型:numeric,
    a_succexecinc=''      #切换入成功次数 不支持 类型:numeric,
    a_succconnreestab_nonsrccell=dd.get('pmRrcConnReestSucc',0)      #RRC连接重建成功次数(非源侧小区)  类型:numeric,
    a_rrc_reconn_rate='' if (dd.get('pmRrcConnReestAtt',0)+dd.get('pmRrcConnEstabAtt',0))==0 else dd.get('pmRrcConnReestAtt',0)/(dd.get('pmRrcConnReestAtt',0)+dd.get('pmRrcConnEstabAtt',0))      #RRC连接重建比例(%)  类型:numeric,
    a_enb_handover_succ_rate=''      #eNB内切换出成功率(%) 无法在pm完成统计  类型:numeric,
    a_down_pdcch_ch_cce_occ_rate='' if (dd.get('pmPdcchCceUtil_0',0)+dd.get('pmPdcchCceUtil_1',0)+dd.get('pmPdcchCceUtil_2',0)+dd.get('pmPdcchCceUtil_3',0)+dd.get('pmPdcchCceUtil_4',0)+dd.get('pmPdcchCceUtil_5',0)+dd.get('pmPdcchCceUtil_6',0)+dd.get('pmPdcchCceUtil_7',0)+dd.get('pmPdcchCceUtil_8',0)+dd.get('pmPdcchCceUtil_9',0)+dd.get('pmPdcchCceUtil_10',0)+dd.get('pmPdcchCceUtil_11',0)+dd.get('pmPdcchCceUtil_12',0)+dd.get('pmPdcchCceUtil_13',0)+dd.get('pmPdcchCceUtil_14',0)+dd.get('pmPdcchCceUtil_15',0)+dd.get('pmPdcchCceUtil_16',0)+dd.get('pmPdcchCceUtil_17',0)+dd.get('pmPdcchCceUtil_18',0)+dd.get('pmPdcchCceUtil_19',0))==0 else ((dd.get('pmPdcchCceUtil_0',0))*2.5+(dd.get('pmPdcchCceUtil_1',0))*7.5+(dd.get('pmPdcchCceUtil_2',0))*12.5+(dd.get('pmPdcchCceUtil_3',0))*17.5+(dd.get('pmPdcchCceUtil_4',0))*22.5+(dd.get('pmPdcchCceUtil_5',0))*27.5+(dd.get('pmPdcchCceUtil_6',0))*32.5+(dd.get('pmPdcchCceUtil_7',0))*37.5+(dd.get('pmPdcchCceUtil_8',0))*42.5+(dd.get('pmPdcchCceUtil_9',0))*47.5+(dd.get('pmPdcchCceUtil_10',0))*52.5+(dd.get('pmPdcchCceUtil_11',0))*57.5+(dd.get('pmPdcchCceUtil_12',0))*62.5+(dd.get('pmPdcchCceUtil_13',0))*67.5+(dd.get('pmPdcchCceUtil_14',0))*72.5+(dd.get('pmPdcchCceUtil_15',0))*77.5+(dd.get('pmPdcchCceUtil_16',0))*82.5+(dd.get('pmPdcchCceUtil_17',0))*87.5+(dd.get('pmPdcchCceUtil_18',0))*92.5+(dd.get('pmPdcchCceUtil_19',0))*97.5)/(dd.get('pmPdcchCceUtil_0',0)+dd.get('pmPdcchCceUtil_1',0)+dd.get('pmPdcchCceUtil_2',0)+dd.get('pmPdcchCceUtil_3',0)+dd.get('pmPdcchCceUtil_4',0)+dd.get('pmPdcchCceUtil_5',0)+dd.get('pmPdcchCceUtil_6',0)+dd.get('pmPdcchCceUtil_7',0)+dd.get('pmPdcchCceUtil_8',0)+dd.get('pmPdcchCceUtil_9',0)+dd.get('pmPdcchCceUtil_10',0)+dd.get('pmPdcchCceUtil_11',0)+dd.get('pmPdcchCceUtil_12',0)+dd.get('pmPdcchCceUtil_13',0)+dd.get('pmPdcchCceUtil_14',0)+dd.get('pmPdcchCceUtil_15',0)+dd.get('pmPdcchCceUtil_16',0)+dd.get('pmPdcchCceUtil_17',0)+dd.get('pmPdcchCceUtil_18',0)+dd.get('pmPdcchCceUtil_19',0))      #下行PDCCH信道CCE占用率(%)  类型:numeric,
    a_down_pdcp_sdu_avg_delay='' if dd['pmPdcpLatPktTransDl']==0 else dd['pmPdcpLatTimeDl']/dd['pmPdcpLatPktTransDl']      #下行PDCP SDU平均时延(ms)  类型:numeric,
    a_mr_sinrul_gt0_ratio='' if (dd.get('pmSinrPucchDistr_0',0)+dd.get('pmSinrPucchDistr_1',0)+dd.get('pmSinrPucchDistr_2',0)+dd.get('pmSinrPucchDistr_3',0)+dd.get('pmSinrPucchDistr_4',0)+dd.get('pmSinrPucchDistr_5',0)+dd.get('pmSinrPucchDistr_6',0)+dd.get('pmSinrPucchDistr_7',0))==0 else (dd.get('pmSinrPucchDistr_6',0)+dd.get('pmSinrPucchDistr_7',0))/(dd.get('pmSinrPucchDistr_0',0)+dd.get('pmSinrPucchDistr_1',0)+dd.get('pmSinrPucchDistr_2',0)+dd.get('pmSinrPucchDistr_3',0)+dd.get('pmSinrPucchDistr_4',0)+dd.get('pmSinrPucchDistr_5',0)+dd.get('pmSinrPucchDistr_6',0)+dd.get('pmSinrPucchDistr_7',0))      #上行SINR大于0占比  类型:numeric,
    a_mr_sinrul_gt0_ratio_fz=dd.get('pmSinrPucchDistr_6','')+dd.get('pmSinrPucchDistr_7','')      #上行SINR大于0占比分子  类型:numeric,
    a_mr_sinrul_gt0_ratio_fm=dd.get('pmSinrPucchDistr_0','')+dd.get('pmSinrPucchDistr_1','')+dd.get('pmSinrPucchDistr_2','')+dd.get('pmSinrPucchDistr_3','')+dd.get('pmSinrPucchDistr_4','')+dd.get('pmSinrPucchDistr_5','')+dd.get('pmSinrPucchDistr_6','')+dd.get('pmSinrPucchDistr_7','')      #上行SINR大于0占比分母  类型:numeric,
    a_vendor='ERICSSON'      #厂商  类型:varchar,
    a_lte_wireless_drop_ratio_cell='' if dd.get('pmUeCtxtEstabSucc',0)==0 else dd.get('pmUeCtxtRelAbnormalEnbAct',0)/dd.get('pmUeCtxtEstabSucc',0)      #小区无线掉线率  类型:numeric
    a_pdcp_sdu_vol_ul_plmn1=dd.get('pmFlexPdcpVolUlDrb_Plmn0',0)/8000      #空口上行业务流量（PDCP）(联通46001)(MByte)  类型:numeric
    a_pdcp_sdu_vol_dl_plmn1=dd.get('pmFlexPdcpVolDlDrb_Plmn0',0)/8000      #空口下行业务流量（PDCP）(联通46001)(MByte)  类型:numeric
    a_effectiveconnmean_plmn1='' if dd.get('pmRrcConnLevSamp',0)==0 else dd.get('pmFlexRrcConnSum_Plmn0',0)/dd.get('pmRrcConnLevSamp',0)      #RRC连接平均数(联通46001)  类型:numeric
    a_erab_abnormal_plmn1=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn0',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn0',0)      #E-RAB异常释放总次数(联通46001)  类型:numeric
    a_erab_normal_plmn1=dd.get('pmFlexErabRelNormalEnb_Plmn0',0)+dd.get('pmFlexErabRelMme_Plmn0',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn0',0)      #E-RAB正常释放次数(联通46001)  类型:numeric
    a_pdcp_sdu_vol_ul_plmn2=dd.get('pmFlexPdcpVolUlDrb_Plmn1',0)/8000      #空口上行业务流量（PDCP）(电信46011)(MByte)  类型:numeric
    a_pdcp_sdu_vol_dl_plmn2=dd.get('pmFlexPdcpVolDlDrb_Plmn1',0)/8000      #空口下行业务流量（PDCP）(电信46011)(MByte)  类型:numeric
    a_effectiveconnmean_plmn2='' if dd.get('pmRrcConnLevSamp',0)==0 else dd.get('pmFlexRrcConnSum_Plmn1',0)/dd.get('pmRrcConnLevSamp',0)      #RRC连接平均数(电信46011)  类型:numeric
    a_erab_abnormal_plmn2=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn1',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn1',0)      #E-RAB异常释放总次数(电信46011)  类型:numeric
    a_erab_normal_plmn2=dd.get('pmFlexErabRelNormalEnb_Plmn1',0)+dd.get('pmFlexErabRelMme_Plmn1',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn1',0)      #E-RAB正常释放次数(电信46011)  类型:numeric
    a_erab_ini_setup_att_plmn1_qci1=dd.get('pmFlexErabEstabAttInit_Plmn0Qci1','')        #初始E-RAB建立请求次数_联通_QCI1    numeric
    a_erab_ini_setup_att_plmn1_qci2=dd.get('pmFlexErabEstabAttInit_Plmn0Qci2',0)        #初始E-RAB建立请求次数_联通_QCI2    numeric
    a_erab_ini_setup_att_plmn1_qci3=dd.get('pmFlexErabEstabAttInit_Plmn0Qci3',0)        #初始E-RAB建立请求次数_联通_QCI3    numeric
    a_erab_ini_setup_att_plmn1_qci4=dd.get('pmFlexErabEstabAttInit_Plmn0Qci4',0)        #初始E-RAB建立请求次数_联通_QCI4    numeric
    a_erab_ini_setup_att_plmn1_qci5=dd.get('pmFlexErabEstabAttInit_Plmn0Qci5',0)        #初始E-RAB建立请求次数_联通_QCI5    numeric
    a_erab_ini_setup_att_plmn1_qci6=dd.get('pmFlexErabEstabAttInit_Plmn0Qci6',0)        #初始E-RAB建立请求次数_联通_QCI6    numeric
    a_erab_ini_setup_att_plmn1_qci7=dd.get('pmFlexErabEstabAttInit_Plmn0Qci7',0)        #初始E-RAB建立请求次数_联通_QCI7    numeric
    a_erab_ini_setup_att_plmn1_qci8=dd.get('pmFlexErabEstabAttInit_Plmn0Qci8',0)        #初始E-RAB建立请求次数_联通_QCI8    numeric
    a_erab_ini_setup_att_plmn1_qci9=dd.get('pmFlexErabEstabAttInit_Plmn0Qci9',0)        #初始E-RAB建立请求次数_联通_QCI9    numeric
    a_erab_ini_setup_succ_plmn1_qci1=dd.get('pmFlexErabEstabSuccInit_Plmn0Qci1',0)        #初始E-RAB建立成功次数_联通_QCI1    numeric
    a_erab_ini_setup_succ_plmn1_qci2=dd.get('pmFlexErabEstabSuccInit_Plmn0Qci2',0)        #初始E-RAB建立成功次数_联通_QCI2    numeric
    a_erab_ini_setup_succ_plmn1_qci3=dd.get('pmFlexErabEstabSuccInit_Plmn0Qci3',0)        #初始E-RAB建立成功次数_联通_QCI3    numeric
    a_erab_ini_setup_succ_plmn1_qci4=dd.get('pmFlexErabEstabSuccInit_Plmn0Qci4',0)        #初始E-RAB建立成功次数_联通_QCI4    numeric
    a_erab_ini_setup_succ_plmn1_qci5=dd.get('pmFlexErabEstabSuccInit_Plmn0Qci5',0)        #初始E-RAB建立成功次数_联通_QCI5    numeric
    a_erab_ini_setup_succ_plmn1_qci6=dd.get('pmFlexErabEstabSuccInit_Plmn0Qci6',0)        #初始E-RAB建立成功次数_联通_QCI6    numeric
    a_erab_ini_setup_succ_plmn1_qci7=dd.get('pmFlexErabEstabSuccInit_Plmn0Qci7',0)        #初始E-RAB建立成功次数_联通_QCI7    numeric
    a_erab_ini_setup_succ_plmn1_qci8=dd.get('pmFlexErabEstabSuccInit_Plmn0Qci8',0)        #初始E-RAB建立成功次数_联通_QCI8    numeric
    a_erab_ini_setup_succ_plmn1_qci9=dd.get('pmFlexErabEstabSuccInit_Plmn0Qci9',0)        #初始E-RAB建立成功次数_联通_QCI9    numeric
    a_erab_add_setup_att_plmn1_qci1=dd.get('pmFlexErabEstabAttAdded_Plmn0Qci1',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn0Qci1',0)        #附加E-RAB建立请求次数_联通_QCI1    numeric
    a_erab_add_setup_att_plmn1_qci2=dd.get('pmFlexErabEstabAttAdded_Plmn0Qci2',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn0Qci2',0)        #附加E-RAB建立请求次数_联通_QCI2    numeric
    a_erab_add_setup_att_plmn1_qci3=dd.get('pmFlexErabEstabAttAdded_Plmn0Qci3',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn0Qci3',0)        #附加E-RAB建立请求次数_联通_QCI3    numeric
    a_erab_add_setup_att_plmn1_qci4=dd.get('pmFlexErabEstabAttAdded_Plmn0Qci4',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn0Qci4',0)        #附加E-RAB建立请求次数_联通_QCI4    numeric
    a_erab_add_setup_att_plmn1_qci5=dd.get('pmFlexErabEstabAttAdded_Plmn0Qci5',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn0Qci5',0)        #附加E-RAB建立请求次数_联通_QCI5    numeric
    a_erab_add_setup_att_plmn1_qci6=dd.get('pmFlexErabEstabAttAdded_Plmn0Qci6',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn0Qci6',0)        #附加E-RAB建立请求次数_联通_QCI6    numeric
    a_erab_add_setup_att_plmn1_qci7=dd.get('pmFlexErabEstabAttAdded_Plmn0Qci7',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn0Qci7',0)        #附加E-RAB建立请求次数_联通_QCI7    numeric
    a_erab_add_setup_att_plmn1_qci8=dd.get('pmFlexErabEstabAttAdded_Plmn0Qci8',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn0Qci8',0)        #附加E-RAB建立请求次数_联通_QCI8    numeric
    a_erab_add_setup_att_plmn1_qci9=dd.get('pmFlexErabEstabAttAdded_Plmn0Qci9',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn0Qci9',0)        #附加E-RAB建立请求次数_联通_QCI9    numeric
    a_erab_add_setup_succ_plmn1_qci1=dd.get('pmFlexErabEstabSuccAdded_Plmn0Qci1',0)        #附加E-RAB建立成功次数_联通_QCI1    numeric
    a_erab_add_setup_succ_plmn1_qci2=dd.get('pmFlexErabEstabSuccAdded_Plmn0Qci2',0)        #附加E-RAB建立成功次数_联通_QCI2    numeric
    a_erab_add_setup_succ_plmn1_qci3=dd.get('pmFlexErabEstabSuccAdded_Plmn0Qci3',0)        #附加E-RAB建立成功次数_联通_QCI3    numeric
    a_erab_add_setup_succ_plmn1_qci4=dd.get('pmFlexErabEstabSuccAdded_Plmn0Qci4',0)        #附加E-RAB建立成功次数_联通_QCI4    numeric
    a_erab_add_setup_succ_plmn1_qci5=dd.get('pmFlexErabEstabSuccAdded_Plmn0Qci5',0)        #附加E-RAB建立成功次数_联通_QCI5    numeric
    a_erab_add_setup_succ_plmn1_qci6=dd.get('pmFlexErabEstabSuccAdded_Plmn0Qci6',0)        #附加E-RAB建立成功次数_联通_QCI6    numeric
    a_erab_add_setup_succ_plmn1_qci7=dd.get('pmFlexErabEstabSuccAdded_Plmn0Qci7',0)        #附加E-RAB建立成功次数_联通_QCI7    numeric
    a_erab_add_setup_succ_plmn1_qci8=dd.get('pmFlexErabEstabSuccAdded_Plmn0Qci8',0)        #附加E-RAB建立成功次数_联通_QCI8    numeric
    a_erab_add_setup_succ_plmn1_qci9=dd.get('pmFlexErabEstabSuccAdded_Plmn0Qci9',0)        #附加E-RAB建立成功次数_联通_QCI9    numeric
    a_erab_abnormal_plmn1_qci1=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn0Qci1',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn0Qci1',0)        #E-RAB异常释放总次数_联通_QCI1    numeric
    a_erab_abnormal_plmn1_qci2=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn0Qci2',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn0Qci2',0)        #E-RAB异常释放总次数_联通_QCI2    numeric
    a_erab_abnormal_plmn1_qci3=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn0Qci3',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn0Qci3',0)        #E-RAB异常释放总次数_联通_QCI3    numeric
    a_erab_abnormal_plmn1_qci4=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn0Qci4',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn0Qci4',0)        #E-RAB异常释放总次数_联通_QCI4    numeric
    a_erab_abnormal_plmn1_qci5=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn0Qci5',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn0Qci5',0)        #E-RAB异常释放总次数_联通_QCI5    numeric
    a_erab_abnormal_plmn1_qci6=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn0Qci6',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn0Qci6',0)        #E-RAB异常释放总次数_联通_QCI6    numeric
    a_erab_abnormal_plmn1_qci7=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn0Qci7',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn0Qci7',0)        #E-RAB异常释放总次数_联通_QCI7    numeric
    a_erab_abnormal_plmn1_qci8=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn0Qci8',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn0Qci8',0)        #E-RAB异常释放总次数_联通_QCI8    numeric
    a_erab_abnormal_plmn1_qci9=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn0Qci9',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn0Qci9',0)        #E-RAB异常释放总次数_联通_QCI9    numeric
    a_erab_normal_plmn1_qci1=dd.get('pmFlexErabRelNormalEnb_Plmn0Qci1',0)+dd.get('pmFlexErabRelMme_Plmn0Qci1',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn0Qci1',0)        #E-RAB正常释放次数_联通_QCI1    numeric
    a_erab_normal_plmn1_qci2=dd.get('pmFlexErabRelNormalEnb_Plmn0Qci2',0)+dd.get('pmFlexErabRelMme_Plmn0Qci2',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn0Qci2',0)        #E-RAB正常释放次数_联通_QCI2    numeric
    a_erab_normal_plmn1_qci3=dd.get('pmFlexErabRelNormalEnb_Plmn0Qci3',0)+dd.get('pmFlexErabRelMme_Plmn0Qci3',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn0Qci3',0)        #E-RAB正常释放次数_联通_QCI3    numeric
    a_erab_normal_plmn1_qci4=dd.get('pmFlexErabRelNormalEnb_Plmn0Qci4',0)+dd.get('pmFlexErabRelMme_Plmn0Qci4',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn0Qci4',0)        #E-RAB正常释放次数_联通_QCI4    numeric
    a_erab_normal_plmn1_qci5=dd.get('pmFlexErabRelNormalEnb_Plmn0Qci5',0)+dd.get('pmFlexErabRelMme_Plmn0Qci5',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn0Qci5',0)        #E-RAB正常释放次数_联通_QCI5    numeric
    a_erab_normal_plmn1_qci6=dd.get('pmFlexErabRelNormalEnb_Plmn0Qci6',0)+dd.get('pmFlexErabRelMme_Plmn0Qci6',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn0Qci6',0)        #E-RAB正常释放次数_联通_QCI6    numeric
    a_erab_normal_plmn1_qci7=dd.get('pmFlexErabRelNormalEnb_Plmn0Qci7',0)+dd.get('pmFlexErabRelMme_Plmn0Qci7',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn0Qci7',0)        #E-RAB正常释放次数_联通_QCI7    numeric
    a_erab_normal_plmn1_qci8=dd.get('pmFlexErabRelNormalEnb_Plmn0Qci8',0)+dd.get('pmFlexErabRelMme_Plmn0Qci8',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn0Qci8',0)        #E-RAB正常释放次数_联通_QCI8    numeric
    a_erab_normal_plmn1_qci9=dd.get('pmFlexErabRelNormalEnb_Plmn0Qci9',0)+dd.get('pmFlexErabRelMme_Plmn0Qci9',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn0Qci9',0)        #E-RAB正常释放次数_联通_QCI9    numeric
    a_erab_ini_setup_att_plmn2_qci1=dd.get('pmFlexErabEstabAttInit_Plmn1Qci1',0)        #初始E-RAB建立请求次数_电信_QCI1    numeric
    a_erab_ini_setup_att_plmn2_qci2=dd.get('pmFlexErabEstabAttInit_Plmn1Qci2',0)        #初始E-RAB建立请求次数_电信_QCI2    numeric
    a_erab_ini_setup_att_plmn2_qci3=dd.get('pmFlexErabEstabAttInit_Plmn1Qci3',0)        #初始E-RAB建立请求次数_电信_QCI3    numeric
    a_erab_ini_setup_att_plmn2_qci4=dd.get('pmFlexErabEstabAttInit_Plmn1Qci4',0)        #初始E-RAB建立请求次数_电信_QCI4    numeric
    a_erab_ini_setup_att_plmn2_qci5=dd.get('pmFlexErabEstabAttInit_Plmn1Qci5',0)        #初始E-RAB建立请求次数_电信_QCI5    numeric
    a_erab_ini_setup_att_plmn2_qci6=dd.get('pmFlexErabEstabAttInit_Plmn1Qci6',0)        #初始E-RAB建立请求次数_电信_QCI6    numeric
    a_erab_ini_setup_att_plmn2_qci7=dd.get('pmFlexErabEstabAttInit_Plmn1Qci7',0)        #初始E-RAB建立请求次数_电信_QCI7    numeric
    a_erab_ini_setup_att_plmn2_qci8=dd.get('pmFlexErabEstabAttInit_Plmn1Qci8',0)        #初始E-RAB建立请求次数_电信_QCI8    numeric
    a_erab_ini_setup_att_plmn2_qci9=dd.get('pmFlexErabEstabAttInit_Plmn1Qci9',0)        #初始E-RAB建立请求次数_电信_QCI9    numeric
    a_erab_ini_setup_succ_plmn2_qci1=dd.get('pmFlexErabEstabSuccInit_Plmn1Qci1',0)        #初始E-RAB建立成功次数_电信_QCI1    numeric
    a_erab_ini_setup_succ_plmn2_qci2=dd.get('pmFlexErabEstabSuccInit_Plmn1Qci2',0)        #初始E-RAB建立成功次数_电信_QCI2    numeric
    a_erab_ini_setup_succ_plmn2_qci3=dd.get('pmFlexErabEstabSuccInit_Plmn1Qci3',0)        #初始E-RAB建立成功次数_电信_QCI3    numeric
    a_erab_ini_setup_succ_plmn2_qci4=dd.get('pmFlexErabEstabSuccInit_Plmn1Qci4',0)        #初始E-RAB建立成功次数_电信_QCI4    numeric
    a_erab_ini_setup_succ_plmn2_qci5=dd.get('pmFlexErabEstabSuccInit_Plmn1Qci5',0)        #初始E-RAB建立成功次数_电信_QCI5    numeric
    a_erab_ini_setup_succ_plmn2_qci6=dd.get('pmFlexErabEstabSuccInit_Plmn1Qci6',0)        #初始E-RAB建立成功次数_电信_QCI6    numeric
    a_erab_ini_setup_succ_plmn2_qci7=dd.get('pmFlexErabEstabSuccInit_Plmn1Qci7',0)        #初始E-RAB建立成功次数_电信_QCI7    numeric
    a_erab_ini_setup_succ_plmn2_qci8=dd.get('pmFlexErabEstabSuccInit_Plmn1Qci8',0)        #初始E-RAB建立成功次数_电信_QCI8    numeric
    a_erab_ini_setup_succ_plmn2_qci9=dd.get('pmFlexErabEstabSuccInit_Plmn1Qci9',0)        #初始E-RAB建立成功次数_电信_QCI9    numeric
    a_erab_add_setup_att_plmn2_qci1=dd.get('pmFlexErabEstabAttAdded_Plmn1Qci1',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn1Qci1',0)        #附加E-RAB建立请求次数_电信_QCI1    numeric
    a_erab_add_setup_att_plmn2_qci2=dd.get('pmFlexErabEstabAttAdded_Plmn1Qci2',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn1Qci2',0)        #附加E-RAB建立请求次数_电信_QCI2    numeric
    a_erab_add_setup_att_plmn2_qci3=dd.get('pmFlexErabEstabAttAdded_Plmn1Qci3',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn1Qci3',0)        #附加E-RAB建立请求次数_电信_QCI3    numeric
    a_erab_add_setup_att_plmn2_qci4=dd.get('pmFlexErabEstabAttAdded_Plmn1Qci4',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn1Qci4',0)        #附加E-RAB建立请求次数_电信_QCI4    numeric
    a_erab_add_setup_att_plmn2_qci5=dd.get('pmFlexErabEstabAttAdded_Plmn1Qci5',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn1Qci5',0)        #附加E-RAB建立请求次数_电信_QCI5    numeric
    a_erab_add_setup_att_plmn2_qci6=dd.get('pmFlexErabEstabAttAdded_Plmn1Qci6',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn1Qci6',0)        #附加E-RAB建立请求次数_电信_QCI6    numeric
    a_erab_add_setup_att_plmn2_qci7=dd.get('pmFlexErabEstabAttAdded_Plmn1Qci7',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn1Qci7',0)        #附加E-RAB建立请求次数_电信_QCI7    numeric
    a_erab_add_setup_att_plmn2_qci8=dd.get('pmFlexErabEstabAttAdded_Plmn1Qci8',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn1Qci8',0)        #附加E-RAB建立请求次数_电信_QCI8    numeric
    a_erab_add_setup_att_plmn2_qci9=dd.get('pmFlexErabEstabAttAdded_Plmn1Qci9',0)-dd.get('pmFlexErabEstabAttAddedHoOngoing_Plmn1Qci9',0)        #附加E-RAB建立请求次数_电信_QCI9    numeric
    a_erab_add_setup_succ_plmn2_qci1=dd.get('pmFlexErabEstabSuccAdded_Plmn1Qci1',0)        #附加E-RAB建立成功次数_电信_QCI1    numeric
    a_erab_add_setup_succ_plmn2_qci2=dd.get('pmFlexErabEstabSuccAdded_Plmn1Qci2',0)        #附加E-RAB建立成功次数_电信_QCI2    numeric
    a_erab_add_setup_succ_plmn2_qci3=dd.get('pmFlexErabEstabSuccAdded_Plmn1Qci3',0)        #附加E-RAB建立成功次数_电信_QCI3    numeric
    a_erab_add_setup_succ_plmn2_qci4=dd.get('pmFlexErabEstabSuccAdded_Plmn1Qci4',0)        #附加E-RAB建立成功次数_电信_QCI4    numeric
    a_erab_add_setup_succ_plmn2_qci5=dd.get('pmFlexErabEstabSuccAdded_Plmn1Qci5',0)        #附加E-RAB建立成功次数_电信_QCI5    numeric
    a_erab_add_setup_succ_plmn2_qci6=dd.get('pmFlexErabEstabSuccAdded_Plmn1Qci6',0)        #附加E-RAB建立成功次数_电信_QCI6    numeric
    a_erab_add_setup_succ_plmn2_qci7=dd.get('pmFlexErabEstabSuccAdded_Plmn1Qci7',0)        #附加E-RAB建立成功次数_电信_QCI7    numeric
    a_erab_add_setup_succ_plmn2_qci8=dd.get('pmFlexErabEstabSuccAdded_Plmn1Qci8',0)        #附加E-RAB建立成功次数_电信_QCI8    numeric
    a_erab_add_setup_succ_plmn2_qci9=dd.get('pmFlexErabEstabSuccAdded_Plmn1Qci9',0)        #附加E-RAB建立成功次数_电信_QCI9    numeric
    a_erab_abnormal_plmn2_qci1=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn1Qci1',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn1Qci1',0)        #E-RAB异常释放总次数_电信_QCI1    numeric
    a_erab_abnormal_plmn2_qci2=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn1Qci2',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn1Qci2',0)        #E-RAB异常释放总次数_电信_QCI2    numeric
    a_erab_abnormal_plmn2_qci3=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn1Qci3',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn1Qci3',0)        #E-RAB异常释放总次数_电信_QCI3    numeric
    a_erab_abnormal_plmn2_qci4=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn1Qci4',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn1Qci4',0)        #E-RAB异常释放总次数_电信_QCI4    numeric
    a_erab_abnormal_plmn2_qci5=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn1Qci5',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn1Qci5',0)        #E-RAB异常释放总次数_电信_QCI5    numeric
    a_erab_abnormal_plmn2_qci6=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn1Qci6',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn1Qci6',0)        #E-RAB异常释放总次数_电信_QCI6    numeric
    a_erab_abnormal_plmn2_qci7=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn1Qci7',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn1Qci7',0)        #E-RAB异常释放总次数_电信_QCI7    numeric
    a_erab_abnormal_plmn2_qci8=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn1Qci8',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn1Qci8',0)        #E-RAB异常释放总次数_电信_QCI8    numeric
    a_erab_abnormal_plmn2_qci9=dd.get('pmFlexErabRelAbnormalEnbAct_Plmn1Qci9',0)+dd.get('pmFlexErabRelAbnormalMmeAct_Plmn1Qci9',0)        #E-RAB异常释放总次数_电信_QCI9    numeric
    a_erab_normal_plmn2_qci1=dd.get('pmFlexErabRelNormalEnb_Plmn1Qci1',0)+dd.get('pmFlexErabRelMme_Plmn1Qci1',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn1Qci1',0)        #E-RAB正常释放次数_电信_QCI1    numeric
    a_erab_normal_plmn2_qci2=dd.get('pmFlexErabRelNormalEnb_Plmn1Qci2',0)+dd.get('pmFlexErabRelMme_Plmn1Qci2',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn1Qci2',0)        #E-RAB正常释放次数_电信_QCI2    numeric
    a_erab_normal_plmn2_qci3=dd.get('pmFlexErabRelNormalEnb_Plmn1Qci3',0)+dd.get('pmFlexErabRelMme_Plmn1Qci3',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn1Qci3',0)        #E-RAB正常释放次数_电信_QCI3    numeric
    a_erab_normal_plmn2_qci4=dd.get('pmFlexErabRelNormalEnb_Plmn1Qci4',0)+dd.get('pmFlexErabRelMme_Plmn1Qci4',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn1Qci4',0)        #E-RAB正常释放次数_电信_QCI4    numeric
    a_erab_normal_plmn2_qci5=dd.get('pmFlexErabRelNormalEnb_Plmn1Qci5',0)+dd.get('pmFlexErabRelMme_Plmn1Qci5',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn1Qci5',0)        #E-RAB正常释放次数_电信_QCI5    numeric
    a_erab_normal_plmn2_qci6=dd.get('pmFlexErabRelNormalEnb_Plmn1Qci6',0)+dd.get('pmFlexErabRelMme_Plmn1Qci6',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn1Qci6',0)        #E-RAB正常释放次数_电信_QCI6    numeric
    a_erab_normal_plmn2_qci7=dd.get('pmFlexErabRelNormalEnb_Plmn1Qci7',0)+dd.get('pmFlexErabRelMme_Plmn1Qci7',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn1Qci7',0)        #E-RAB正常释放次数_电信_QCI7    numeric
    a_erab_normal_plmn2_qci8=dd.get('pmFlexErabRelNormalEnb_Plmn1Qci8',0)+dd.get('pmFlexErabRelMme_Plmn1Qci8',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn1Qci8',0)        #E-RAB正常释放次数_电信_QCI8    numeric
    a_erab_normal_plmn2_qci9=dd.get('pmFlexErabRelNormalEnb_Plmn1Qci9',0)+dd.get('pmFlexErabRelMme_Plmn1Qci9',0)+dd.get('pmFlexErabRelAbnormalEnb_Plmn1Qci9',0)        #E-RAB正常释放次数_电信_QCI9    numeric
    a_isp='联通'        #承建运营商    
    a_share=dd.get('share','否')        #是否共享，通过cm来解析    
    a_rrc_max_plmn1=dd.get('pmRrcConnMaxPlmn0',0)        #RRC最大连接数(联通46001)    
    a_rrc_max_plmn2=dd.get('pmRrcConnMaxPlmn1',0)        #RRC最大连接数(电信46011)    
    a_dl_prb_used='' if dd.get('pmPrbUsedDlFirstTrans',0)==0 else (dd.get('pmPrbUsedDlBcch',0)+dd.get('pmPrbUsedDlDtch',0)+dd.get('pmPrbUsedDlPcch',0)+dd.get('pmPrbUsedDlSrbFirstTrans',0)*(1+dd.get('pmPrbUsedDlReTrans',0)/dd.get('pmPrbUsedDlFirstTrans',0)))/interval/1000	#下行PRB平均占用数 [pmPrbUsedDlDtch+ pmPrbUsedDlBcch+ pmPrbUsedDlPcch+pmPrbUsedDlSrbFirstTrans*(1+pmPrbUsedDlReTrans/pmPrbUsedDlFirstTrans)]/统计时长(ms)
    print(a_sdate,a_eci,a_dl_prb_used,dd['pmPrbUsedDlBcch'],dd['pmPrbUsedDlDtch'],dd['pmPrbUsedDlPcch'],dd['pmPrbUsedDlSrbFirstTrans'],dd['pmPrbUsedDlReTrans'],dd['pmPrbUsedDlFirstTrans'])
    a_dl_prb_total=dd.get('pmPrbAvailDl',0)/interval/1000			#下行PRB可用数
    return [a_sdate,a_eci,a_drop_duration,a_total_duration,a_cell_available_ratio,a_noise,a_cqi0,a_cqi1,a_cqi2,a_cqi3,a_cqi4,a_cqi5,a_cqi6,a_cqi7,a_cqi8,a_cqi9,a_cqi10,a_cqi11,a_cqi12,a_cqi13,a_cqi14,a_cqi15,a_cqi_avg,a_cqi_ge7,a_cqi_le7,a_ul_pdcp_package_drop,a_ul_pdcp_package_total,a_dl_pdcp_package_drop,a_dl_pdcp_package_total,a_ul_pdcp_package_drop_ratio,a_ul_pdcp_package_drop_ratio_qci1,a_dl_pdcp_package_drop_ratio_qci1,a_dl_pdcp_package_discard_ratio,a_ul_speed_mbps,a_dl_speed_mbps,a_rrc_req,a_rrc_suc,a_rrc_congest,a_rrc_avg,a_rrc_max,a_erab_req,a_erab_req_qci3,a_erab_req_qci4,a_erab_req_qci6,a_erab_req_qci7,a_erab_req_qci8,a_erab_req_qci9,a_erab_suc,a_erab_suc_qci3,a_erab_suc_qci4,a_erab_suc_qci6,a_erab_suc_qci7,a_erab_suc_qci8,a_erab_suc_qci9,a_erab_congest,a_s1_signaling_att,a_s1_signaling_suc,a_s1_signaling_suc_r,a_erab_suc_r,a_erab_suc_r_qci1,a_erab_suc_r_qci2,a_erab_suc_r_qci3,a_erab_suc_r_qci4,a_erab_suc_r_qci5,a_radio_conn_suc_r,a_erab_abnormal_release,a_uecontext_abnormal_release,a_uecontext_release,a_lte_drop_r,a_lte_drop,a_ho_pingpang,a_ho_out_suc_r,a_ul_tra_mb,a_dl_tra_mb,a_total_tra_mb,a_rrc_fail_license,a_rrc_conn_suc,a_ul_prb_utilization,a_dl_prb_utilization,a_rrc_congest_license_r,a_double_link_r,a_radio_resource_utilization,a_sgnb_add_req,a_sgnb_add_suc,a_sgnb_add_suc_r,a_dl_16qam_utilization,a_dl_64qam_utilization,a_erab_congest_radio,a_erab_congest_trans,a_erab_fail_ue,a_erab_fail_core,a_erab_fail_trans,a_erab_fail_radio,a_erab_fail_resource,a_rrc_release_csfb,a_dl_high_msc_utilization,a_ul_high_msc_utilization,a_csfb_suc_r,a_s1_ho_out_req,a_s1_ho_out_suc,a_x2_ho_out_req,a_x2_ho_out_suc,
        a_erab_req_qci1,a_erab_req_qci5,a_erab_suc_qci1,a_erab_suc_qci5,a_erab_normal_qci1,a_erab_abnormal_qci1,a_lte_drop_r_qci1,a_lru_blind,a_lru_not_blind,a_succoutintraenb,a_attoutintraenb,a_rru_puschprbassn,a_rru_puschprbtot,a_rru_pdschprbassn,a_rru_pdschprbtot,a_effectiveconnmean,a_effectiveconnmax,a_pdcch_signal_occupy_ratio,a_rru_pdcchcceutil,a_rru_pdcchcceavail,a_succexecinc,a_succconnreestab_nonsrccell,a_rrc_reconn_rate,a_enb_handover_succ_rate,a_down_pdcch_ch_cce_occ_rate,a_down_pdcp_sdu_avg_delay,a_mr_sinrul_gt0_ratio,a_mr_sinrul_gt0_ratio_fz,a_mr_sinrul_gt0_ratio_fm,a_vendor,a_lte_wireless_drop_ratio_cell,
        a_pdcp_sdu_vol_ul_plmn1,a_pdcp_sdu_vol_dl_plmn1,a_effectiveconnmean_plmn1,a_erab_abnormal_plmn1,a_erab_normal_plmn1,a_pdcp_sdu_vol_ul_plmn2,a_pdcp_sdu_vol_dl_plmn2,a_effectiveconnmean_plmn2,a_erab_abnormal_plmn2,a_erab_normal_plmn2,a_erab_ini_setup_att_plmn1_qci1,a_erab_ini_setup_att_plmn1_qci2,a_erab_ini_setup_att_plmn1_qci3,a_erab_ini_setup_att_plmn1_qci4,a_erab_ini_setup_att_plmn1_qci5,a_erab_ini_setup_att_plmn1_qci6,a_erab_ini_setup_att_plmn1_qci7,a_erab_ini_setup_att_plmn1_qci8,a_erab_ini_setup_att_plmn1_qci9,a_erab_ini_setup_succ_plmn1_qci1,a_erab_ini_setup_succ_plmn1_qci2,a_erab_ini_setup_succ_plmn1_qci3,a_erab_ini_setup_succ_plmn1_qci4,a_erab_ini_setup_succ_plmn1_qci5,a_erab_ini_setup_succ_plmn1_qci6,a_erab_ini_setup_succ_plmn1_qci7,a_erab_ini_setup_succ_plmn1_qci8,a_erab_ini_setup_succ_plmn1_qci9,a_erab_add_setup_att_plmn1_qci1,a_erab_add_setup_att_plmn1_qci2,a_erab_add_setup_att_plmn1_qci3,a_erab_add_setup_att_plmn1_qci4,a_erab_add_setup_att_plmn1_qci5,a_erab_add_setup_att_plmn1_qci6,a_erab_add_setup_att_plmn1_qci7,a_erab_add_setup_att_plmn1_qci8,a_erab_add_setup_att_plmn1_qci9,a_erab_add_setup_succ_plmn1_qci1,a_erab_add_setup_succ_plmn1_qci2,a_erab_add_setup_succ_plmn1_qci3,a_erab_add_setup_succ_plmn1_qci4,a_erab_add_setup_succ_plmn1_qci5,a_erab_add_setup_succ_plmn1_qci6,a_erab_add_setup_succ_plmn1_qci7,a_erab_add_setup_succ_plmn1_qci8,a_erab_add_setup_succ_plmn1_qci9,a_erab_abnormal_plmn1_qci1,a_erab_abnormal_plmn1_qci2,a_erab_abnormal_plmn1_qci3,a_erab_abnormal_plmn1_qci4,a_erab_abnormal_plmn1_qci5,a_erab_abnormal_plmn1_qci6,a_erab_abnormal_plmn1_qci7,a_erab_abnormal_plmn1_qci8,a_erab_abnormal_plmn1_qci9,a_erab_normal_plmn1_qci1,a_erab_normal_plmn1_qci2,a_erab_normal_plmn1_qci3,a_erab_normal_plmn1_qci4,a_erab_normal_plmn1_qci5,a_erab_normal_plmn1_qci6,a_erab_normal_plmn1_qci7,a_erab_normal_plmn1_qci8,a_erab_normal_plmn1_qci9,a_erab_ini_setup_att_plmn2_qci1,a_erab_ini_setup_att_plmn2_qci2,a_erab_ini_setup_att_plmn2_qci3,a_erab_ini_setup_att_plmn2_qci4,a_erab_ini_setup_att_plmn2_qci5,a_erab_ini_setup_att_plmn2_qci6,a_erab_ini_setup_att_plmn2_qci7,a_erab_ini_setup_att_plmn2_qci8,a_erab_ini_setup_att_plmn2_qci9,a_erab_ini_setup_succ_plmn2_qci1,a_erab_ini_setup_succ_plmn2_qci2,a_erab_ini_setup_succ_plmn2_qci3,a_erab_ini_setup_succ_plmn2_qci4,a_erab_ini_setup_succ_plmn2_qci5,a_erab_ini_setup_succ_plmn2_qci6,a_erab_ini_setup_succ_plmn2_qci7,a_erab_ini_setup_succ_plmn2_qci8,a_erab_ini_setup_succ_plmn2_qci9,a_erab_add_setup_att_plmn2_qci1,a_erab_add_setup_att_plmn2_qci2,a_erab_add_setup_att_plmn2_qci3,a_erab_add_setup_att_plmn2_qci4,a_erab_add_setup_att_plmn2_qci5,a_erab_add_setup_att_plmn2_qci6,a_erab_add_setup_att_plmn2_qci7,a_erab_add_setup_att_plmn2_qci8,a_erab_add_setup_att_plmn2_qci9,a_erab_add_setup_succ_plmn2_qci1,a_erab_add_setup_succ_plmn2_qci2,a_erab_add_setup_succ_plmn2_qci3,a_erab_add_setup_succ_plmn2_qci4,a_erab_add_setup_succ_plmn2_qci5,a_erab_add_setup_succ_plmn2_qci6,a_erab_add_setup_succ_plmn2_qci7,a_erab_add_setup_succ_plmn2_qci8,a_erab_add_setup_succ_plmn2_qci9,a_erab_abnormal_plmn2_qci1,a_erab_abnormal_plmn2_qci2,a_erab_abnormal_plmn2_qci3,a_erab_abnormal_plmn2_qci4,a_erab_abnormal_plmn2_qci5,a_erab_abnormal_plmn2_qci6,a_erab_abnormal_plmn2_qci7,a_erab_abnormal_plmn2_qci8,a_erab_abnormal_plmn2_qci9,a_erab_normal_plmn2_qci1,a_erab_normal_plmn2_qci2,a_erab_normal_plmn2_qci3,a_erab_normal_plmn2_qci4,a_erab_normal_plmn2_qci5,a_erab_normal_plmn2_qci6,a_erab_normal_plmn2_qci7,a_erab_normal_plmn2_qci8,a_erab_normal_plmn2_qci9,a_isp,a_share,
        a_rrc_max_plmn1,a_rrc_max_plmn2,
        #2022-6-15添加
        a_dl_prb_used,a_dl_prb_total]


#def deal_with_file(fname):
def deal_with_file(xmltext):
    kpi=[]
    out={}
    ns={'ns1':'http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec',
        'ns2':'http://www.w3.org/2001/XMLSchema-instance',
        'ns3':'http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec'}
    #tree = ET.parse(fname)
    #root = tree.getroot()
    root=ET.fromstring(xmltext)
    #print(root.tag)
    #print(root.attrib)
    fileHeader = root.find('ns1:fileHeader',ns)
    localDn=fileHeader.find('ns1:fileSender',ns).attrib['localDn']
    elementType=fileHeader.find('ns1:fileSender',ns).attrib['elementType']
    beginTime=fileHeader.find('ns1:measCollec',ns).attrib['beginTime']
    beginTime=beginTime[0:10]+' '+beginTime[11:19]  #2022-03-13T03:45:00+00:00 to 2022-03-13 03:45:00
    beginTime=(datetime.datetime.strptime(beginTime,'%Y-%m-%d %H:%M:%S')+datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    measData=root.find('ns1:measData',ns)
    #for measInfo in [i for i in measData.findall('ns1:measInfo',ns) if i.attrib['measInfoId']=='PM=1,PmGroup=EUtranCellFDD' and (i.find('ns1:job',ns).attrib['jobId']=='USERDEF-LTECONTERS.Cont.Y.STATS' or i.find('ns1:job',ns).attrib['jobId']=='USERDEF-LTE_Flexible_counter.Cont.Y.STATS')]:
    for measInfo in [i for i in measData.findall('ns1:measInfo',ns) if i.attrib['measInfoId']=='PM=1,PmGroup=EUtranCellFDD' or i.attrib['measInfoId']=='PM=1,PmGroup=EUtranCellRelation' or i.attrib['measInfoId']=='PM=1,PmGroup=PmFlexEUtranCellFDD']:
        if measInfo.attrib['measInfoId']=='PM=1,PmGroup=EUtranCellFDD' and measInfo.find('ns1:job',ns).attrib['jobId']=='USERDEF-LTECONTERS.Cont.Y.STATS':
            duration=int(measInfo.find('ns1:repPeriod',ns).attrib['duration'][2:-1])
            #print(duration)
            #print(measInfo)
            #print(measInfo.find('ns1:job',ns).attrib['jobId'])
            #print(len(measInfo.findall('ns1:measType',ns)))
            pmList=[i.text for i in list(measInfo) if i.tag=='{http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec}measType']
            #_=[print(i.text) for i in list(measInfo) if i.tag=='{http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec}measType']
            for measValue in [i for i in list(measInfo) if i.tag=='{http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec}measValue']:
                measObjLdn=measValue.attrib['measObjLdn']     #小区名节点  measObjLdn="ManagedElement=CZ_CX_cuierzhuang2100M1ERR-share,ENodeBFunction=1,EUtranCellFDD=CZ_CX_CEZZliangzhanDXERF-1"
                measObjLdn=[i for i in measObjLdn.split(',') if i.startswith('EUtranCellFDD=')][0].split('=')[1]
                #print(measObjLdn)
                d_=dict(zip(pmList,[i.text for i in list(measValue)]))
                for c in pmList:
                    if ',' not in d_[c]:
                        d_[c]=int(d_[c])
                    elif c.endswith('Qci') or c.startswith('pmBadCovMeasTime') or c.startswith('pmErab'):
                        d_[c]=d_[c].split(',')
                        for i in zip(d_[c][1::2],d_[c][2::2]):
                            #try:
                            d_[c+'_'+i[0]]= 0 if i[1]==' ' else int(i[1])
                            #except:
                            #    logger.info(measObjLdn+'  '+c)
                            #    d_[c+'_'+i[0]]=int(i[1])
                    else:
                        d_[c]=d_[c].split(',')
                        for inx, val in enumerate(d_[c]):
                            d_[c+'_%d'%inx]=int(val)
                if measObjLdn in out:
                    out[measObjLdn].update(d_)
                else:
                    out[measObjLdn]=d_
        elif (measInfo.attrib['measInfoId']=='PM=1,PmGroup=EUtranCellFDD' or measInfo.attrib['measInfoId']=='PM=1,PmGroup=PmFlexEUtranCellFDD') and (measInfo.find('ns1:job',ns).attrib['jobId']=='USERDEF-LTE_Flexible_counter.Cont.Y.STATS' or measInfo.find('ns1:job',ns).attrib['jobId']=='USERDEF-LTE_Flexible_counter_20220613.Cont.Y.STATS' or measInfo.find('ns1:job',ns).attrib['jobId']=='USERDEF-L900.Cont.Y.STATS'):
            #包含有分运营商的counter
            duration=int(measInfo.find('ns1:repPeriod',ns).attrib['duration'][2:-1])
            pmList=[i.text for i in list(measInfo) if i.tag=='{http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec}measType']
            for measValue in [i for i in list(measInfo) if i.tag=='{http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec}measValue']:
                measObjLdn=measValue.attrib['measObjLdn']     #小区名节点
                measObjLdn=[i for i in measObjLdn.split(',') if i.startswith('EUtranCellFDD=')][0].split('=')[1]
                #print(measObjLdn)
                d_=dict(zip(pmList,[i.text for i in list(measValue)]))
                for c in pmList:
                    if ',' not in d_[c]:
                        d_[c]=int(d_[c])
                    else:
                        logger.warning(c+'  '+d_[c])
                if measObjLdn in out:
                    out[measObjLdn].update(d_)
                else:
                    out[measObjLdn]=d_
        elif measInfo.attrib['measInfoId']=='PM=1,PmGroup=EUtranCellFDD' and measInfo.find('ns1:job',ns).attrib['jobId']=='PREDEF_Lrat':
            duration=int(measInfo.find('ns1:repPeriod',ns).attrib['duration'][2:-1])
            pmList=[i.text for i in list(measInfo) if i.tag=='{http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec}measType']
            for measValue in [i for i in list(measInfo) if i.tag=='{http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec}measValue']:
                measObjLdn=measValue.attrib['measObjLdn']     #小区名节点
                measObjLdn=[i for i in measObjLdn.split(',') if i.startswith('EUtranCellFDD=')][0].split('=')[1]
                #print(measObjLdn)
                d_=dict(zip(pmList,[i.text for i in list(measValue)]))
                for c in pmList:
                    if ',' not in d_[c]:
                        d_[c]=int(d_[c])
                    #else:
                    #    print(c,d_[c])
                if measObjLdn in out:
                    out[measObjLdn].update(d_)
                else:
                    out[measObjLdn]=d_
        elif measInfo.attrib['measInfoId']=='PM=1,PmGroup=EUtranCellRelation' and measInfo.find('ns1:job',ns).attrib['jobId']=='USERDEF-LTECONTERS.Cont.Y.STATS':  #邻区对
            duration=int(measInfo.find('ns1:repPeriod',ns).attrib['duration'][2:-1])
            pmList=[i.text for i in list(measInfo) if i.tag=='{http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec}measType']
            for measValue in [i for i in list(measInfo) if i.tag=='{http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec}measValue']: #邻区对列表
                measObjLdn=measValue.attrib['measObjLdn']     #小区名节点
                measObjLdn=[i for i in measObjLdn.split(',') if i.startswith('EUtranCellFDD=')][0].split('=')[1]
                #print(measObjLdn)
                d_=dict(zip(pmList,[i.text for i in list(measValue)]))
                for c in pmList:
                    if ',' not in d_[c]:
                        d_[c]=int(d_[c])
                if measObjLdn in out:
                    for c in pmList:
                        out[measObjLdn][c]=out[measObjLdn].get(c,0)+d_[c]
                else:
                    out[measObjLdn]=d_
    for measObjLdn in out:
        if measObjLdn in cell:
            cellId=cell[measObjLdn][0]
            out[measObjLdn]['share']=cell[measObjLdn][1]
        else:
            cellId=measObjLdn
        kpi.append(deal_with_kpi(cellId,out[measObjLdn],beginTime,duration))
    return kpi


def deal_with_tar(tarName):
    kpi=[]
    with tarfile.open(tarName,'r') as tar:
        for gz in tar.getmembers()[:]:    #测试时只解析前5个，记得改回去
            #logger.info(gz.name)
            #if 'BDBG_DongMaYingErZhan2100MERF_1' in gzip.decompress(tar.extractfile(gz).read()).decode():
            if 'BDGB_XiCaiYuanUL900ERF' in gz.name:
                print(gz.name)
                kpi+=deal_with_file(gzip.decompress(tar.extractfile(gz).read()))
    return kpi


if __name__ == '__main__':
    os.chdir(sys.path[0])
    if len(sys.argv) > 1:
        ds=str(sys.argv[1])[0:8]
        hs=str(sys.argv[1])[0:10]
    else:
        ds=(datetime.datetime.now() - datetime.timedelta(hours=hour_delay)).strftime('%Y%m%d')
        hs=(datetime.datetime.now() - datetime.timedelta(hours=hour_delay)).strftime('%Y%m%d%H')
    if 'linux' in sys.platform:
        tarfiles=glob.glob('/data/esbftp/pm/4G/ERICSSON/OMC*/PM/%s/PM_A%s*.tar.gz'%(ds,hs))
        logger.info('/data/esbftp/pm/4G/ERICSSON/OMC*/PM/%s/PM_A%s*.tar.gz'%(ds,hs))
    else:
        tarfiles=glob.glob('PM_A*.tar.gz')
        logger.info('PM_A*.tar.gz')
    #获取小区相关参数
    cellcsv=glob.glob(cmpath)
    cellcsv.sort()
    cellcsv=cellcsv[-1]
    logger.info(cellcsv)
    cell=[i.split(',') for i in open(cellcsv,encoding='utf8').read().split('\n')][1:]
    cell={i[5]:[i[4],i[9]] for i in cell}
    kpi=[]
    for tarName in tarfiles:
        logger.info(tarName)
        kpi+=deal_with_tar(tarName)
    #deal_with_tar('/data/esbftp/pm/4G/ERICSSON/OMC1/PM/20220531/PM_A20220531200000_900_4GNSA.tar.gz')

    #csvName=outpath+'ericsson_4g_%s00.csv'%hs
    #if os.path.isfile(csvName):os.remove(csvName)
    #with open(csvName+'.temp','w') as f:
    #    for l in kpi:
    #        l_=[i if isinstance(i,str) else "{0:.4f}".format(i) if isinstance(i,float) else str(i) for i in l]
    #        f.write(','.join(l_)+'\n')
    #os.rename(csvName+'.temp',csvName)
