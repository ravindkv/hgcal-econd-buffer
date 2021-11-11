import uproot4
import awkward as ak
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import ROOT

import os
import sys
sys.path.insert(0, "%s/hgcalEnv/lib/python3.6/site-packages/"%os.getcwd())

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--inputFile',default="ntuple_Events_4807434_0.root")
parser.add_argument('--outputFile',default="data_Events_4807336_0.csv")
parser.add_argument('--job',default=1,type=int)
parser.add_argument('--layerStart',default=5)
parser.add_argument('--layerStop',default=9)
parser.add_argument('--chunkSize',default=10,type=int)
parser.add_argument('--maxEvents',default=None, type=int)
args = parser.parse_args()
fName = "root://cmseos.fnal.gov//store/user/rverma/Output/cms-hgcal-econd/ntuple/%s"%args.inputFile

pd.options.mode.chained_assignment = None
adcLSB_ = 100./1024.
tdcLSB_ = 10000./4096.
tdcOnsetfC_ = 60.

jobNumber_eventOffset = 100000
negZ_eventOffset      =  50000
cassette_eventOffset  =   1000

linkMap = pd.read_csv('geomInfo/eLinkInputMapFull.csv')
calibrationCells = pd.read_csv('geomInfo/calibrationCells.csv')

waferRemap = pd.read_csv('geomInfo/WaferNumberingMatch.csv')[['layer','waferu','waferv','C1_waferu','C1_waferv','Cassette']]
waferRemap.set_index(['layer','waferu','waferv'],inplace=True)

def getTree(fName): 
    treeName = 'hgcalTriggerNtuplizer/HGCalTriggerNtuple'
    print ("File %s"%fName)
    try:
        _tree = uproot4.open(fName,xrootdsource=dict(chunkbytes=1024**3, limitbytes=1024**3))[treeName]
        return _tree
    except:
        print ("---Unable to open file, skipping")
        return None

def processDF(fulldf, outputName="test.csv"):

    #if data is ADC, charge = data * adcLSB
    #else data is TDC, charge = tdcStart  + data*tdcLSB

    fulldf["charge"] = np.where(fulldf.isadc==1,fulldf.data*adcLSB_, (int(tdcOnsetfC_/adcLSB_) + 1.0)*adcLSB_ + fulldf.data*tdcLSB_)
    fulldf["charge_BXm1"] = np.where(fulldf.isadc_BXm1==1,fulldf.data_BXm1*adcLSB_, (int(tdcOnsetfC_/adcLSB_) + 1.0)*adcLSB_ + fulldf.data*tdcLSB_)

    #ZS_thr = np.array([1.03 , 1.715, 2.575]) #0.5 MIP threshold, in fC, as found in CMSSW
    ZS_thr = np.array([5, 5, 5]) #5 ADC threshold for each wafer type
    ZS_thr_BXm1 = ZS_thr*5 #2.5 MIP threshold, in fC, as found in CMSSW
    # ZS_thr = np.array([0.7, 0.7, 0.7])
    # ToA_thr = 12. # below this, we don't send ToA, above this we do, 12 fC is threshold listed in TDF

    #drop cells below ZS_thr
    #Correction for leakage from BX1, following Pedro's suggestion
    #https://github.com/cms-sw/cmssw/blob/master/SimCalorimetry/HGCalSimAlgos/interface/HGCalSiNoiseMap.icc#L18-L26
    #80fC for 120um, 160 fC for 200 um and 320 fC for 300 um
    BX1_leakage = np.array([0.066/0.934, 0.153/0.847, 0.0963/0.9037])
    fulldf['data'] = fulldf.data-fulldf.data_BXm1*BX1_leakage[fulldf.gain]
    zsCut = np.where(fulldf.isadc==0, False, fulldf.data>ZS_thr[fulldf.wafertype])
    df_ZS = fulldf.loc[zsCut]

    df_ZS['BXM1_readout'] = np.where(df_ZS.isadc_BXm1==0, False,  df_ZS.data_BXm1>ZS_thr_BXm1[df_ZS.wafertype])
    df_ZS['TOA_readout'] = (df_ZS.toa>0).astype(int)
    df_ZS['TOT_readout'] = ~df_ZS.isadc

    df_ZS['Bits'] = 16 + 8*(df_ZS.BXM1_readout + df_ZS.TOA_readout)


    df_ZS.set_index(['zside','layer','waferu','waferv'],inplace=True)
    df_ZS['HDM'] = df_ZS.wafertype==0

    df_ZS.reset_index(inplace=True)
    df_ZS.set_index(['entry','zside','layer','waferu','waferv'],inplace=True)

    df_ZS = df_ZS.reset_index().merge(linkMap,on=['HDM','cellu','cellv']).set_index(['entry','zside','layer','waferu','waferv'])

    calCellDF = df_ZS.reset_index().merge(calibrationCells,on=['HDM','cellu','cellv']).set_index(['entry','zside','layer','waferu','waferv']).fillna(0).drop('isCal',axis=1)
    calCellDF['linkChannel'] = 32
    calCellDF.loc[calCellDF.HDM,'linkChannel'] = 36
    calCellDF = calCellDF.reset_index().set_index(['HDM','eLink','linkChannel'])
    calCellDF.SRAM_read_group = linkMap.set_index(['HDM','eLink','linkChannel']).SRAM_read_group
    calCellDF = calCellDF.reset_index().set_index(['entry','zside','layer','waferu','waferv'])

    df_ZS = pd.concat([df_ZS,calCellDF]).sort_index()


    group = df_ZS.reset_index()[['entry','zside','layer','waferu','waferv','eLink','HDM','Bits','BXM1_readout','TOA_readout']].groupby(['entry','zside','layer','waferu','waferv','eLink'])

    dfBitsElink = group.sum()
    dfBitsElink['HDM'] = group[['HDM']].any()
    dfBitsElink['occ'] = group['HDM'].count()

    dfBitsElink['eRxPacket_Words'] = (dfBitsElink.Bits/32+1).astype(int) + 2


    group = dfBitsElink.reset_index()[['entry','zside','layer','waferu','waferv','HDM','occ','eRxPacket_Words']].groupby(['entry','zside','layer','waferu','waferv'])
    del dfBitsElink
    dfBits = group.sum()
    dfBits['HDM'] = group[['HDM']].any()
    dfBits['NonEmptyLinks'] = group[['HDM']].count()
    dfBits['EmptyLinks'] = np.where(dfBits.HDM,12,6) - dfBits.NonEmptyLinks


    evt_headerWords = 2
    evt_trailerWords = 2
    dfBits['TotalWords'] = evt_headerWords + dfBits.eRxPacket_Words + dfBits.EmptyLinks + evt_trailerWords

    dfBits.reset_index(inplace=True)
    dfBits.set_index(['layer','waferu','waferv'],inplace=True)

    #relabel wafers to take advantage of phi symmetry
    dfBits = dfBits.merge(waferRemap,left_index=True,right_index=True)
    dfBits.reset_index(inplace=True)
    dfBits.entry = dfBits.entry + cassette_eventOffset*dfBits.Cassette
    dfBits.waferu = dfBits.C1_waferu
    dfBits.waferv = dfBits.C1_waferv
    dfBits.drop(['C1_waferu','C1_waferv','Cassette','zside'],axis=1,inplace=True)
    dfBits.set_index(['entry','layer','waferu','waferv'],inplace=True)

    dfBits.sort_index()
    dfBits.to_csv(outputName)
    del dfBits

#----------------------------------------
#Process the tree
#----------------------------------------
_tree = getTree(fName)
Nentries = _tree.num_entries
print(Nentries)
branchesOld = ['hgcdigi_zside','hgcdigi_layer','hgcdigi_waferu','hgcdigi_waferv','hgcdigi_cellu','hgcdigi_cellv','hgcdigi_wafertype','hgcdigi_data','hgcdigi_isadc','hgcdigi_dataBXm1','hgcdigi_isadcBXm1']
branchesNew = ['hgcdigi_zside','hgcdigi_layer','hgcdigi_waferu','hgcdigi_waferv','hgcdigi_cellu','hgcdigi_cellv','hgcdigi_wafertype','hgcdigi_data_BX2','hgcdigi_isadc_BX2','hgcdigi_toa_BX2','hgcdigi_gain_BX2','hgcdigi_data_BX1','hgcdigi_isadc_BX1']

if b'hgcdigi_data' in _tree.keys():
    branches = branchesOld
else:
    branches = branchesNew

N=0
for x in _tree.iterate(branches,entry_stop=args.maxEvents,step_size=args.chunkSize):
    print(N)
    N += args.chunkSize
    layerCut = (x['hgcdigi_layer']>=args.layerStart) & (x['hgcdigi_layer']<=args.layerStop)
    df = ak.to_pandas(x[layerCut])
    df.columns = ['zside','layer','waferu','waferv','cellu','cellv','wafertype','data','isadc','toa','gain','data_BXm1','isadc_BXm1']

    #drop subentry from index
    df.reset_index('subentry',drop=True,inplace=True)
    df.reset_index(inplace=True)

    #update entry number for negative endcap
    df['entry'] = df['entry'] + args.job*jobNumber_eventOffset
    df.loc[df.zside==-1, 'entry'] = df.loc[df.zside==-1, 'entry'] + negZ_eventOffset

    processDF(df, outputName=args.outputFile)
