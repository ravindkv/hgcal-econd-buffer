#!/bin/bash

inF=$1
outF=$2
echo $sampleType
#If running on condor, checkout CMSSW and get extra libraries
if [ -z ${_CONDOR_SCRATCH_DIR} ] ; then 
    echo "Running Interactively" ; 
else
    echo "Running In Batch"
    (>&2 echo "Starting job on " `date`) # Date/time of start of job
    (>&2 echo "Running on: `uname -a`") # Condor job is running on this node
    (>&2 echo "System software: `cat /etc/redhat-release`") # Operating System on that node

    source /cvmfs/cms.cern.ch/cmsset_default.sh
    echo $SCRAM_ARCH
    export SCRAM_ARCH=slc7_amd64_gcc900
    xrdcp root://cmseos.fnal.gov//store/user/rverma/Output/CMSSW_12_0_0_pre3.tar.gz .
    tar -zxf CMSSW_12_0_0_pre3.tar.gz
    rm CMSSW_12_0_0_pre3.tar.gz
    cd CMSSW_12_0_0_pre3/src/
    pwd
    #scramv1 b ProjectRename
    scram b -r ProjectRename
    eval `scramv1 runtime -sh`
    echo $CMSSW_BASE
    cd L1Trigger/L1THGCalUtilities/test/
fi
cmsRun daqData_cfg.py inputFiles=${inF} outputFile=${outF}
printf "Copying output files ..."
condorOutDir=/store/user/rverma/Output/cms-hgcal-econd/ntuple
xrdcp -rf ${outF} root://cmseos.fnal.gov/$condorOutDir
echo ${_CONDOR_SCRATCH_DIR}
cd ${_CONDOR_SCRATCH_DIR}
printf "Cleaning..."
rm -rf CMSSW* 
printf "Done ";/bin/date
