#!/bin/bash
inF=$1
outF=$2
job=$3
echo $sampleType
#If running on condor, checkout CMSSW and get extra libraries
if [ -z ${_CONDOR_SCRATCH_DIR} ] ; then 
    echo "Running Interactively" ; 
else
    echo "Running In Batch"
    (>&2 echo "Starting job on " `date`) # Date/time of start of job
    (>&2 echo "Running on: `uname -a`") # Condor job is running on this node
    (>&2 echo "System software: `cat /etc/redhat-release`") # Operating System on that node

    tar -zxf hgcalEnv.tar.gz 
    tar -zxf geomInfo.tar.gz
    rm hgcalEnv.tar.gz 
    rm geomInfo.tar.gz
    pwd
    source hgcalEnv/bin/activate
fi
python getData.py --inputFile=${inF} --outputFile=${outF} --job=${job}
printf "Copying output files ..."
condorOutDir=/store/user/rverma/Output/cms-hgcal-econd/dataZS
xrdcp -rf ${outF} root://cmseos.fnal.gov/$condorOutDir
echo ${_CONDOR_SCRATCH_DIR}
cd ${_CONDOR_SCRATCH_DIR}
printf "Done ";/bin/date
