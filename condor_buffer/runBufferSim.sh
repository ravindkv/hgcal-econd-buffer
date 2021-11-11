#!/bin/bash

sampleType=$1
echo $sampleType
#If running on condor, checkout CMSSW and get extra libraries
if [ -z ${_CONDOR_SCRATCH_DIR} ] ; then 
    echo "Running Interactively" ; 
else
    echo "Running In Batch"
    (>&2 echo "Starting job on " `date`) # Date/time of start of job
    (>&2 echo "Running on: `uname -a`") # Condor job is running on this node
    (>&2 echo "System software: `cat /etc/redhat-release`") # Operating System on that node

    cd ${_CONDOR_SCRATCH_DIR}
    echo ${_CONDOR_SCRATCH_DIR}
    tar --strip-components=1 -zxvf ECOND_Buffer.tar.gz
    tar -zxf hgcalEnv.tar.gz
    rm hgcalEnv.tar.gz
    source hgcalEnv/bin/activate
fi
python bufferSim.py --source ${sampleType}

if [ -z ${_CONDOR_SCRATCH_DIR} ] ; then 
    echo "Running Interactively" ; 
else
    echo "Cleanup" 
    rm -r *.csv
    rm -r *.gz
fi
