import os
import sys

if os.path.exists("tmpSub"):
	os.system("rm -r tmpSub")
else:
    os.makedirs("tmpSub/log")
condorLogDir = "log"
tarFile = "tmpSub/HistMain.tar.gz"
if os.path.exists("../hists"):
    os.system("rm -r ../hists")
os.system("cp runGetData.sh tmpSub/")
os.system("cp ../hgcalEnv.tar.gz tmpSub/")
os.system("tar -zcf tmpSub/geomInfo.tar.gz ../geomInfo")
os.system("cp ../getData.py tmpSub/")
common_command = \
'Universe   = vanilla\n\
should_transfer_files = YES\n\
when_to_transfer_output = ON_EXIT\n\
Transfer_Input_Files =  runGetData.sh, hgcalEnv.tar.gz, getData.py, geomInfo.tar.gz \n\
use_x509userproxy = true\n\
Output = %s/log_$(cluster)_$(process).stdout\n\
Error  = %s/log_$(cluster)_$(process).stderr\n\
Log    = %s/log_$(cluster)_$(process).condor\n\n'%(condorLogDir, condorLogDir, condorLogDir)

#----------------------------------------
#Create jdl files
#----------------------------------------
condorOutDir = "/eos/uscms/store/user/rverma/Output/cms-hgcal-econd/dataZS"
os.system("eos root://cmseos.fnal.gov mkdir -p %s"%condorOutDir)
f = open('gsd_ttbar_path.txt', 'r')
jdlName = 'submitJobs.jdl'
jdlFile = open('tmpSub/%s'%jdlName,'w')
jdlFile.write('Executable =  runGetData.sh \n')
jdlFile.write(common_command)
job=0
for line in f:
    job+=1
    inFile_ = line.strip()
    inFile = "ntuple_%s"%inFile_
    outFile = "dataZS_%s"%inFile_.replace("root", "csv")
    run_command =  \
    'arguments  = %s %s %s \nqueue 1\n\n' %(inFile, outFile, job)
    jdlFile.write(run_command)
jdlFile.close() 
