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
os.system("cp runDaqData_cfg.sh tmpSub/")
common_command = \
'Universe   = vanilla\n\
should_transfer_files = YES\n\
when_to_transfer_output = ON_EXIT\n\
Transfer_Input_Files =  runDaqData_cfg.sh\n\
use_x509userproxy = true\n\
Output = %s/log_$(cluster)_$(process).stdout\n\
Error  = %s/log_$(cluster)_$(process).stderr\n\
Log    = %s/log_$(cluster)_$(process).condor\n\n'%(condorLogDir, condorLogDir, condorLogDir)

#----------------------------------------
#Create jdl files
#----------------------------------------
condorOutDir = "/eos/uscms/store/user/rverma/Output/cms-hgcal-econd/ntuple"
os.system("eos root://cmseos.fnal.gov mkdir -p %s"%condorOutDir)
subFile = open('tmpSub/condorSubmit.sh','w')
f = open('gsd_ttbar_path.txt', 'r')
jdlName = 'submitJobs.jdl'
jdlFile = open('tmpSub/%s'%jdlName,'w')
jdlFile.write('Executable =  runDaqData_cfg.sh \n')
jdlFile.write(common_command)

for line in f:
    inFile = line.strip()
    outFile = "ntuple_%s"%inFile
    run_command =  \
    'arguments  = %s %s \nqueue 1\n\n' %(inFile, outFile)
    jdlFile.write(run_command)
jdlFile.close() 
