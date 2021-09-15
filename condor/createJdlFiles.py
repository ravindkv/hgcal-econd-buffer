import os
import sys
sys.path.insert(0, os.getcwd().replace("condor", ""))
if os.path.exists("tmpSub"):
	os.system("rm -r tmpSub")
else:
    os.makedirs("tmpSub/log")
condorLogDir = "log"
tarFile = "tmpSub/ECOND_Buffer.tar.gz"
if os.path.exists("../hists"):
    os.system("rm -r ../hists")
os.system("tar -zcvf %s ../../ECOND_Buffer --exclude condor"%tarFile)
os.system("cp runBufferSim.sh tmpSub/")
os.system("cp submitCondorJob.jdl tmpSub/")
