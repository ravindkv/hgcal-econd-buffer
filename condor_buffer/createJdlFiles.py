import os
os.makedirs("tmpSub")
os.system("cp runBufferSim.sh tmpSub/")
tarFile = "tmpSub/ECOND_Buffer.tar.gz"
os.system("tar -zcvf %s ../../hgcal-econd-buffer --exclude condor* --exclude hgcalEnv  --exclude .git"%tarFile)

common_command = \
'Universe   = vanilla\n\
Executable = runBufferSim.sh\n\
should_transfer_files = YES\n\
when_to_transfer_output = ON_EXIT\n\
Transfer_Input_Files = ECOND_Buffer.tar.gz, runBufferSim.sh\n\
+RequestMemory=3000\n\
use_x509userproxy = true\n\n'

freqNZS = [0, 100, 1000]
subFile = open('tmpSub/condorSubmit.sh','w')
for freq in freqNZS:
    jdlName = 'submitJobs_%s.jdl'%freq
    jdlFile = open('tmpSub/%s'%jdlName, 'w')
    jdlFile.write(common_command)
    os.makedirs("tmpSub/log_%s"%freq)
    logPath = \
    'Output=log_%s/bufferSim__$(cluster)_$(process).stdout\n\
    Error  =log_%s/bufferSim__$(cluster)_$(process).stderr\n\
    Log    =log_%s/bufferSim__$(cluster)_$(process).condor\n\n'%(freq, freq, freq)
    run_command =  'arguments  = %s \nqueue 100\n\n' %freq
    jdlFile.write(logPath)
    jdlFile.write(run_command)
    jdlFile.close() 
    subFile.write("condor_submit %s\n"%jdlName)
subFile.close()
