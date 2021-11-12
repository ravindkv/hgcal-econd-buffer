import os
import pandas as pd

fPath_1 = '/eos/uscms/store/user/rverma/Output/cms-hgcal-econd/dataZS'
fPath_2 = 'root://cmseos.fnal.gov//store/user/rverma/Output/cms-hgcal-econd/dataZS'
f = open("gsd_ttbar_path.txt", "r")
i=0
for line in f:
    name = line.strip()
    fileName = f'%s/dataZS_%s'%(fPath_1, name.replace("root", "csv"))
    print(i, fileName)
    if i==0:
        daq_Data = pd.read_csv(fileName)
    else:
        daq_Data = pd.concat([daq_Data, pd.read_csv(fileName)], ignore_index=True)
    i+=1
print(len(daq_Data))
outFile = "dataZS_merged.csv"
daq_Data.to_csv(outFile)
os.system("rm %s/%s"%(fPath_1, outFile))
os.system("xrdcp %s %s"%(outFile, fPath_2))
os.system("mv %s ../"%outFile)
print("%s/%s"%(fPath_2, outFile))



