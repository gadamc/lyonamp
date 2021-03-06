#!/usr/bin/env python
import sys, shutil, os
from ROOT import *


if __name__ == '__main__':
  filName = sys.argv[1]
  dataPath = '/sps/edelweis/kdata/data/run12/amp'
  detectorName = sys.argv[2]
  
  smin = smax = 0
  sambamin = sambamax = 0
  channelName = 'centre ID3AB'
  if len(sys.argv) > 2:
    channelName = sys.argv[2]

  for file in allFiles:
    if os.path.basename(file).startswith(runName):
      f = KDataReader(dataPath + '/' + os.path.basename(file))
      
      e = f.GetEvent()

      for i in range(f.GetEntries()):
        f.GetEntry(i)
        for j in range(e.GetNumBolos()):
          b = e.GetBolo(j)
   
          if b.GetDetectorName() == 'ID3':
            for k in range(b.GetNumPulseRecords()):
              p = b.GetPulseRecord(k)
        
              if p.GetChannelName() == channelName:
                for n in range(p.GetNumPulseAnalysisRecords()):
                  r = p.GetPulseAnalysisRecord(n)
          
                  if r.GetName() == "KTrapKamperProto":
                    if r.IsBaseline()==0:
                      if r.GetAmp() < smin: smin = r.GetAmp()
                      if r.GetAmp() > smax: smax = r.GetAmp()
                  elif r.GetName() == "samba":
                    if r.IsBaseline() == 0:
                      if r.GetAmp() < sambamin: sambamin = r.GetAmp()
                      if r.GetAmp() > sambamax: sambamax = r.GetAmp()

  print 'trap range', smin, smax
  print 'samba range', sambamin, sambamax

  
  fout = TFile(channelName + '.root','recreate')

  trap = TH1D('trap','trap',5000,smin,smax)
  samba = TH1F('samba','samba',5000,sambamin,sambamax)
  print trap


  for file in allFiles:
    if os.path.basename(file).startswith(runName):
      f = KDataReader(dataPath + '/' + os.path.basename(file))
      
      e = f.GetEvent()

      for i in range(f.GetEntries()):
        f.GetEntry(i)
        for j in range(e.GetNumBolos()):
          b = e.GetBolo(j)
   
          if b.GetDetectorName() == 'ID3':
            for k in range(b.GetNumPulseRecords()):
              p = b.GetPulseRecord(k)
        
              if p.GetChannelName() == channelName:
                for n in range(p.GetNumPulseAnalysisRecords()):
                  r = p.GetPulseAnalysisRecord(n)
          
                  if r.GetName() == "KTrapKamperProto":
                    if r.IsBaseline()==0:
                      trap.Fill(r.GetAmp())
                  elif r.GetName() == "samba":
                    if r.IsBaseline() == 0:
                      samba.Fill(r.GetAmp())


      fout.cd()
      trap.Write()
      samba.Write()
     
  fout.Close()
