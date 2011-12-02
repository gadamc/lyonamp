#!/usr/bin/env python
import sys, shutil, os
from ROOT import *

def getHist(rootfile, name, amin, amax):
  h = fout.Get(name)
  try:
    print h.GetEntries()
  except:
    print 'making new hist:', name
    h = TH1D(name,name,5000,amin,amax)
  
  return h


if __name__ == '__main__':
  runName = sys.argv[1]
  dataPath = '/sps/edelweis/kdata/data/run12/amp'
  allFiles = os.listdir(dataPath)
  
  smin = smax = 0
  sambamin = sambamax = 0
  peakmin = peakmax = 0

  detectorName = sys.argv[2]
  channelName = sys.argv[3]
  runList = open(sys.argv[4],'r')
  calibRuns = []
  newFile = 'false'
  if len(sys.argv) > 5:
    newFile = sys.argv[5]

  for line in runList:
    calibRuns.append(line.split(' ')[0].strip('\n'))

  print runName, detectorName, channelName, newFile

  print calibRuns
  trapEventCounter = 0
  sambaEventCounter = 0

#  for item in calibRuns:
#    print 'calib run', item

  for file in allFiles:
    #print os.path.basename(file).split('_')[0], os.path.basename(file).startswith(runName), os.path.basename(file).split('_')[0] in calibRuns

    if os.path.basename(file).startswith(runName) and (os.path.basename(file).split('_')[0] in calibRuns):
      
      f = KDataReader(dataPath + '/' + os.path.basename(file))
      print 'spinning file', file, f.GetEntries(), 'entries'

      e = f.GetEvent()

      for i in range(f.GetEntries()):
        f.GetEntry(i)
        for j in range(e.GetNumBolos()):
          b = e.GetBolo(j)
   
          if b.GetDetectorName() == detectorName:
            for k in range(b.GetNumPulseRecords()):
              p = b.GetPulseRecord(k)
        
              if p.GetChannelName() == channelName:
                for n in range(p.GetNumPulseAnalysisRecords()):
                  r = p.GetPulseAnalysisRecord(n)
          
                  if r.GetName() == "KTrapKamperProto":
                    if r.IsBaseline()==0:
                      if r.GetAmp() < smin: smin = r.GetAmp()
                      if r.GetAmp() > smax: smax = r.GetAmp()
                      if r.GetPeakPosition() < peakmin: peakmin = r.GetPeakPosition()
                      if r.GetPeakPosition() > peakmax: peakmax = r.GetPeakPosition()
                      trapEventCounter += 1
                  elif r.GetName() == "samba":
                    if r.IsBaseline() == 0:
                      sambaEventCounter += 1
                      if r.GetAmp() < sambamin: sambamin = r.GetAmp()
                      if r.GetAmp() > sambamax: sambamax = r.GetAmp()

  print 'trap range', smin, smax
  print 'samba range', sambamin, sambamax
  print 'peak range', peakmin, peakmax
  
  print 'expected counts - trap / samba', trapEventCounter, sambaEventCounter
  
  if newFile == 'new':
    print 'creating new file: ' + channelName + '.root'
    fout = TFile(channelName + '.root','recreate')
  else:
    print 'updating existing file: ' + channelName + '.root'
    fout = TFile(channelName + '.root','update')
  
  trap = getHist(fout,'trap',smin,smax)
  trapBase = getHist(fout,'trapBase',smin,smax)
  samba = getHist(fout, 'samba', sambamin, sambamax)
  sambaBase = getHist(fout, 'sambaBase', sambamin, sambamax)
  trapPeak = getHist(fout, 'trapPeak', peakmin, peakmax)
  peakDiff = getHist(fout, 'peakDiff', -2500, 2500)

  histlist = []
  histlist.append(trap)
  histlist.append(trapBase)
  histlist.append(samba)
  histlist.append(sambaBase)
  histlist.append(trapPeak)
  histlist.append(peakDiff)

  trapEventCounter = 0
  sambaEventCounter = 0

  for file in allFiles:
    if os.path.basename(file).startswith(runName) and os.path.basename(file).split('_')[0] in calibRuns:
      print 'plotting file', file
      f = KDataReader(dataPath + '/' + os.path.basename(file))
      
      e = f.GetEvent()

      for i in range(f.GetEntries()):
        f.GetEntry(i)
        for j in range(e.GetNumBolos()):
          b = e.GetBolo(j)
   
          if b.GetDetectorName() == detectorName:

            goodHeatTime = False
            for k in range(b.GetNumPulseRecords()):
              p = b.GetPulseRecord(k)
              if p.GetIsHeatPulse():
                for n in range(p.GetNumPulseAnalysisRecords()):
                  r = p.GetPulseAnalysisRecord(n)
                  if r.GetPeakPosition() >= p.GetPretriggerSize() and r.GetPeakPosition() < p.GetPretriggerSize()+10:
                    goodHeatTime = True


            if goodHeatTime:
              for k in range(b.GetNumPulseRecords()):
                p = b.GetPulseRecord(k)

                if p.GetChannelName() == channelName:
                  for n in range(p.GetNumPulseAnalysisRecords()):
                    r = p.GetPulseAnalysisRecord(n)
          
                    if r.GetName() == "KTrapKamperProto":
                      if r.IsBaseline()==0:
                        trapEventCounter += 1
                        trapPeak.Fill(r.GetPeakPosition())
                        peakDiff.Fill(r.GetPeakPosition() - p.GetPretriggerSize())
                        
                        if p.GetIsHeatPulse():
                          if r.GetPeakPosition() > p.GetPretriggerSize() and r.GetPeakPosition() < p.GetPretriggerSize()+10:
                            trap.Fill(r.GetAmp())
                        else:
                          if r.GetPeakPosition() > p.GetPretriggerSize():
                            trap.Fill(r.GetAmp())
                      
                      else:
                        trapBase.Fill(r.GetAmp())
                    
                    elif r.GetName() == "samba":
                      if r.IsBaseline() == 0:
                        sambaEventCounter += 1
                        samba.Fill(r.GetAmp())
                      else:
                        sambaBase.Fill(r.GetAmp())



  fout.cd()
  print 'found events = trap/samba:', trapEventCounter, sambaEventCounter
  print 'hist sizes = trap/samba', trap.GetEntries(), samba.GetEntries()

  print 'baseline hist sizes = trap/samba', trapBase.GetEntries(), sambaBase.GetEntries()

  for hist in histlist:
    hist.Write()


  fout.Close()
