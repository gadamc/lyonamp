#!/usr/bin/env python
import sys, shutil, os
from ROOT import *

from couchdbkit import Server, Database

def getHist(rootfile, name, amin, amax):
  h = rootfile.Get(name)
  try:
    print h.GetEntries()
  except:
    print 'making new hist:', name
    h = TH1D(name,name,5000,amin,amax)
  
  return h

def get2dHist(rootfile, name, xmin, xmax, ymin, ymax):
  h = rootfile.Get(name)
  try:
    print h.GetEntries()
  except:
    print 'making new hist:', name
    h = TH2D(name,name,200,xmin,xmax,200,ymin,ymax)
  
  return h


if __name__ == '__main__':

  gROOT.SetBatch(True)

  runName = sys.argv[1]

  s = Server('https://edwdbik.fzk.de:6984')
  db = s['datadb']
  vr = db.view('proc/proc3', reduce=False, key = runName, include_docs=True)
  
  smin = smax = 0
  sambamin = sambamax = 0
  peakmin = peakmax = 0

  detectorName = sys.argv[2]

  newFile= 'new'
  if len(sys.argv) > 3:
    newFile = sys.argv[3]

  fileList = []
  for row in vr:
    doc = row['doc']
    fileList.append(doc['proc2']['file'])


  print runName, detectorName, newFile


  trapEventCounter = 0
  sambaEventCounter = 0

  polCalc = KPulsePolarityCalculator()


  for file in fileList:

      
      f = KDataReader(file)
      print 'spinning file', file, f.GetEntries(), 'entries'

      e = f.GetEvent()

      for i in range(f.GetEntries()):
        f.GetEntry(i)
        for j in range(e.GetNumBolos()):
          b = e.GetBolo(j)
          if b.GetDetectorName() == detectorName:

            trapEventCounter += 1
            sambaEventCounter += 1

            for k in range(b.GetNumPulseRecords()):
              p = b.GetPulseRecord(k)
              
              sumIon = 0
              sumSambaIon = 0
              if p.GetIsHeatPulse() == False:
                for n in range(p.GetNumPulseAnalysisRecords()):
                  r = p.GetPulseAnalysisRecord(n)
                  polarity = polCalc.GetExpectedPolarity(p)
                  if r.GetName() == "KTrapKamperProto":
                    if r.IsBaseline() == 0:
                      sumIon += polarity*r.GetAmp()
                      if r.GetPeakPosition() < peakmin: peakmin = r.GetPeakPosition()
                      if r.GetPeakPosition() > peakmax: peakmax = r.GetPeakPosition()

                  elif r.GetName() == "samba":
                    if r.IsBaseline() == 0:
                      sumSambaIon += polarity*r.GetAmp()

              if sumIon > smax: smax = sumIon
              if sumIon < smin: smin = sumIon
              if sumSambaIon > sambamax: sambamax = sumSambaIon
              if sumSambaIon < sambamin: sambamin = sumSambaIon

  print 'trap range', smin, smax
  print 'samba range', sambamin, sambamax
  print 'peak range', peakmin, peakmax
  
  print 'expected counts - trap / samba', trapEventCounter, sambaEventCounter
  
  if newFile == 'new':
    print 'creating new file: ' + detectorName + '_totalion.root'
    fout = TFile(detectorName + '_totalion.root','recreate')
  else:
    print 'updating existing file: ' + detectorName + '_totalion.root'
    fout = TFile(detectorName + '_totalion.root','update')
  
  trap = getHist(fout,'trap',smin,smax)
  trapBase = getHist(fout,'trapBase',smin,smax)
  samba = getHist(fout, 'samba', sambamin, sambamax)
  sambaBase = getHist(fout, 'sambaBase', sambamin, sambamax)
  trapPeak = getHist(fout, 'trapPeak', peakmin, peakmax)
  peakDiff = getHist(fout, 'peakDiff', -2500, 2500)
  sambaTrap = get2dHist(fout, 'sambaTrap', smin, smax, sambamin, sambamax)
  
  histlist = []
  histlist.append(trap)
  histlist.append(trapBase)
  histlist.append(samba)
  histlist.append(sambaBase)
  histlist.append(trapPeak)
  histlist.append(peakDiff)
  histlist.append(sambaTrap)

  trapEventCounter = 0
  sambaEventCounter = 0
  
  for file in fileList:

      print 'plotting file', file
      f = KDataReader(file)
      
      e = f.GetEvent()

      for i in range(f.GetEntries()):
        f.GetEntry(i)
        for j in range(e.GetNumBolos()):
          b = e.GetBolo(j)
   
          if b.GetDetectorName() == detectorName:
            sumIon = 0
            sumSambaIon = 0
            sumIonBase = 0
            sumSambaIonBase = 0
            
            

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
                trapEventCounter += 1
                if p.GetIsHeatPulse()==False:
                  for n in range(p.GetNumPulseAnalysisRecords()):
                    r = p.GetPulseAnalysisRecord(n)
                    polarity = polCalc.GetExpectedPolarity(p)

                    if r.GetName() == "KTrapKamperProto":
                      if r.IsBaseline()==0:
                        
                        trapPeak.Fill(r.GetPeakPosition())
                        peakDiff.Fill(r.GetPeakPosition() - p.GetPretriggerSize())
                        if r.GetPeakPosition() > 4090 and r.GetPeakPosition() < 4100:
                          sumIon += polarity*r.GetAmp()
                      else:
                        sumIonBase += polarity*r.GetAmp()
                    
              trapEventCounter += 1       
              #print trapEventCounter, sumIon, sumIonBase
              trap.Fill(sumIon)
              trapBase.Fill(sumIonBase)
              
                
            for k in range(b.GetNumPulseRecords()):
              p = b.GetPulseRecord(k)
              sambaEventCounter += 1
              if p.GetIsHeatPulse()==False:
                for n in range(p.GetNumPulseAnalysisRecords()):
                  r = p.GetPulseAnalysisRecord(n)
                  polarity = polCalc.GetExpectedPolarity(p)
                  if r.GetName() == "samba":
                    if r.IsBaseline() == 0:
                      
                      sumSambaIon += polarity*r.GetAmp()
                    else:
                      sumSambaIonBase += polarity*r.GetAmp()

            sambaEventCounter += 1          
            samba.Fill(sumSambaIon)
            sambaBase.Fill(sumSambaIonBase)
            if goodHeatTime:
              sambaTrap.Fill(sumIon, sumSambaIon)
  fout.cd()
  print 'found events = trap/samba:', trapEventCounter, sambaEventCounter
  print 'hist sizes = trap/samba', trap.GetEntries(), samba.GetEntries()

  print 'baseline hist sizes = trap/samba', trapBase.GetEntries(), sambaBase.GetEntries()

  for hist in histlist:
    hist.Write()


  fout.Close()
