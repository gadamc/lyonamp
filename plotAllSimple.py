#!/usr/bin/env python

from ROOT import *
from couchdbkit import Server, Database
import sys, os, string, math, json

def getHist(name, amin, amax):  
  #print name,name,500,amin,amax
  h = TH1D(name,name,2000,amin,amax)
  return h

def get2dHist(name, xmin, xmax, ymin, ymax):
  #print name,name,100,xmin,xmax,100,ymin,ymax
  h = TH2D(name,name,200,xmin,xmax,200,ymin,ymax)
  return h


  
def main(*arg):  
  '''
  Usage: ./plotAllChannelsAllDetectors.py startRun endRun PulseAnalysisRecordName outputFile
  example: ./plotAllChannelsAllDetectors.py lk18b022 lk18b024 KTrapKamperProto lk18b.results.root
  '''  
  gROOT.SetBatch(True)
  startRunName = arg[0]
  endRunName = arg[1]
  resultName = arg[2] #such as 'samba' or 'KTrapKamperProto'
  s = Server('https://edwdbik.fzk.de:6984')
  db = s['datadb']
  vr = db.view('proc/proc3', reduce=False, startkey = startRunName, endkey = endRunName, include_docs=True)
  fileList = []
  for row in vr:
    doc = row['doc']
    if doc.has_key('proc2'):
      fileList.append(doc['proc2']['file'])
      
  detectorInfo = {}
  
  polCalc = KPulsePolarityCalculator()
  
  for afile in fileList:
    
    f = KDataReader(afile)
    print 'spinning file', afile, f.GetEntries(), 'entries'
    
    event = f.GetEvent()
    
 
    f.GetEntry(i)
      
    for j in range(event.GetNumBolos()):
      bolo = event.GetBolo(j)
      detname = bolo.GetDetectorName()
      if detectorInfo.has_key(detname) == False:
        detectorInfo[detname] = {}
        detectorInfo[detname]['maxSumIon'] = 2*33000
        detectorInfo[detname]['minSumIon'] = -2*33000
        detectorInfo[detname]['chans'] = {}
        
      sumIon = 0  
      for k in range(bolo.GetNumPulseRecords()):
        pulse = bolo.GetPulseRecord(k)
        channame = pulse.GetChannelName()
        if pulse.GetPulseLength() == 0:
          continue  #skip events with zero pulse lenght
        
        if detectorInfo[detname]['chans'].has_key(channame) == False:
          detectorInfo[detname]['chans'][channame] = {}
          detectorInfo[detname]['chans'][channame]['min'] = 33000
          detectorInfo[detname]['chans'][channame]['max'] = -33000
          detectorInfo[detname]['chans'][channame]['pulselength'] = pulse.GetPulseLength()
          
  
  #for each detector and channel, set up the histograms
  fout = TFile(arg[3],'recreate')
  histList = []
  for det in detectorInfo.iterkeys():
    #print det
    #print json.dumps(detectorInfo[det], indent=1)

    detectorInfo[det]['sumIonHist'] = 'test'
    detectorInfo[det]['sumIonHist'] = getHist(det+'_sumIon', detectorInfo[detname]['minSumIon'], detectorInfo[detname]['maxSumIon'])
    histList.append(detectorInfo[det]['sumIonHist'])
    
    chans = detectorInfo[det]['chans']

    for chan in chans.iterkeys():
      #print chan
      chanInfo = chans[chan]
      
      #print json.dumps(chanInfo, indent=1)
      chanInfo['rawhist'] = getHist(string.replace(chan, ' ', '_')+'_rawhist', chanInfo['min'], chanInfo['max'])
      chanInfo['narrowhist'] = getHist(string.replace(chan, ' ', '_')+'_narrowhist', chanInfo['min'], chanInfo['max'])
      chanInfo['positiveTriggerHist'] = getHist(string.replace(chan, ' ', '_')+'_postrighist', chanInfo['min'], chanInfo['max'])
      chanInfo['peakPos'] = TH1D(string.replace(chan, ' ', '_')+'_peakPos', string.replace(chan, ' ', '_')+'_peakPos', 10000, -50e6, 50e6)  
      #chanInfo['allIonPeakDiff'] = TH1D(string.replace(chan, ' ', '_')+'_allIonPeakDiff', string.replace(chan, ' ', '_')+'_allIonPeakDiff', 10000, -50e6, 50e6)  
      
      histList.append(chanInfo['narrowhist'])
      histList.append(chanInfo['rawhist'])
      histList.append(chanInfo['positiveTriggerHist'])
      histList.append(chanInfo['peakPos'])
      #histList.append(chanInfo['allIonPeakDiff'])
      histList.append(chanInfo['maxIonPeakDiff'])
      
      chanInfo['corrhists'] = {}
      for otherChan in chans.iterkeys():
        if otherChan != chan:
          #print '     ', otherChan
          #print '       min', chans[otherChan]['min']
          #print '       max', chans[otherChan]['max']
          #print '     ', json.dumps(chans[otherChan], indent=1)
          chanInfo['corrhists'][otherChan] = get2dHist(string.replace(chan, ' ', '_')+string.replace(otherChan, ' ', '_')+'_hist', 
                                                      chanInfo['min'], chanInfo['max'], chans[otherChan]['min'], 
                                                      chans[otherChan]['max'])
          histList.append(chanInfo['corrhists'][otherChan])
               

  #for i in range(len(histList)):
    #print histList[i].GetName()
  
  #all of the histograms are now set up... loop back through the data and fill them in.
  for afile in fileList:
    
    f = KDataReader(afile)
    print 'spinning file', afile, f.GetEntries(), 'entries'
    
    event = f.GetEvent()
    
    for i in range(f.GetEntries()):
      f.GetEntry(i)


      for j in range(event.GetNumBolos()):

        bolo = event.GetBolo(j)
        detname = bolo.GetDetectorName()
        
            
        for k in range(bolo.GetNumPulseRecords()):
          pulse = bolo.GetPulseRecord(k)
          
          if pulse.GetPulseLength() == 0:
            #print 'pulse length is zero', pulse.GetChannelName()
            continue 

          chanInfo = detectorInfo[detname]['chans'][pulse.GetChannelName()]
          result = pulse.GetPulseAnalysisRecord(resultName)
          polarity = polCalc.GetExpectedPolarity(pulse)
          #print pulse.GetChannelName()
          chanInfo['peakPos'].Fill( (result.GetPeakPosition()-pulse.GetPretriggerSize())*pulse.GetPulseTimeWidth())
          chanInfo['rawhist'].Fill(polarity*result.GetAmp()) 
          
          if result.GetPeakPosition() > pulse.GetPretriggerSize()*0.95:
            chanInfo['positiveTriggerHist'].Fill(polarity*result.GetAmp())  #sort the if statements this way so that I get the heat pulses too...
                    
          if math.fabs(relPulseTime-ionPulseTime) <  10.0*pulse.GetPulseTimeWidth():
              #print 'good heat pulse found', pulse.GetChannelName()
              #print relPulseTime, ionPulseTime, math.fabs(relPulseTime-ionPulseTime), '<', 500.0*pulse.GetPulseTimeWidth()
          
              chanInfo['narrowhist'].Fill(polarity*result.GetAmp())
          
              #fill the correlation histogram for the other channels
              for kk in range(bolo.GetNumPulseRecords()):
                if kk != k:
                  otherPulse = bolo.GetPulseRecord(kk)
                  otherPol = polCalc.GetExpectedPolarity(pulse)
                  otherResult = otherPulse.GetPulseAnalysisRecord(resultName)
                  
                  chanInfo['corrhists'][otherPulse.GetChannelName()].Fill( polarity*result.GetAmp(), otherPol*otherResult.GetAmp())

        sumIon = 0

        for k in range(bolo.GetNumPulseRecords()):
          pulse = bolo.GetPulseRecord(k)
                    
          chanInfo = detectorInfo[bolo.GetDetectorName()]['chans'][pulse.GetChannelName()]
          result = pulse.GetPulseAnalysisRecord(resultName)
          polarity = polCalc.GetExpectedPolarity(pulse)
          
          #just focus on the ionization pulses here.
          if pulse.GetIsHeatPulse() == False:
            
            if math.fabs( (result.GetPeakPosition() -  pulse.GetPretriggerSize())*pulse.GetPulseTimeWidth()  - ionPulseTime) < 100.0e3: #100e3 ns = 100 us, which is typically 10 bins in ionization
              #print 'good ion pulse found', pulse.GetChannelName()
              #print result.GetPeakPosition(), peakPositionOfIon, math.fabs(result.GetPeakPosition() - peakPositionOfIon), '<', 500.0
              chanInfo['narrowhist'].Fill(result.GetAmp())  
              sumIon += polarity*result.GetAmp()
              
              #fill the correlation histogram for the other channels
              for kk in range(bolo.GetNumPulseRecords()):
                if kk != k:
                  otherPulse = bolo.GetPulseRecord(kk)
                  otherPol = polCalc.GetExpectedPolarity(pulse)
                  otherResult = otherPulse.GetPulseAnalysisRecord(resultName)
                  
                  chanInfo['corrhists'][otherPulse.GetChannelName()].Fill( result.GetAmp(), otherResult.GetAmp())      
                  
        detectorInfo[bolo.GetDetectorName()]['sumIonHist'].Fill(sumIon)  

  fout.cd()
  for i in range(len(histList)):
    if histList[i]!=None:
      if histList[i].GetEntries() == 0:
        continue
      #print 'writing hist', histList[i].GetName(), histList[i].GetEntries()
      histList[i].Write()
  fout.Close()



if __name__ == '__main__':
  main(*sys.argv[1:])
