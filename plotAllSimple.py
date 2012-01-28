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
  Usage: ./plotAllSimple.py startRun endRun PulseAnalysisRecordName outputFile
  example: ./plotAllSimple.py lk18b022 lk18b024 KTrapKamperProto lk18b.results.root
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
    print 'opening file', afile, f.GetEntries(), 'entries'
    
    event = f.GetEvent()
    #assuming that we're running with raw, unmerged data files and that for each 
    #event, there is a record for each channel found in the whole file.
    #this won't work if we use merged data
      
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
          
  
  #for each detector and channel, set up the histograms
  fout = TFile(arg[3],'recreate')
  histList = []
  for det in detectorInfo.iterkeys():
    #print det
    #print json.dumps(detectorInfo[det], indent=1)

    detectorInfo[det]['sumIonHist'] = getHist(det+'_sumIon', detectorInfo[detname]['minSumIon'], detectorInfo[detname]['maxSumIon'])
    histList.append(detectorInfo[det]['sumIonHist'])
    detectorInfo[det]['narrowSumIonHist'] = getHist(det+'_narrowSumIon', detectorInfo[detname]['minSumIon'], detectorInfo[detname]['maxSumIon'])
    histList.append(detectorInfo[det]['narrowSumIonHist'])
    
    chans = detectorInfo[det]['chans']

    for chan in chans.iterkeys():
      #print chan
      chanInfo = chans[chan]
      
      #print json.dumps(chanInfo, indent=1)
      chanInfo['rawhist'] = getHist(string.replace(chan, ' ', '_')+'_rawhist', chanInfo['min'], chanInfo['max'])
      chanInfo['narrowhist'] = getHist(string.replace(chan, ' ', '_')+'_narrowhist', chanInfo['min'], chanInfo['max'])
      chanInfo['positiveTriggerHist'] = getHist(string.replace(chan, ' ', '_')+'_postrighist', chanInfo['min'], chanInfo['max'])
      chanInfo['peakPos'] = TH1D(string.replace(chan, ' ', '_')+'_peakPos', string.replace(chan, ' ', '_')+'_peakPos', 10000, -50e6, 50e6)  
      
      histList.append(chanInfo['narrowhist'])
      histList.append(chanInfo['rawhist'])
      histList.append(chanInfo['positiveTriggerHist'])
      histList.append(chanInfo['peakPos'])
 
      
      chanInfo['rawcorrhists'] = {}
      chanInfo['narrowcorrhists'] = {}
      
      for otherChan in chans.iterkeys():
        if otherChan != chan:
          chanInfo['rawcorrhists'][otherChan] = get2dHist(string.replace(chan, ' ', '_')+string.replace(otherChan, ' ', '_')+'_rawcorr', 
                                                      chanInfo['min'], chanInfo['max'], chans[otherChan]['min'], 
                                                      chans[otherChan]['max'])
          histList.append(chanInfo['rawcorrhists'][otherChan])
          chanInfo['narrowcorrhists'][otherChan] = get2dHist(string.replace(chan, ' ', '_')+string.replace(otherChan, ' ', '_')+'_narrowcorr', 
                                                      chanInfo['min'], chanInfo['max'], chans[otherChan]['min'], 
                                                      chans[otherChan]['max'])
          histList.append(chanInfo['narrowcorrhists'][otherChan])
               

  
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
        
        sumIon = 0
        narrowSumIon = 0
        
        for k in range(bolo.GetNumPulseRecords()):
          pulse = bolo.GetPulseRecord(k)
          
          if pulse.GetPulseLength() == 0:
            #print 'pulse length is zero', pulse.GetChannelName()
            continue 

          chanInfo = detectorInfo[detname]['chans'][pulse.GetChannelName()]
          result = pulse.GetPulseAnalysisRecord(resultName)
          polarity = polCalc.GetExpectedPolarity(pulse)
          #print pulse.GetChannelName()
          chanInfo['peakPos'].Fill( result.GetPeakPosition() )
          chanInfo['rawhist'].Fill( polarity*result.GetAmp()/(result.GetExtra(0)*result.GetExtra(1)) ) 
          sumIon += polarity*result.GetAmp()/(result.GetExtra(0)*result.GetExtra(1)) 
          
          if result.GetPeakPosition() > pulse.GetPretriggerSize()*0.99:
            chanInfo['positiveTriggerHist'].Fill(polarity*result.GetAmp()/(result.GetExtra(0)*result.GetExtra(1)))  #sort the if statements this way so that I get the heat pulses too...
                 
          min = 4090
          max = 4110
          if pulse.GetIsHeatPulse():
            min = 250
            max = 265
            
          if result.GetPeakPosition() > min and result.GetPeakPosition() < max:
            chanInfo['narrowhist'].Fill(polarity*result.GetAmp()/(result.GetExtra(0)*result.GetExtra(1)))
            narrowSumIon += polarity*result.GetAmp()/(result.GetExtra(0)*result.GetExtra(1)) 
            
            #fill the correlation histogram for the other if we have a narrow peak time, but only if the 
            #peak time is also narrow on the other detectors
            for kk in range(bolo.GetNumPulseRecords()):
              if kk != k:
                otherPulse = bolo.GetPulseRecord(kk)
                otherPol = polCalc.GetExpectedPolarity(pulse)
                otherResult = otherPulse.GetPulseAnalysisRecord(resultName)
                
                min = 4090
                max = 4110
                if otherPulse.GetIsHeatPulse():
                  min = 250
                  max = 265
                if otherResult.GetPeakPosition() > min and otherResult.GetPeakPosition() < max:  
                  chanInfo['narrowcorrhists'][otherPulse.GetChannelName()].Fill( polarity*result.GetAmp()/(result.GetExtra(0)*result.GetExtra(1)), otherPol*otherResult.GetAmp()/(otherResult.GetExtra(0)*otherResult.GetExtra(1)))

          #fill the correlation histogram for the other channels
          for kk in range(bolo.GetNumPulseRecords()):
            if kk != k:
              otherPulse = bolo.GetPulseRecord(kk)
              otherPol = polCalc.GetExpectedPolarity(pulse)
              otherResult = otherPulse.GetPulseAnalysisRecord(resultName)
              try:
                chanInfo['rawcorrhists'][otherPulse.GetChannelName()].Fill( polarity*result.GetAmp()/(result.GetExtra(0)*result.GetExtra(1)), otherPol*otherResult.GetAmp()/(otherResult.GetExtra(0)*otherResult.GetExtra(1)))
              except Exception as e:
                print str(type(e)) + ": " + str(e)
                print 'pulse lengths: ', pulse.GetPulseLength(), otherPulse.GetPulseLength()
  
        detectorInfo[bolo.GetDetectorName()]['sumIonHist'].Fill(sumIon)
        detectorInfo[bolo.GetDetectorName()]['narrowSumIonHist'].Fill(narrowSumIon)
          

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
