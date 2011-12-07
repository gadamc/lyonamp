#!/usr/bin/env python

from ROOT import *
from couchdbkit import Server, Database
import sys, os, string, math

def getHist(name, amin, amax):  
  h = TH1D(name,name,5000,amin,amax)
  return h

def get2dHist(name, xmin, xmax, ymin, ymax):
  h = TH2D(name,name,5000,xmin,xmax,5000,ymin,ymax)
  return h
  
def main(arg):  
  
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
    
    for i in range(f.GetEntries()):
      f.GetEntry(i)
      
      for j in range(event.GetNumBolos()):
        bolo = e.GetBolo(j)
        detname = bolo.GetDetectorName()
        if detectorInfo.has_key(detname) == False:
          detectorInfo[detname] = {}
          detectorInfo['maxSumIon'] = {}
          detectorInfo['minSumIon'] = {}
        
        sumIon = 0
          
        for k in range(bolo.GetNumPulseRecords()):
          pulse = bolo.GetPulseRecord(k)
          channame = pulse.GetChannelName()
          
          if detectorInfo[detname].has_key(channame) == False:
            detectorInfo[detname][channame] = {}
            detectorInfo[detname][channame]['min'] = 0
            detectorInfo[detname][channame]['max'] = 0
            detectorInfo[detname][channame]['pulselength'] = pulse.GetPulseLength()
          
          chanInfo = detectorInfo[detname][channame]
          result = pulse.GetPulseAnalysisRecord(resultName)
          polarity = polCalc.GetExpectedPolarity(p)
            
          if result.IsBaseline() == 0:    
            if pulse.GetIsHeatPulse() == False:
              sumIon += polarity*result.GetAmp()
                
            if result.GetAmp() > chanInfo['max']: chanInfo['max'] = result.GetAmp()
            if result.GetAmp() < chanInfo['min']: chanInfo['min'] = result.GetAmp()

        if sumIon > detectorInfo['maxSumIon']:  detectorInfo['maxSumIon'] = sumIon
        if sumIon < detectorInfo['minSumIon']:  detectorInfo['minSumIon'] = sumIon

  #for each detector and channel, set up the histograms
  histList = []
  for det in detectorInfo.iterkeys():
    detectorInfo[det]['sumIonHist'] = getHist(det+'_sumIon', detectorInfo['minSumIon'], detectorInfo['maxSumIon'])
    histList.append(detectorInfo[det]['sumIonHist'])
    
    for chan in detectorInfo[det].iterkeys():
      chanInfo = detectorInfo[det][chan]
      chanInfo['hist'] = getHist(string.replace(chan, ' ', '_')+'_hist', chanInfo['min'], chanInfo['max'])
      histList.append(chanInfo['hist'])
      
      chanInfo['corrhists'] = {}
      for otherChan in detectorInfo[det].iterkeys():
        if otherChan != chan:
          chanInfo['corrhists'][otherChan] = get2dHist(string.replace(chan, ' ', '_')+string.replace(otherChan, ' ', '_')+'_hist', 
                                                      chanInfo['min'], chanInfo['max'], detectorInfo[det][otherChan]['min'], 
                                                      detectorInfo[det][otherChan]['max'])
          histList.append(chanInfo['corrhists'][otherChan])
               
  
  #all of the histograms are now set up... loop back through the data and fill them in.
  for afile in fileList:
    
    f = KDataReader(afile)
    print 'spinning file', afile, f.GetEntries(), 'entries'
    
    event = f.GetEvent()
    
    for i in range(f.GetEntries()):
      f.GetEntry(i)
      
      for j in range(event.GetNumBolos()):
        bolo = e.GetBolo(j)
        detname = bolo.GetDetectorName()
        
        
        #first step is to find the ionization pulse with the largest peak
        #this will define the time of the event
        maxAmp = 0
        pulseWithMaxAmp = -1
        for k in range(bolo.GetNumPulseRecords()):
          pulse = bolo.GetPulseRecord(k)
          
          chanInfo = detectorInfo[detname][pulse.GetChannelName()]
          result = pulse.GetPulseAnalysisRecord(resultName)
          polarity = polCalc.GetExpectedPolarity(p)
            
          if result.IsBaseline() == 0 and pulse.GetIsHeatPulse() == False:
            if polarity*result.GetAmp() < maxAmp: 
              maxAmp = math.fabs(result.GetAmp())
              pulseWithMaxAmp = k
        
        pulse = bolo.GetPulseRecord(pulseWithMaxAmp)
        result = pulse.GetPulseAnalysisRecord(resultName)
        
        if result.GetPeakPosition() < pulse.GetPretriggerSize()*0.9:
          continue  #if the pulse position is less than the pretrigger size, let's assume this is noise, continuing to the next bolo record
          
        timeRelativeToTrigger = (result.GetPeakPosition() -  pulse.GetPretriggerSize())*pulse.GetPulseTimeWidth() 
        peakPositionOfMax = result.GetPeakPosition()
        
        
        #loop back through the pulses to gather the results, requiring that the pulse peak position be within a 
        #reasonable range (+- 3 bins in ionization and +- 1 bin in heat.)
        
        #first make sure that we have at least one "good" heat pulse
        goodHeatPulse = False
        for k in range(bolo.GetNumPulseRecords()):
          if result.IsBaseline() == 0 and pulse.GetIsHeatPulse():
            if result.GetPeakPostion() < peakPositionOfMax + 1 and result.GetPeakPosition() > peakPositionOfMax - 1:
              chanInfo['hist'].Fill(polarity*result.GetAmp())
              goodHeatPulse = True
          
        if goodHeatPulse == False:
          continue  # move on to the bolometer event
              
        for k in range(bolo.GetNumPulseRecords()):
          pulse = bolo.GetPulseRecord(k)
          sumIon = 0
          
          chanInfo = detectorInfo[bolo->GetDetectorName()][pulse->GetChannelName()]
          result = pulse.GetPulseAnalysisRecord(resultName)
          polarity = polCalc.GetExpectedPolarity(p)
              
          #just focus on the ionization pulses here.
          if result.IsBaseline() == 0 and pulse.GetIsHeatPulse() == False:
            
            if result.GetPeakPostion() < peakPositionOfMax + 3 and result.GetPeakPosition() > peakPositionOfMax - 3:
              chanInfo['hist'].Fill(polarity*result.GetAmp())  
              sumIon += polarity*result.GetAmp()
              
              #fill the correlation histogram for the other channels
              for kk in range(bolo.GetNumPulseRecords()):
                if kk != k:
                  otherPulse = bolo.GetPulseRecord(kk)
                  otherPol = polCalc.GetExpectedPolarity(p)
                  otherResult = otherPulse.GetPulseAnalyaisRecord(resultName)
                  
                  chanInfo['corrhists'][otherPulse.GetChannelName()].Fill( polarity*result.GetAmp(), otherPol*otherResult.GetAmp())
          
          
              
          
        detectorInfo[det]['sumIonHist'].Fill(sumIon)  
      
if __name__ == '__main__':
  main(*sys.argv[1:])