#!/usr/bin/env python

from ROOT import *
from couchdbkit import Server, Database
import sys, os, string, math, json

def getHist(name, amin, amax):  
  print name,name,500,amin,amax
  h = TH1D(name,name,500,amin,amax)
  return h

def get2dHist(name, xmin, xmax, ymin, ymax):
  print name,name,100,xmin,xmax,100,ymin,ymax
  h = TH2D(name,name,100,xmin,xmax,100,ymin,ymax)
  return h


  
def main(*arg):  
  
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
        bolo = event.GetBolo(j)
        detname = bolo.GetDetectorName()
        if detectorInfo.has_key(detname) == False:
          detectorInfo[detname] = {}
          detectorInfo[detname]['maxSumIon'] = 0
          detectorInfo[detname]['minSumIon'] = 0
          detectorInfo[detname]['chans'] = {}
        
        sumIon = 0  
        for k in range(bolo.GetNumPulseRecords()):
          pulse = bolo.GetPulseRecord(k)
          channame = pulse.GetChannelName()
          if pulse.GetPulseLength() == 0:
            continue  #skip events with zero pulse lenght

          if detectorInfo[detname]['chans'].has_key(channame) == False:
            detectorInfo[detname]['chans'][channame] = {}
            detectorInfo[detname]['chans'][channame]['min'] = 0
            detectorInfo[detname]['chans'][channame]['max'] = 0
            detectorInfo[detname]['chans'][channame]['pulselength'] = pulse.GetPulseLength()
          
          chanInfo = detectorInfo[detname]['chans'][channame]
          result = pulse.GetPulseAnalysisRecord(resultName)
          polarity = polCalc.GetExpectedPolarity(pulse)
            
          if result.IsBaseline() == 0:    
            if pulse.GetIsHeatPulse() == False:
              sumIon += polarity*result.GetAmp()
                
            if result.GetAmp() > chanInfo['max']: chanInfo['max'] = result.GetAmp()
            if result.GetAmp() < chanInfo['min']: chanInfo['min'] = result.GetAmp()

        if sumIon > detectorInfo[detname]['maxSumIon']:  detectorInfo[detname]['maxSumIon'] = sumIon
        if sumIon < detectorInfo[detname]['minSumIon']:  detectorInfo[detname]['minSumIon'] = sumIon

  #for each detector and channel, set up the histograms
  fout = TFile(arg[3],'recreate')
  histList = []
  for det in detectorInfo.iterkeys():
    print det
    print json.dumps(detectorInfo[det], indent=1)

    detectorInfo[det]['sumIonHist'] = 'test'
    detectorInfo[det]['sumIonHist'] = getHist(det+'_sumIon', detectorInfo[detname]['minSumIon'], detectorInfo[detname]['maxSumIon'])
    histList.append(detectorInfo[det]['sumIonHist'])
    
    chans = detectorInfo[det]['chans']

    for chan in chans.iterkeys():
      print chan
      chanInfo = chans[chan]
      #print json.dumps(chanInfo, indent=1)
      chanInfo['hist'] = getHist(string.replace(chan, ' ', '_')+'_hist', chanInfo['min'], chanInfo['max'])
      chanInfo['peakPos'] = TH1D(string.replace(chan, ' ', '_')+'_peakPos', det+'_peakPos', 10000, -500e6, 500e6)  
      #chanInfo['allIonPeakDiff'] = TH1D(string.replace(chan, ' ', '_')+'_allIonPeakDiff', string.replace(chan, ' ', '_')+'_allIonPeakDiff', 10000, -50e6, 50e6)  
      chanInfo['maxIonPeakDiff'] = TH1D(string.replace(chan, ' ', '_')+'_maxIonPeakDiff', det+'_maxIonPeakDiff', 10000, -500e6, 500e6)  

      histList.append(chanInfo['hist'])
      histList.append(chanInfo['peakPos'])
      #histList.append(chanInfo['allIonPeakDiff'])
      histList.append(chanInfo['maxIonPeakDiff'])
      
      chanInfo['corrhists'] = {}
      for otherChan in chans.iterkeys():
        if otherChan != chan:
          print '     ', otherChan
          print '       min', chans[otherChan]['min']
          print '       max', chans[otherChan]['max']
          #print '     ', json.dumps(chans[otherChan], indent=1)
          chanInfo['corrhists'][otherChan] = get2dHist(string.replace(chan, ' ', '_')+string.replace(otherChan, ' ', '_')+'_hist', 
                                                      chanInfo['min'], chanInfo['max'], chans[otherChan]['min'], 
                                                      chans[otherChan]['max'])
          histList.append(chanInfo['corrhists'][otherChan])
               

  for i in range(len(histList)):
    print histList[i].GetName()
  
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
        
        
        #first step is to find the ionization pulse with the largest peak
        #this will define the time of the event
        maxAmp = 0
        pulseWithMaxAmp = -1
        for k in range(bolo.GetNumPulseRecords()):
          pulse = bolo.GetPulseRecord(k)
          
          if pulse.GetPulseLength() == 0:
            #print 'pulse length is zero'
            continue 

          chanInfo = detectorInfo[detname]['chans'][pulse.GetChannelName()]
          result = pulse.GetPulseAnalysisRecord(resultName)
          polarity = polCalc.GetExpectedPolarity(pulse)
       
          chanInfo['peakPos'].Fill( (result.GetPeakPosition()-pulse.GetPretriggerSize())*pulse.GetPulseTimeWidth())

          if pulse.GetIsHeatPulse() == False:
            if polarity*result.GetAmp() < maxAmp: 
              maxAmp = polarity*result.GetAmp()
              pulseWithMaxAmp = k

        if pulseWithMaxAmp == -1:
          #print 'no maximum amp found'
          break  #we didn't find a pulse with a maximum amplitude. was it because of events with zero pulse length?

        pulse = bolo.GetPulseRecord(pulseWithMaxAmp)
        result = pulse.GetPulseAnalysisRecord(resultName)
        
        if result.GetPeakPosition() < pulse.GetPretriggerSize()*0.9:
          continue  #if the pulse position is less than the pretrigger size, let's assume this is noise, continuing to the next bolo record
        
      
        ionPulseTime = (result.GetPeakPosition() -  pulse.GetPretriggerSize())*pulse.GetPulseTimeWidth() 
        peakPositionOfIon = result.GetPeakPosition()
        
        #print 'max peak position' peakPositionOfIon, ionPulseTime
        
        #loop back through the pulses to gather the results, requiring that the pulse peak position be within a 
        #reasonable range (+- 500 bins in ionization and +- 100 bin in heat.)
        
        #first make sure that we have at least one "good" heat pulse
        goodHeatPulse = False
        for k in range(bolo.GetNumPulseRecords()):
          pulse = bolo.GetPulseRecord(k)
          result = pulse.GetPulseAnalysisRecord(resultName)
          polarity = polCalc.GetExpectedPolarity(pulse)
          relPulseTime = (result.GetPeakPosition() -  pulse.GetPretriggerSize())*pulse.GetPulseTimeWidth()
          chanInfo = detectorInfo[bolo.GetDetectorName()]['chans'][pulse.GetChannelName()]
          chanInfo['maxIonPeakDiff'].Fill(relPulseTime)

          if pulse.GetIsHeatPulse():
            
            if math.fabs(relPulseTime-ionPulseTime) <  500.0*pulse.GetPulseTimeWidth():
              #print 'good heat pulse found', pulse.GetChannelName()
              #print relPulseTime, ionPulseTime, math.fabs(relPulseTime-ionPulseTime), '<', 500.0*pulse.GetPulseTimeWidth()
          
              chanInfo['hist'].Fill(result.GetAmp())
              goodHeatPulse = True


              #fill the correlation histogram for the other channels
              for kk in range(bolo.GetNumPulseRecords()):
                if kk != k:
                  otherPulse = bolo.GetPulseRecord(kk)
                  otherPol = polCalc.GetExpectedPolarity(pulse)
                  otherResult = otherPulse.GetPulseAnalysisRecord(resultName)
                  
                  chanInfo['corrhists'][otherPulse.GetChannelName()].Fill( result.GetAmp(), otherPol*otherResult.GetAmp())

          
        if goodHeatPulse == False:
          continue  # move on to the next bolometer event
              
        sumIon = 0

        for k in range(bolo.GetNumPulseRecords()):
          pulse = bolo.GetPulseRecord(k)
                    
          chanInfo = detectorInfo[bolo.GetDetectorName()]['chans'][pulse.GetChannelName()]
          result = pulse.GetPulseAnalysisRecord(resultName)
          polarity = polCalc.GetExpectedPolarity(pulse)
              
          #just focus on the ionization pulses here.
          if pulse.GetIsHeatPulse() == False:
            
            if math.fabs(result.GetPeakPosition() - peakPositionOfIon) < 500.0:
              #print 'good ion pulse found', pulse.GetChannelName()
              #print result.GetPeakPosition(), peakPositionOfIon, math.fabs(result.GetPeakPosition() - peakPositionOfIon), '<', 500.0
              chanInfo['hist'].Fill(result.GetAmp())  
              sumIon += polarity*result.GetAmp()
              
              #fill the correlation histogram for the other channels
              for kk in range(bolo.GetNumPulseRecords()):
                if kk != k:
                  otherPulse = bolo.GetPulseRecord(kk)
                  otherPol = polCalc.GetExpectedPolarity(pulse)
                  otherResult = otherPulse.GetPulseAnalysisRecord(resultName)
                  
                  chanInfo['corrhists'][otherPulse.GetChannelName()].Fill( result.GetAmp(), otherResult.GetAmp())      
                  
        detectorInfo[det]['sumIonHist'].Fill(sumIon)  

  fout.cd()
  for i in range(len(histList)):
    if histList[i]!=None:
      print 'writing hist', histList[i].GetName(), histList[i].GetEntries()
      histList[i].Write()
  fout.Close()

if __name__ == '__main__':
  main(*sys.argv[1:])
