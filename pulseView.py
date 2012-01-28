#!/usr/bin/env python

from ROOT import KDataReader, KDataWriter, KBaselineRemoval, KPatternRemoval, TH1D, KPulsePolarityCalculator, TF1, TCanvas
import matplotlib.pyplot as plt
import sys, os
import numpy as np

def saveAndExit(f):
  f.Write()
  f.Close()
  sys.exit(0)


def cleanPulse(p):
  b = KBaselineRemoval()
  b.SetInputPulse(p.GetTrace())
  pta = ''
  if b.RunProcess():
    if p.GetIsHeatPulse() == False:
      pat = KPatternRemoval()
      pat.SetInputPulse(b.GetOutputPulse(), b.GetOutputPulseSize())
      pat.SetPatternLength(200)  #just for quick stuff here - assume its 200
      pat.RunProcess()
      pat.SetInputPulse(pat.GetOutputPulse(), pat.GetOutputPulseSize())
      pat.SetPatternLength(400)
      pat.RunProcess()
    
      pta = pat
    else:
      pta = b
    
  if pta != '':  
    out = np.zeros(pta.GetOutputPulseSize())
    for i in range(len(out)):
      out[i] = pta.GetOutputPulse()[i]
    
    return out
  else:
    return None
    
def main(*arg):
  plt.ion()
  
  f = KDataReader(arg[0])
  ff = KDataWriter(arg[1], 'KRawEvent')
  ee = ff.GetEvent()
  e = f.GetEvent()
  saveEvent = False
  
  
  
  polCalc = KPulsePolarityCalculator()
  
  for i in range(f.GetEntries()):
    if saveEvent == True:
      ff.Fill()
      
    f.GetEntry(i)
    ee.__assign__(e)
    ee.Clear()
    saveEvent = False
    
    for j in range(e.GetNumBolos()):
      b = e.GetBolo(j)
      bb = False
      for k in range(b.GetNumPulseRecords()):
        p = b.GetPulseRecord(k)
        print 'Entry', i, 'Bolo', b.GetDetectorName(), 'Chan', p.GetChannelName()
        
        if p.GetPulseLength()==0:
          continue #skip
        
        pulse = np.array(p.GetTrace())
            
        #pulse = cleanPulse(p)
        if pulse==None:
          continue #skip this pulse, something bad happened
          
        plt.cla()
        plt.plot( pulse )
        plt.show()
        go = raw_input('s to save pulse, q to quit, f to fit, Enter to continue')
        if(go == 'q'):
          if saveEvent == True:
            ff.Fill()
          saveAndExit(ff)
        if(go == 'p'):
          outFile = open('%s_Entry_%d_Bolo_%s_Chan_%s.csv' % (os.path.basename(arg[0]).split('.')[0], i, b.GetDetectorName(), p.GetChannelName()), 'w')
          outFile.write('Entry_%d_Bolo_%s_Chan_%s\n' % (i, b.GetDetectorName(), p.GetChannelName()) ) 
          for y in pulse:
            outFile.write(str(y)+'\n')
          outFile.close()
          
        if(go == 'f'): #f for fit
          polarity = polCalc.GetExpectedPolarity(p)
          posPulse = polarity* pulse
          maxPos = np.argmax(posPulse)
          # print maxPos
          #          newMaxPos = raw_input('new max')
          #          if newMaxPos != '':
          #            maxPos = int(newMaxPos)
            
          lastPos = int(raw_input('last pos'))
          subPulse = np.log(posPulse[maxPos:lastPos])
          
          plt.cla()
          plt.plot( subPulse )
          plt.show()
          lowMid, highMid = raw_input('low,high: ').split(' ')
          lowMid = int(lowMid)
          highMid = int(highMid)
          subOne = subPulse[0:lowMid]
          subTwo = subPulse[highMid:lastPos]
          
          
          plt.cla()
          plt.plot( subOne )
          plt.show()
          
          raw_input('see next')
          
          plt.cla()
          plt.plot( subTwo )
          plt.show()

          raw_input('fill hists')
            
          hOne = TH1D('hOne', 'hOne', len(subOne), 0, len(subOne))
          for ii in range(len(subOne)):
            if subOne[ii] > 0: hOne.SetBinContent(ii+1, subOne[ii])
            
          hTwo = TH1D('hTwo', 'hTwo', len(subTwo), 0, len(subTwo))
          for ii in range(len(subTwo)):
            if subTwo[ii] > 0: hTwo.SetBinContent(ii+1, subTwo[ii])
            
          c1 = TCanvas('c1')
          hOne.Draw()
          
          c2 = TCanvas('c2')
          hTwo.Draw()
          
          raw_input('hit enter to fit')
          
          fitOne = TF1('one', '[0]+[1]*x', 0, len(subOne))
          hOne.Fit(fitOne)
          
          
          fitTwo = TF1('two', '[0]+[1]*x', 0, len(subTwo))
          hTwo.Fit(fitTwo)
          
          if fitOne.GetParameter(1)< 0.0: print 'time constant 1:', -1./fitOne.GetParameter(1)
          if fitTwo.GetParameter(1)< 0.0: print 'time constant 2:', -1./fitTwo.GetParameter(1)
          
          c1.cd()
          hOne.Draw()
          c2.cd()
          hTwo.Draw()
          c1.Update()
          c2.Update()
            
          raw_input('continue')
          
        if(go == 's'):
          if bb == False:
            bb = ee.AddBolo()
            ss = ee.AddSamba()
            ss.__assign__(b.GetSambaRecord())
            bb.SetSambaRecord(ss)
            
          pp = ee.AddBoloPulse()
          pp.__assign__(p)
          bb.AddPulseRecord(pp)
          pp.SetBolometerRecord(bb)
          
          saveEvent = True
          
  saveAndExit(ff)
  
if __name__== '__main__':
  main(*sys.argv[1:])