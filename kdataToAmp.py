#!/usr/bin/env python

import os, sys, tempfile, shutil, datetime, copy, socket, json
from couchdbkit import Server, Database

import ROOT
global sv

def fillGrandCanyonParameters(dataFile, kg):
  global sv
  db = sv['pulsetemplates']
  
  #have to scan through the file to find
  #the names of the channels that have data in this file
  #and also need to get a date string for the first event
  #in this file.
  
  ftemp = ROOT.KDataReader(dataFile)
  e = ftemp.GetEvent()
  chanList = []
  filedate = ''
  for i in range(ftemp.GetEntries()):
    ftemp.GetEntry(i)
    if filedate == '':
      if e.GetNumSambas()>0:
        s = e.GetSamba(0)
        dd = datetime.datetime.utcfromtimestamp(s.GetNtpDateSec())
        filedate = str(dd)
    for j in range(e.GetNumBoloPulses()):
      p = e.GetBoloPulse(j)
      if p.GetChannelName() not in chanList:
        chanList.append(p.GetChannelName())
  ftemp.Close()
  
  trapKamper = kg.GetTrapKamperProto()
  for chan in chanList:
    vr = db.view('analytical/bychandate', descending=True, reduce=False,startkey=[chan,filedate], endkey=[chan,''], limit=1, include_docs=True)
    try:
      doc = vr.first()['doc']
      trapKamper.SetTrapAmplitudeDecayConstant(doc['channel'],doc['kampsites']['KGrandCanyonKAmpSite']['trapAmpDecayConstant'])
    except: #this will throw if vr.first() doesn't return a document. just ignore it and move on to the next channel
      pass
  
def runProcess(*args, **kwargs):
  
  if len(args) > 1:
    print 'takes just one argument... the database document.'
    #sys.exit(-1)

  
  newFileName = args[0]['proc1']['file'].strip('.root') + '.amp.root'
  print 'running process to create file', newFileName
  theRet = ''
  exc = {}
  try:
    ROOT.gSystem.Load('libkds')
    ROOT.gSystem.Load('libkpta')
    ROOT.gSystem.Load('libkamping')
  

    k = ROOT.KAmpKounselor()
    print 'creating kampsites'
    kg = ROOT.KGrandCanyonKAmpSite()
    print 'filling kampsite paramters'
    fillGrandCanyonParameters(args[0]['proc1']['file'], kg)
    print 'running analysis'
    k.AddKAmpSite(kg)
    theRet = k.RunKamp(args[0]['proc1']['file'], newFileName)
  
  except Exception as theExcep:
    theRet = ''
    print theExcep
    exc['print'] = str(theExcep)
    exc['type'] = str(type(theExcep))
    #exc['args'] = theExcep.args
    #exc['message'] = theExcep.message
    
  processdoc = {}
  
  if theRet != '':
    processdoc['file'] = newFileName
  elif exc.has_key('print'):
    processdoc['exception'] = copy.deepcopy(exc)
      
 
  return (processdoc, theRet)
  
def main(*argv):
  '''
  argv[0] is the couchdb server (http://127.0.0.1:5984)
  argv[1] is the database (datadb)
  argv[2] is the document id

  process 2 - runs the KAmpKounselor with the KGrandCanyonKampSite
  This script is meant to be run on ccage.in2p3.fr with access to the /sps/edelweis directory
  because that is where we expect the data files to be located.
  '''
  
  #create a DBProcess instance, which will assist in uploading the proc
  #document to the database... although, its barely useful... 
  global sv
  sv = Server(argv[0])
  db = sv[argv[1]]
  doc = db[argv[2]]

  (resultdoc, status) = runProcess(doc)
  print 'status', status
  print json.dumps(resultdoc, indent=1)


if __name__ == '__main__':
  ROOT.gROOT.SetBatch(True)
  main(*sys.argv[1:])
