#!/usr/bin/env python

from couchdbkit import Server, Database
import sys, os, subprocess

def main(arg):  

  startRunName = arg[0]  #like lg23b001
  endRunName = arg[1]  #something that is alpha-numerically greater than startRunName, like lg24a005

  s = Server('https://edwdbik.fzk.de:6984')
  db = s['datadb']
  vr = db.view('proc/daqdoc', reduce=False, startkey = startRunName, endkey = endRunName, include_docs=True)

  for row in vr:
    doc = row['doc']
    if doc.has_key('proc0'):
      #if you wanted, you could search this doc for particular criteria, 
      #such as the 'Condition' if you want to only process "calibration gamma" 
      #data
      # if doc['Condition'] == 'calibration gamma'

      #you'll need these for your script below
      myArg = 'method=A'
      myOutput = '/sps/edelweis/myOutput/results'
      scriptDir = '/sps/edelweis/myOutput/batchJobOutput'
      myCommand = '$HOME/bin/myProgram %s %s %s', % (doc['proc0']['file'], myArg, myOutput) 

      #this is how you submit at job on ccage.in2p3.fr. 
      # the -b y tells qsub to treat your script as a binary program. otherwise, it will think that it is a script
      #of course, if 'myProgram' is a script, then don't use -b y

      batchJob = 'qsub -P P_edelweis -b y -o %s -e %s -l sps=1 -l vmem=3G -l fsize=4096M  %s' % (scriptDir, scriptDir, myCommand)

      #now pipe the batchJob command to a unix shell
      proc = subprocess.Popen(myCommand, shell=True, stdout=subprocess.PIPE)
      val = proc.communicate()[0]
      if val != '':
        print val
        
if __name__ == '__main__':
  main(*sys.argv[1:])

