#!/usr/bin/env python
import sys, shutil, os, stat, glob

def getSambaFilePattern():
  return getSambaDirPattern() + '_[0-9][0-9][0-9]'

def getSambaDirPattern():
  return '[d-n][a-m][0-9][0-9][a-z][0-9][0-9][0-9]'


def buildRunScript(file):
  fname = 'run%s.sh' % os.path.basename(file)
  script = open(fname, 'w')
  script.write('#!/usr/local/bin/tcsh\n')
  kdataDir = '/sps/edelweis/kdata/data/run12/'
  outFile = os.path.join(kdataDir, 'raw/'+os.path.basename(file)+'.root')
  script.write('/sps/edelweis/kdata/data/run12/scratch/scripts/sambaToKdata.py %s %s\n' %(file, outFile))
  script.close()
  return fname

if __name__ == '__main__':
  runListFile = open(sys.argv[1],'r')
  runAll = open('runAll.sh', 'a')
  rawDataPath = '/sps/edelweis/RawData/Run12'
  
  runAll.write('#!/usr/local/bin/tcsh\n')
  runAll.write('set scriptdir="/sps/edelweis/kdata/data/run12/scratch/scripts"\n')
  submitJob = 'qsub -P P_edelweis -o $scriptdir/qsubout -e $scriptdir/qsubout -l sps=1 -l vmem=3G -l fsize=4096M  $scriptdir'
  
  for line in runListFile:
    line = line.strip('\n')
    print line
    runPattern = os.path.join(rawDataPath, '%s/%s_%s' %(line, line, '[0-9][0-9][0-9]') )
    print ' ', runPattern
    runs = glob.glob( runPattern  )
    for run in runs:
      print '   ', run
      scriptName = buildRunScript(run)
      runAll.write('%s/%s\n' %(submitJob, scriptName))
      
  runAll.close()
  
