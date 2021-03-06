#!/usr/bin/env python
import sys, shutil, os, stat

def buildRunScript(file):
  fname = 'run%s.sh' % os.path.basename(file)
  script = open(fname, 'w')
  script.write('#!/usr/local/bin/tcsh\n')
  dataDir = '/sps/edelweis/kdata/data/run12/raw'
  script.write('set data="%s"\n' % dataDir)
  dataFile = os.path.join(dataDir, 'raw/'+os.path.basename(file))
  outFile = os.path.join(dataDir, 'scratch/'+os.path.basename(file).split('.')[0]+'_amp.root')
  script.write('/sps/edelweis/kdata/data/run12/scratch/scripts/kdataToAmp.py %s %s\n' %(dataFile, outFile))
  script.close()
  return fname

if __name__ == '__main__':
  runName = sys.argv[1]
  runAll = open('runAll.sh', 'w')
  rawDataPath = '/sps/edelweis/kdata/data/run12/raw'
  allFiles = os.listdir(rawDataPath)
  runAll.write('#!/usr/local/bin/tcsh\n')
  runAll.write('set scriptdir="/sps/edelweis/kdata/data/run12/scratch/scripts"\n')
  submitJob = 'qsub -P P_edelweis -o $scriptdir/qsubout -e $scriptdir/qsubout -l sps=1 -l vmem=2G -l fsize=4096M  $scriptdir'
  for file in allFiles:
    if os.path.basename(file).startswith(runName):
      scriptName = buildRunScript(file)
      runAll.write('%s/%s\n' %(submitJob, scriptName))
      
  runAll.close()
