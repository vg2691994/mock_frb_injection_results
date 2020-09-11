#!/home/observer/miniconda2/bin/python

import numpy as N
import argparse,os, sys
sys.path.append("/home/vgupta/Codes/Fake_FRBs")

from helpers import Observation, Furby, list_UTCs_from, list_UTCs_after, get_latest_UTC
from Furby_reader import Furby_reader as F
from datetime import datetime as DT
from collections import namedtuple
from datetime import timedelta as TD
import subprocess as S



_c_  = namedtuple('conf',
    ['get_zap_fraction_code',
     'final_result_file',
     'number_of_pipelines'
     ])

conf = _c_( '/home/vgupta/Codes/mmplot/zapf/get_zapf.py',
            #'/home/vgupta/Obs/injection_results_with_tstamps.txt',
            '/data/mopsr/VG/master_db_for_ayushi/results/pipeline_recovery.txt',
            4
          )


class Pipeline:
  def __init__(self, name):
    self.name = name
    self.detected = False
  
  def __repr__(self):
    return str(self.name)

  def __str__(self):
    return str(self.name)

def get_zapfs(utc):
  cmd = "{1} -utcs {0}".format(utc, conf.get_zap_fraction_code)
  out = S.Popen(cmd, shell=True, stdout=S.PIPE).communicate()[0]
  return out.strip().split()[-2], out.strip().split()[-1]

def write_compiled(obs, furby):
  ofile = conf.final_result_file
  if os.path.exists(ofile):
    f = open(ofile, 'a')
  else:
    f = open(ofile, 'w')
    header_string = "UTC\tLocalTime\tzapf(wted)\tzapf(bwted)\tFurby\tBeam\t"
    for pipeline in furby.pipelines:
      header_string += "{0}.snr\t{0}.dm\t{0}.width\t{0}.prob\t{0}.tstamp\t".format(pipeline)
    f.write(header_string+"\n")

  beam = furby.pipelines[0].beam
  ut = DT.strptime(obs.utc, "%Y-%m-%d-%H:%M:%S")
  timezone_diff = TD(hours=10)
  local_time = ut + timezone_diff
  ctime = local_time.strftime("%y-%m-%d_%H:%M:%S")
  #zapf_wt, zapf_bwt = get_zapfs(obs.utc)
  zapf_wt, zapf_bwt = 0,0
  data_string = "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t".format(obs.utc, ctime, str(zapf_wt), str(zapf_bwt), furby.name, str(beam))
  for p in furby.pipelines:
    data_string += "{0}\t{1}\t{2}\t{3}\t{4}\t".format(str(p.snr), str(p.dm), str(p.width), str(p.prob), str(p.tstamp))

  f.write(data_string+"\n")
  f.close()

def parse_detection_params(obs, furby, params):
  pipelines = []
  for line in params:
    l = line.strip().split()
    pipeline = Pipeline(l[0])
    pipeline.detected = True if l[1].lower()=="true" else False
    if "None" in line:
      l = [pipeline, pipeline.detected, 0, 0, 0, 0, 0, 0]
    elif "N/A" in line:
      l = [pipeline, pipeline.detected, -1, -1, -1, -1, -1, -1]
    else:
      pipeline.detected = True
      
    pipeline.beam = int(l[2])
    pipeline.tstamp = float(l[3])
    pipeline.snr = float(l[4])
    pipeline.dm = float(l[5])
    pipeline.width = float(l[6])
    pipeline.prob = float(l[7])

    pipelines.append(pipeline)

  furby.pipelines = pipelines
  write_compiled(obs, furby)


def process(obs):
  print "Collecting furby results for {0}".format(obs.utc)
  results_file = os.path.join(obs.results_dir, obs.utc, "furby.results")
  if not os.path.exists(results_file):
    return
  with open(results_file, 'r') as f:
    lines = f.readlines()
    start = N.inf
    current = False
    for i,line in enumerate(lines):
      if line == "" or line.startswith("--"):
        continue
      l = line.strip().split()
      if line.startswith("INJECTED_FURBYS"):
        injected_furbys = int(l[1])
        if injected_furbys < 1:
          return
        detected = 0
        continue
      if line.startswith("#Detection params"):
        reading_detected = True
        continue
      if line.startswith("#Dropped furbies"):
        reading_detected=False
        reading_dropped=True
        continue
      if line.startswith("#furby_") and reading_detected:
        start = i
        detected += 1
        detection_params=[]
        current = True
        furby = Furby(line.strip("#:\n\t").split("_")[1], db = os.path.join(obs.archives_dir, obs.utc, "Furbys"))
        continue
      if i > start and i <= start+conf.number_of_pipelines and reading_detected:
        detection_params.append(line)
        continue
      if (current) and line == "\n" and reading_detected:
        parse_detection_params(obs, furby, detection_params)
        current = False

def main(args):
  UTCs = []
  if args.utcs_from:
    UTCs.extend(list_UTCs_from(args.utcs_from))
  if args.utcs:
    UTCs.extend(args.utcs)
  if args.utc:
    UTCs.extend([args.utc])
  if args.latest_utc:
    UTCs = get_latest_UTC()
  if args.all_utcs:
    UTCs = list_UTCs_from(conf.start_of_injection)

  if len(UTCs) == 0:
    print "No UTCs specified to process\nExiting.."
    sys.exit(0)
 
  for utc in UTCs:
    obs = Observation(utc)
    if obs.is_failed:
      print "{0} is a failed observation".format(obs.utc)
      continue
    process(obs)

if __name__ == '__main__':
  a = argparse.ArgumentParser()
  a.add_argument("-utc", type=str, help="Process only this UTC")
  a.add_argument("-utcs", type=str, nargs='+', help="Process only these UTCs (regex allowed)")
  a.add_argument("-utcs_from", type=str, help="Process UTCs from this UTC onwards until latest")
  #a.add_argument("-utcs_until", type=str, help="Process UTCs from start of injection until this UTC")
  a.add_argument("-latest_utc", action='store_true', help="Process only the latest UTC", default = False)
  a.add_argument("-all_utcs", action='store_true', help="Process all UTCs since the start of injection")

  a.add_argument("-show", action='store_true', help="Print the results to stdout instead of writing them to a file", default = False)
  a.add_argument("-overwrite", action='store_true', help="Overwrite existing results (if any) [Def: False]", default = False)

  args = a.parse_args()

  main(args)
