#!/home/observer/miniconda2/bin/python

import numpy as N
import sys, os, time, argparse
import logging as L
from logging.handlers import RotatingFileHandler
from collections import namedtuple
from Furby_reader import Furby_reader

sys.path.append("/home/vgupta/Codes/Fake_FRBs")
from helpers import Observation, Furby, list_UTCs_from, list_UTCs_after, get_latest_UTC


_c_ = namedtuple('conf', 
    ['start_of_injection',
     'log_file',
     'log_name',
     'furby_log_file',
     'sleep_time',
     ])

conf = _c_( '2018-07-03-05:29:21',
            '/data/mopsr/logs/amandlik_furby_results_manager.log',
            'furby_results_manager',
            'furbys.log',
            15,
          )

def setup_logger(name, log_level):
  allowed_levels = N.array(['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'])
  if log_level.upper() not in allowed_levels:
    raise ValueError("log_level {0} not recognised".format(log_level))

  level = 10 * (int(N.where(allowed_levels == log_level.upper())[0]) + 1)
  print "Logging level:", level
  log = L.getLogger(name)
  
  log.setLevel(level)

  log_handler = RotatingFileHandler(conf.log_file, maxBytes = 5.0E7, backupCount = 20)
  log_handler.setLevel(level)

  log_formatter = L.Formatter('%(asctime)s - %(funcName)s: %(levelname)s: %(message)s', datefmt='%Y-%m-%d-%H:%M:%S')

  log_handler.setFormatter(log_formatter)
  log.addHandler(log_handler)
  return log

def find(furby, all_cands):
  log = L.getLogger(conf.log_name)
  log.debug("Furby = {2}: Furby.s_time= {0}. furby.i_tstamp= {1}, Furby.i_beam = {3}".format(furby.s_time, furby.i_tstamp, furby.ID, furby.i_beam))
  step1 = all_cands[ N.where( (all_cands['beam'] == furby.i_beam ) ) ]
  log.debug("Cands after filtering on beam: \n{0}\n".format(step1))

  if step1.size == 0:
    log.info("No candidate found in the injection beam: {0}".format(furby.i_beam))
    log.debug("The beams for the candidates were: \n{0}".format(all_cands['beam']))
    cand = None
  elif step1.size == 1:
    if N.abs(step1['tstamp'] - furby.s_time)  < furby.length/2:
      cand = step1
      log.info("Found my candidate here: {0}".format(cand))
    else:
      cand = None
      log.info("No candidate made it through the time fitler")
  else:
    tdiff = N.abs(step1['tstamp'] - furby.s_time)
    log.debug("TDIFF = {0}\n".format(tdiff))
    step2 = step1[ N.where( (tdiff < furby.length/2) )]
    log.info("Cands after filtering on BEAM and Time: \n{0}\n".format(step2))
    cand = step2
    if cand.size > 1:
      dm_diff = N.abs(cand['dm'] - furby.header.DM)
      cand = cand[ N.array([N.argsort(dm_diff)[0]]) ]   #cand[0] gives tuple; cand[ N.array([0])] gives structured array
      log.debug("Final candidate: {0}".format(cand))
    elif len(cand) == 0:
      cand = None

  return None if cand is None else cand.copy()


def find_heimdall_cand(obs):
  log = L.getLogger(conf.log_name)
  log.info("Finding Heimdall detections")
  all_cands_file = os.path.join(obs.results_dir, obs.utc, "FB", "all_candidates.dat")
  all_cands = N.loadtxt(all_cands_file, usecols = (0, 2, 3, 5, 12), dtype={ 'names': ('snr', 'tstamp', 'filter_wdth', 'dm', 'beam'), 'formats': (float, float, int, float, int) } )
  if all_cands.size == 0:
    for i, furby in enumerate(obs.furbies):
      obs.furbies[i].heimdall_result = None
      log.debug("For {0} found {1}".format(furby.name, obs.furbies[i].heimdall_result))
  elif all_cands.size == 1:
    for i, furby in enumerate(obs.furbies):
      if all_cands['beam'] == furby.i_beam and N.abs(all_cands['tstamp'] - furby.i_tstamp) < furby.length/2 :
        obs.furbies[i].heimdall_result = all_cands.copy()
      else:
        obs.furbies[i].heimdall_result = None
      log.debug("For {0} found {1}".format(furby.name, obs.furbies[i].heimdall_result))
  else:
    for i, furby in enumerate(obs.furbies):
      log.info("Looking for heimdall detections for {0}".format(furby.name))
      obs.furbies[i].heimdall_result = find(furby, all_cands)
      log.info("For {0} Heimdall found {1}".format(furby.name, obs.furbies[i].heimdall_result))

def find_online_cand(obs):
  log = L.getLogger(conf.log_name)
  log.info("Finding online pipeline detections...")
  furby_log_file = os.path.join(obs.archives_dir, obs.utc, "Furbys", conf.furby_log_file)

  if os.path.exists(furby_log_file):
    log.debug("Checking {0}".format(furby_log_file))
    flogf = N.loadtxt(furby_log_file, usecols = (0, 1, 2 ,3, 4, 5, 6), dtype={ 'names': ('id', 'tstamp', 'beam', 'filter_wdth', 'dm', 'snr', 'prob'), 'formats': ('S10', float, int, int, float, float, float) } )
    flogf['tstamp'] *= obs.header.TSAMP * 1e-6

    for i,furby in enumerate(obs.furbies):
      if flogf.size ==0:
        result = None
      elif flogf.size == 1:
        if flogf['id'] == furby.ID and flogf['beam'] == furby.i_beam and N.abs(flogf['tstamp']-furby.s_time) < furby.length:
          result = flogf.copy()
        else:
          result = None
      else:
        result = flogf[N.where( (flogf['id'] == furby.ID) & (flogf['beam'] == furby.i_beam) )]
        if len(result)==0:
          result = None
        elif len(result)> 1:
          tdiff = N.abs(result['tstamp'] - furby.s_time)
          result = result[ N.where(tdiff < furby.length) ]
          if len(result) == 0:
            result = None
          if len(result) > 1:
            log.critical("HEAVENS HAVE CONSPIRED AGAINST US")
            #raise ValueError("Found {1} candidates within 1 second of an injected furby with same ID and beam: {0}\nNot possible".format(result, len(result) ) )
            log.critical("Found {1} candidates within 1 second of an injected furby with same ID and beam: {0}\nHeimdall must have messed up. Putting the first detection in the results".format(result, len(result) ) )
            print "Found {1} candidates within 1 second of an injected furby with same ID and beam: {0}\nHeimdall must have messed up. Putting the first detection in the results and continuing".format(result, len(result) )
            result = result[0]

      obs.furbies[i].online_result = None if result is None else result.copy()
      log.info("For {0} Online pipeline found {1}".format(furby.name, obs.furbies[i].online_result))

  else:
    log.error("Could not find the furby log file ({0})".format(furby_log_file))
    for i, furby in enumerate(obs.furbies):
      obs.furbies[i].online_result = None

def find_offline_cand(obs):
  log = L.getLogger(conf.log_name)
  log.info("Finding offline pipeline detections")
  log.debug("Checking {0}".format(obs.conf.offline_output_file))
  ofile = os.path.join(obs.archives_dir, obs.utc, obs.conf.offline_output_dir, obs.conf.offline_output_file)
  out = N.loadtxt(ofile, usecols=(3, 5, 6, 7, 8, 9), dtype={ 'names': ('dm', 'width', 'snr', 'name', 'beam', 'tstamp'), 'formats': (float, float, float, 'S100', int, float) })

  log.debug("All offline pipeline cands :\n{0}\n".format(out))
  if out.size == 0:
    for i, furby in enumerate(obs.furbies):
      obs.furbies[i].offline_result = None
      log.info("For {0} found {1}".format(furby.name, obs.furbies[i].offline_result))
  elif out.size == 1:
    out['beam'] = int(out.item(0)[3].split("B")[1].split("_")[0])
    out['tstamp'] = float(out.item(0)[3].split("_")[1].strip(".ar"))
    for i, furby in enumerate(obs.furbies):
      if out['beam'] == furby.i_beam and N.abs(out['tstamp'] - furby.s_time) < furby.length/2 :
        obs.furbies[i].offline_result = out.copy()
      else:
        obs.furbies[i].offline_result = None
      log.info("For {0} found {1}".format(furby.name, obs.furbies[i].offline_result))
  else:
    out['beam'] = [int(name.split("B")[1].split("_")[0]) for name in out['name']]
    out['tstamp'] = [float(name.split("_")[1].strip(".ar")) for name in out['name']]
    for i, furby in enumerate(obs.furbies):
      log.debug("Going into offline_find() now for {0}".format(furby))
      obs.furbies[i].offline_result = find(furby, out)
      log.info("For {0} Offline pipeline found {1}".format(furby.name, obs.furbies[i].offline_result))
        

def process(observation):
  log = L.getLogger(conf.log_name)
  log.info("Processing UTC: {0}".format(observation.utc))
  while(observation.if_processing()):
    log.warn("This UTC ({0}) is still being processed by the backend. Waiting for {1} seconds".format(observation.utc, conf.sleep_time))
    time.sleep(conf.sleep_time)
    
  log.debug("Reading furby params")
  observation.read_furby_params()

  if observation.inj_furbys == -1:
    if observation.is_failed:
      reason = "failed"
    elif observation.info.MB_ENABLED:
      reason = "Module Beam"
    elif observation.info.CORR_ENABLED:
      reason = "Correlation"
    log.info("Furby injection not possible in this observation because this was a {0} observation".format(reason))
    return

  if observation.inj_furbys == 0:
    log.info("0 furbys injected in this observation")
    return

  if observation.inj_furbys > 0:
    log.info("{0} furby(s) might have been injected in this observation".format(observation.inj_furbys))
    observation.split_and_filter_furby_params()
    log.info("Succesfully injected furbies: {0}, dropped furbies: {1}".format(len(observation.furbies), len(observation.dropped_furbies)))

    find_heimdall_cand(observation)

    find_online_cand(observation)

    if observation.processed_offline:
      find_offline_cand(observation)
    else:
      log.info("This observation has not been processed by the offline pipeline yet")

def write_results(obs, outf, overwrite):
  log = L.getLogger(conf.log_name)
  log.info("Writing out the results into {0}".format(outf))

  if outf == sys.stdout or outf == sys.stderr:
    dump_to(obs, outf)
  else:
    if os.path.exists(outf):
      log.warn("{0} already exists".format(outf))
      if overwrite == True:
        log.info("Over-writing {0}".format(outf))
      else:
        log.error("Cannot over-write {0}".format(outf))
        raise IOError("Please scpecify the -overwrite flag if you want to overwite the results")

    with open(outf, 'w') as o:
      dump_to(obs, o)
    log.info("Succesfully written results for {0}".format(obs.utc))


def dump_to(obs, o):
 o.write("#furby.results for {1} created by furby_manager.py on {0}\n".format(time.ctime(), obs.utc))
 o.write("\n" + "---"*20 + "\n\n")
 o.write("#Injection params:\n")
 o.write("INJECTED_FURBYS:\t{0}\n".format(obs.inj_furbys))

 if obs.inj_furbys < 1:
   return
 
 o.write("FURBY_IDS\t{0}\n".format(obs.furby_ids))
 o.write("FURBY_BEAMS\t{0}\n".format(obs.furby_beams))
 o.write("FURBY_TSTAMPS\t{0}\n".format(obs.furby_tstamps))

 o.write("\n#Detection params:\n\n")
 o.write("#Pipeline\tDetected\tBeam\tTime(s)\tSNR\tDM\tWidth(ms)\tProb.")
 
 for furby in obs.furbies:
   o.write("\n\n#"+furby.name+":\n")

   o.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format("Injector", "True", furby.i_beam, furby.s_time, furby.header.SNR, furby.header.DM, furby.header.WIDTH, 1))

   if furby.heimdall_result is None:
     o.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format("Heimdall", "False", None, None, None, None, None, 1))
   else:
     o.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format("Heimdall", "True", str(furby.heimdall_result['beam']).strip("[] "), str(furby.heimdall_result['tstamp']).strip("[] "), str(furby.heimdall_result['snr']).strip("[] "), str(furby.heimdall_result['dm']).strip("[] "), str(2**furby.heimdall_result['filter_wdth'] * obs.header.TSAMP*1e-3).strip("[] "), 1))

   if furby.online_result is None:
     o.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format("Online  ", "False", None, None, None, None, None, 1))
   else:
     o.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format("Online  ", "True", str(furby.online_result['beam']).strip("[] "), str(furby.online_result['tstamp']).strip("[] "), str(furby.online_result['snr']).strip("[] "), str(furby.online_result['dm']).strip("[] "), str(2**furby.online_result['filter_wdth'] * obs.header.TSAMP*1e-3).strip("[] "), str(furby.online_result['prob']).strip("[] ")))

   if obs.processed_offline:
     if furby.offline_result is None:
       o.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format("Offline ", "False", None, None, None, None, None, 1))
     else:
       o.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format("Offline ", "True", str(furby.offline_result['beam']).strip("[] "), str(furby.offline_result['tstamp']).strip("[] "), str(furby.offline_result['snr']).strip("[] "), str(furby.offline_result['dm']).strip("[] "), str(furby.offline_result['width']).strip("[] "), 1))
   else:
     o.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format("Offline ", "False", "N/A", "N/A", "N/A", "N/A", "N/A", 0))


 o.write("\n\n"+"---"*20+"\n")
 o.write("#Dropped furbies:\n")
 if len(obs.dropped_furbies) == 0:
   o.write("None\n\n")
 else:
   for furby in obs.dropped_furbies:
     o.write("#"+furby.name+":\n")
     o.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n\n".format("Injector", "False", furby.i_beam, furby.s_time, furby.header.SNR, furby.header.DM, furby.header.WIDTH, 1))



def main(args):
  log = setup_logger(conf.log_name, args.log_level)
  log.debug("Logger setup")

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
  print "-======>"
  for utc in UTCs:
    if utc.startswith("2018-07-12") or utc.startswith("2018-07-16") or utc.startswith("2020-04-27-06:49:59"):
      log.info("Ignoring {0}".format(utc))
      print "Ignoring {0}".format(utc)
      continue
    observation = Observation(utc)
    print observation
    process(observation)

    outf = os.path.join(observation.results_dir, observation.utc, "furby.results")
    if args.show:
      outf = sys.stdout
    write_results(observation, outf, args.overwrite)
  

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
  a.add_argument("-log_level", type=str, help="Logging verbosity. Chose from :[DEBUG, INFO, WARN, ERROR, CRITICAL], [Def:INFO]", default = "INFO")

  args = a.parse_args()
  main(args)
