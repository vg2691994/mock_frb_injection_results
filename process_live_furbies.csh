#!/bin/csh

if ( "$1" == "" ) then
  echo "Usage : process_live_furbies.csh <UTC>"
  exit 1
endif

set utc=$1

if ( -d "/data/mopsr/archives/$utc" ) then
  set archives_dir="/data/mopsr/archives"
else if ( -d "/data/mopsr/old_archives/$utc" ) then
  set archives_dir="/data/mopsr/old_archives"
else
  echo "Invalid UTC"
  exit 1
endif

  #set archives_dir="/data/mopsr/archives"
  #set results_dir="/data/mopsr/results"

set out_file="furby.snrs"
echo "Processing furbies for UTC: $utc"

cd $archives_dir/$utc

if (-d "FB") then
  echo "Looking for injected furbies"
else
  echo "Not a Fan-beam observation, Injection not possible, exiting"
  exit 0
endif

set flag=0


if (-d "Furbys") then
  cd Furbys

  if ( -e "pdmp.per" ) then
    rm pdmp.per
  endif
  if ( -e "pdmp.posn" ) then
    rm pdmp.posn
  endif

  set furbies=`ls $archives_dir/$utc/FB/BEAM_*/FURBY_*.dada`
  if ( "$furbies" == "" ) then
    echo "No live data + furby snippets were saved for this UTC"
    exit 0
  else
    echo "#furby                                 SNR     DM     Width   " > $out_file
    foreach furby( $furbies )
      set furby_name=`echo $furby | awk '{n=split($1,A,"/"); print A[n]}'`
      echo "furby_name $furby_name"
      
      set dm=`head -c 16384 $furby | grep -a "DM" | grep -v "DMSMEAR" | awk '{print $2}'`
      set nchan=`head -c 16384 $furby | grep -a "NCHAN" | awk '{print $2}'`
      set tres=`head -c 16384 $furby | grep -a TSAMP | awk '{print $2*0.000001}'`
      set nbit=`head -c 16384 $furby | grep -a "NBIT" | awk '{print $2}'`
      set hdr_size=`head -c 16384 $furby | grep -a "HDR_SIZE" | awk '{print $2}'`
      
      set bytes_per_sample=`echo $nbit | awk '{print $1/8}'`
      set file_size=`du -b $furby | awk '{print $1}'`
      
      set nsamps=`echo $file_size $hdr_size $nchan $bytes_per_sample | awk '{print ($1-$2)/$3/$4}'`
      set length=`echo $nsamps $tres | awk '{print $1 * $2}'`

      echo "DM:$dm NCH:$nchan Tres:$tres NB:$nbit HS:$hdr_size L:$length BpS:$bytes_per_sample FS:$file_size NS:$nsamps"
      
      #dspsr -k MO -cepoch=start -c $length -T $length -b $nsamps -D $dm $furby -O "$furby_name"
      #dspsr -k MO -cepoch=start -c $length -T $length -b 2048 -D $dm $furby -O "$furby_name"
      dspsr -k MO -cepoch=start -c $length -T $length -D $dm $furby -O "$furby_name"

      /home/vgupta/Codes/Bandpass_stuff/FRB_bp_correct.py "$furby_name.ar" -o "$furby_name.ar.bp_corr"
      
      #pam -D "$furby_name.ar.bp_corr" -e ddp
      #pam -F "$furby_name.ar.ddp" -e F

      ##pdmp -g $furby_name.png/png "$furby_name.ar"
      ##pdmp -g $furby_name.bp_corr.png/png "$furby_name.ar.bp_corr"
      #pdmp -g $furby_name.bp_corr.png/png "$furby_name.ar.F"

      #cat pdmp.per | awk '{print $8 " " $7 " " $4 " " $6}' >> $out_file
      #rm pdmp.per
      #rm pdmp.posn
      #rm $furby_name.ar
      #rm $furby_name.ar.bp_corr
      #rm $furby_name.ar.ddp
      #rm $furby_name.ar.F
    end
  endif
  cd ../

else
  echo "No furby injection in this UTC"
endif
