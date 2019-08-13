#!/bin/sh
set -ex
dirs="./directories"
log=processing.log
while read line
do
  file="/filestore/output/"$line
  echo "==Next==" | tee -a $log
  date | tee -a $log
  echo "Processing " $line | tee -a $log
  # Count number of entries in directory
  find "/filestore/output/"$line -type f | wc -l| tee -a $log
  # Upload to datamon
  label=$line"-aug-09-2019"
  echo "label="$label
  ./datamon bundle upload --concurrency-factor 300 --skip-on-error --repo backup-filestore-output --path $file --label $label --message "backup aug 9th" 1> datamon.log 2> datamon.err
  # check number of entries
  ./datamon bundle list files --repo backup-filestore-output --label $label > $line-files.log
  # If correct
  count=`cat $line-files.log| grep -i name |wc -l`
  echo $count in bundle
  count2=`find $file -type f | wc -l`
  echo $count2 in nfs
  if [ $count -eq $count2 ]
  # delete old files
  then
    echo "Deleting " $line | tee -a $log
    find $file -mtime +20 -delete | tee -a $log
  fi
  echo "Done " $file | tee -a $log
done < $dirs
