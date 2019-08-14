#!/bin/zsh

setopt ERR_EXIT

backup_dirs_filelist

backup_dir=
backup_dirs_filelist=

log=processing.log

while getopts d:f: opt; do
    case $opt in
        (d)
            backup_dir="$OPTARG"
            ;;
        (f)
            backup_dirs_filelist="$OPTARG"
            ;;
        (\?)
            print Bad option, aborting.
            exit 1
            ;;
    esac
done
(( OPTIND > 1 )) && shift $(( OPTIND - 1 ))

if [[ -n $backup_dir && -n $backup_dirs_filelist ]]; then
    print 'backup directory (-d) and backup filelist (-f) params are mutually exclusive' 1>&2
    exit 1
fi
if [[ -z $backup_dir && -z $backup_dirs_filelist ]]; then
    print 'must specify at least one of backup directory (-d) and backup filelist (-f) params' 1>&2
    exit 1
fi
if [[ -n $backup_dir ]]; then
    if [[ ! -d $backup_dir ]]; then
        print "backup directory $backup_dir doesn't exist"
        exit 1
    fi
fi

##

if [[ -z $backup_dirs_filelist ]]; then
    backup_dirs_filelist=/tmp/datamover-backup.list
    find . -type d -mindepth 1 -maxdepth 1 > $backup_dirs_filelist
fi


while read line; do
    file="/filestore/output/${line}"
    print "==Next==" | tee -a $log
    date | tee -a $log
    print "Processing " $line | tee -a $log
    # Count number of entries in directory
    find "/filestore/output/${line}" -type f |wc -l | tee -a $log
    # Upload to datamon
    label="${line}-aug-09-2019"
    print "label=${label}"
    1>datamon.log 2>datamon.err \
    ./datamon bundle upload \
        --concurrency-factor 300 \
        --skip-on-error \
        --repo backup-filestore-output \
        --path $file \
        --label $label \
        --message "backup aug 9th"
    # check number of entries
    ./datamon bundle list files \
        --repo backup-filestore-output \
        --label $label \
        > ${line}-files.log
    # If correct
    count=$(cat $line-files.log |grep -i name |wc -l)
    print -- "$count in bundle"
    count2=$(find $file -type f | wc -l)
    print -- "$count2 in nfs"
    if [ $count -eq $count2 ]; then
        # delete old files
        echo "Deleting " $line | tee -a $log
        find $file -mtime +20 -delete | tee -a $log
    fi
    echo "Done " $file | tee -a $log
done < $backup_dirs_filelist
