#! /bin/zsh

#####

### util

terminate() {
    print -- "$*" 1>&2
    exit 1
}

#####

setopt ERR_EXIT
setopt PIPE_FAIL

POLL_INTERVAL=1 # sec

PG_VERSION=$(postgres --version)

PG_SU=postgres

STAGE_BASE=/pg_stage
PG_DATA_DIR_ROOT=${STAGE_BASE}/pg_data_dir
MNT_DIR_ROOT=${STAGE_BASE}/mounts
LOG_ROOT=${STAGE_BASE}/logs
UPLOAD_STAGE=${STAGE_BASE}/upload

# todo: populate from pod info via downward api
# https://kubernetes.io/docs/tasks/inject-data-application/downward-api-volume-expose-pod-information/
CFG_EMAIL="pg_wrap@oneconcern.com"
CFG_NAME="pg_wrap"

## in-bundle paths
BP_META=meta
BP_PG_VERSION=${BP_META}/pg_version
BP_DATA=data
BP_PG_TAR=${BP_DATA}/backup.tar.gz

#####

typeset COORD_POINT
typeset -a PG_DB_IDS
typeset -A PG_DB_PORTS
# dest
typeset -A PG_DB_REPOS
typeset -A PG_DB_MSGS
typeset -A PG_DB_LABELS
# src
typeset -A PG_DB_HAS_SRC
typeset -A PG_DB_SRC_REPOS
typeset -A PG_DB_SRC_BUNDLES
typeset -A PG_DB_SRC_LABELS
# temp
typeset -i pg_db_idx
typeset pg_db_id

SLEEP_INSTEAD_OF_EXIT=

while getopts Sc:xsm:l:r:b:p: opt; do
    case $opt in
        (S)
            SLEEP_INSTEAD_OF_EXIT=true
            ;;
        (c)
            COORD_POINT="$OPTARG"
            ;;
        (x)
            ((pg_db_idx++)) || true
            pg_db_id="db_${pg_db_idx}"
            PG_DB_IDS=($PG_DB_IDS $pg_db_id)
            ;;
        (s)
            PG_DB_HAS_SRC[$pg_db_id]=true
            ;;
        (p)
            PG_DB_PORTS[$pg_db_id]="$OPTARG"
            ;;
        (m)
            if [[ -z ${PG_DB_HAS_SRC[$pg_db_id]} ]]; then
                PG_DB_MSGS[$pg_db_id]="$OPTARG"
            else
                # todo: messages aren't written to source
                PG_DB_SRC_MSGS[$pg_db_id]="$OPTARG"
            fi
            ;;
        (l)
            if [[ -z ${PG_DB_HAS_SRC[$pg_db_id]} ]]; then
                PG_DB_LABELS[$pg_db_id]="$OPTARG"
            else
                PG_DB_SRC_LABELS[$pg_db_id]="$OPTARG"
            fi
            ;;
        (r)
            if [[ -z ${PG_DB_HAS_SRC[$pg_db_id]} ]]; then
                PG_DB_REPOS[$pg_db_id]="$OPTARG"
            else
                PG_DB_SRC_REPOS[$pg_db_id]="$OPTARG"
            fi
            ;;
        (b)
            if [[ -z ${PG_DB_HAS_SRC[$pg_db_id]} ]]; then
                terminate "bundle flag is only meaningful when specifying source"
            fi
            PG_DB_SRC_BUNDLES[$pg_db_id]="$OPTARG"
            ;;
        (\?)
            print -- "Bad option, aborting."
            exit 1
            ;;
    esac
done
if [[ "$OPTIND" -gt 1 ]]; then
    shift $(( OPTIND - 1 ))
fi

##

for pg_db_id in $PG_DB_IDS; do
    if [[ -z ${PG_DB_PORTS[$pg_db_id]} ]]; then
        terminate "missing port for $pg_db_id"
    fi
    if [[ -z ${PG_DB_REPOS[$pg_db_id]} ]]; then
        terminate "missing repo for $pg_db_id"
    fi
    if [[ -z ${PG_DB_MSGS[$pg_db_id]} ]]; then
        terminate "missing message for $pg_db_id"
    fi
    if [[ -n ${PG_DB_HAS_SRC[$pg_db_id]} ]]; then
        if [[ -z ${PG_DB_SRC_REPOS[$pg_db_id]} ]]; then
            terminate "missing source repo for $pg_db_id"
        fi
        if [[ -z ${PG_DB_SRC_BUNDLES[$pg_db_id]} && -z ${PG_DB_SRC_LABELS[$pg_db_id]} ]]; then
            terminate "no source data specified for $pg_db_id"
        fi
        if [[ -n ${PG_DB_SRC_BUNDLES[$pg_db_id]} && -n ${PG_DB_SRC_LABELS[$pg_db_id]} ]]; then
            terminate "specifying source data by bundleid or and labelid is mutually exclusive"
        fi
    fi
done

#####

### util

# #% =EVENT_NAME= <- wrap_application.sh
await_event() {
    COORD_DONE=
    EVENT_NAME="$1"
    DBG_MSG="$2"
    DBG_POLLS="$3"
    if [[ -n $DBG_MSG ]]; then
        echo "$DBG_MSG"
    fi
    while [[ -z $COORD_DONE ]]; do
        if [[ -f "${COORD_POINT}/${EVENT_NAME}" ]]; then
            COORD_DONE=1
        fi
        if [[ -n $DBG_POLLS ]]; then
            echo "... $DBG_MSG ..."
        fi
        sleep "$POLL_INTERVAL"
    done
}

#% wrap_application.sh <- =EVENT_NAME=
emit_event() {
    EVENT_NAME="$1"
    DBG_MSG="$2"
    echo "$DBG_MSG"
    touch "${COORD_POINT}/${EVENT_NAME}"
}

dbg_print() {
    typeset dbg=true
    if $dbg; then
        print -- $*
    fi
}

# todo: this could wait on the process, escalate signal, etc.
slay() {
    typeset -i pid
    typeset pids_str
    typeset -a pids_arr
    typeset -i num_tries
    typeset sent_term
    sent_term=false
    pid="$1"
    kill $pid
    while true; do
        if [[ num_tries -eq 10 ]]; then
           dbg_print "sending SIGTERM to $pid"
           kill -9 $pid
        fi
        pids_str=$(ps | awk 'NR > 1 { print $1 }')
        pids_arr=(${(f)pids_str})
        if ! ((${pids_arr[(Ie)$pid]})); then
            break
        fi
        dbg_print "awaiting $pid exit after signal"
        sleep 1
        ((num_tries++)) || true
    done
}

# todo: block until clean shutdown or error out
stop_postgres() {
    pid="$1"
    kill $pid
}

#####

mkdir -p $PG_DATA_DIR_ROOT
mkdir -p $MNT_DIR_ROOT
mkdir -p $LOG_ROOT

##

datamon config create \
        --name $CFG_NAME \
        --email $CFG_EMAIL

##

dbg_print "setting privs on fuse device"
sudo chgrp developers /dev/fuse

dbg_print "starting postgres database processes"

typeset -A pg_pids
for pg_db_id in $PG_DB_IDS; do
    data_dir=${PG_DATA_DIR_ROOT}/${pg_db_id}
    if [[ -e ${data_dir} ]]; then
        terminate "data directory path $data_dir already exists"
    fi
    mkdir ${data_dir}
    # ??? could download the entire bundle upfront instead of mount?
    if [[ -n ${PG_DB_HAS_SRC[$pg_db_id]} ]]; then
        mount_dir=${MNT_DIR_ROOT}/${pg_db_id}
        mount_params=(bundle mount \
                             --stream \
                             --repo ${PG_DB_SRC_REPOS[$pg_db_id]} \
                             --mount $mount_dir)
        log_file_mount=${LOG_ROOT}/datamon_mount.${pg_db_id}.log
        if [[ -z ${PG_DB_SRC_BUNDLES[$pg_db_id]} ]]; then
            mount_params=($mount_params --label ${PG_DB_SRC_LABELS[$pg_db_id]})
        else
            mount_params=($mount_params --bundle ${PG_DB_SRC_BUNDLES[$pg_db_id]})
        fi
        if [[ -e $mount_dir ]]; then
            terminate "mount directory path $mount_dir already exists"
        fi
        mkdir $mount_dir
        unsetopt ERR_EXIT
        datamon $mount_params > $log_file_mount 2>&1 &
        datamon_status=$?
        datamon_pid=$!
        setopt ERR_EXIT
        if [[ ! $datamon_status -eq 0 ]]; then
            cat $log_file_mount
            terminate "error starting 'datamon $mount_params'"
        fi
        dbg_print "started 'datamon $mount_params' with log ${log_file_mount}"
        # block until mount found by os
        mount_waiting=true
        mount_data=$(mount | cut -d" " -f 3,5)
        if $mount_waiting; then
            if print "$mount_data" | grep -q "^$mount_dir fuse$"; then
                mount_waiting=false
            fi
            dbg_print "waiting on mount at $mount_dir"
            sleep 1
        fi
        # verify metadata
        bundle_pg_version=$(cat ${mount_dir}/${BP_PG_VERSION})
        if [[ $bundle_pg_version != $PG_VERSION ]]; then
            terminate "pg version mistmatch $bundle_pg_version -- $PG_VERSION"
        fi
        (cd $data_dir && \
             >${LOG_ROOT}/untar.${pg_db_id}.log \
              2>${LOG_ROOT}/untar_err.${pg_db_id}.log \
              tar -xvf ${mount_dir}/${BP_PG_TAR})
        slay $datamon_pid
        chmod -R 750 $data_dir
    else
        # --no-locale flag helps artifact portability
        # ??? other parms to set here?
        initdb --no-locale -D $data_dir
    fi
    log_file_pg=${LOG_ROOT}/pg.${pg_db_id}.log
    log_file_pg_err=${LOG_ROOT}/pg_err.${pg_db_id}.log
    unsetopt ERR_EXIT
    >${log_file_pg} 2>${log_file_pg_err} \
     postgres -D $data_dir -p $PG_DB_PORTS[$pg_db_id] &
    pg_pid=$!
    pg_status=$?
    setopt ERR_EXIT
    if [[ ! $pg_status -eq 0 ]]; then
        cat ${log_file_pg}
        cat ${log_file_pg_err}
        terminate "error starting postgres"
    fi
    pg_pids[$pg_db_id]=$pg_pid
    if [[ -n ${PG_DB_HAS_SRC[$pg_db_id]} ]]; then
        # todo
        print -- 'delay on extant db start unimpl, defaulting to dumb timeout'
        sleep 5
    else
        while ! &>/dev/null createuser -p $PG_DB_PORTS[$pg_db_id] -s $PG_SU; do
            print "waiting on ${pg_db_id} db start..."
            sleep $POLL_INTERVAL
        done
    fi
done

dbg_print "postgres database processes started"

emit_event \
    'dbstarted' \
    'dispatching db started event'

await_event \
    'initdbupload' \
    'waiting on db upload event'

# todo: vaccum

dbg_print "stopping postgres processes"

for pg_db_id in $PG_DB_IDS; do
    stop_postgres $pg_pids[$pg_db_id]
done

# todo: dumb timout on postgres shutdown
dbg_print "dumb timeout on postgres shutdown"
sleep 10

dbg_print "uploading data directories"

if [[ -e ${UPLOAD_STAGE} ]]; then
    terminate "upload staging area ${UPLOAD_STAGE} already exists"
fi

for pg_db_id in $PG_DB_IDS; do
    mkdir -p ${UPLOAD_STAGE}
    mkdir ${UPLOAD_STAGE}/${BP_META}
    mkdir ${UPLOAD_STAGE}/${BP_DATA}
    dbg_print "prepare staging area"
    data_dir=${PG_DATA_DIR_ROOT}/${pg_db_id}
    (cd $data_dir && \
       >${LOG_ROOT}/tar.${pg_db_id}.log 2>${LOG_ROOT}/tar_err.${pg_db_id}.log \
         tar -cvf ${UPLOAD_STAGE}/${BP_PG_TAR} *)
    print -- ${PG_VERSION} > ${UPLOAD_STAGE}/${BP_PG_VERSION}
    dbg_print "perform upload"
    log_file_upload=${LOG_ROOT}/datamon_upload.${upload_idx}.log
    upload_params=(bundle upload \
                          --path ${UPLOAD_STAGE} \
                          --message $PG_DB_MSGS[$pg_db_id] \
                          --repo $PG_DB_REPOS[$pg_db_id])
    if [[ -n $PG_DB_LABELS[$pg_db_id] ]]; then
        upload_params=($upload_params --label $PG_DB_LABELS[$pg_db_id])
    fi
    unsetopt ERR_EXIT
    datamon $upload_params > $log_file_upload 2>&1
    datamon_status=$?
    setopt ERR_EXIT
    if [[ ! $datamon_status -eq 0 ]]; then
        cat $log_file_upload
        terminate "upload command failed"
    fi
    dbg_print "upload command had nominal status"
    rm -rf ${UPLOAD_STAGE}
done

emit_event \
    'dbuploaddone' \
    'dispatching db upload done event'

if [[ -z $SLEEP_INSTEAD_OF_EXIT ]]; then
    exit 0
fi

echo "wrap_datamon_pg sleeping indefinitely (for debug)"
while true; do sleep 100; done
