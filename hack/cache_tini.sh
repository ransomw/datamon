#! /bin/zsh

POLL_INTERVAL=$((60 * 60 * 24)) # seconds
TIME_FILE='.tini_lastupdate'
VERSION_FILE='.tini_lastversion'
BASE_URL='https://github.com/krallin/tini/releases/download'

# seconds since epoch
CURR_TIME=$(date -j -f "%a %b %d %T %Z %Y" "$(date)" "+%s")
if [ ! -f $TIME_FILE ]; then
    echo $CURR_TIME > $TIME_FILE
fi
LAST_TIME=$(cat $TIME_FILE)
print -- $CURR_TIME > $TIME_FILE

TIME_DIFF=$((${CURR_TIME} - ${LAST_TIME}))


if [[ ! -f ${TINI_LOC} || ${TIME_DIFF} -gt ${POLL_INTERVAL} ]]; then
    curl -o ${TINI_LOC} \
         "${BASE_URL}/${TINI_VERSION}/tini-static-amd64"
fi
