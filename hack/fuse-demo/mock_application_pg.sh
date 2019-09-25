#! /bin/zsh

setopt ERR_EXIT
setopt PIPE_FAIL

### data-science application placeholder/mock
# this program is a stand-in for a data-science application
#
# todo: sandbox poc python (sqla/pandas) use rather than psql

OUTPUT_PG_PORT="$1"
INPUT_PG_PORT="$2"
SOME_CONST="$3"

usage() {
    print -- 'usage: ' \
          './mock_application_pg.sh input-pg-port ouput-pg-port some-const' \
          1>&2
    exit 1
}

if [ -z "$OUTPUT_PG_PORT" ]; then
    usage
fi
if [ -z "$INPUT_PG_PORT" ]; then
    usage
fi
if [ -z "$SOME_CONST" ]; then
    usage
fi

# convention from sidecar is to create provide initial postgres su
PG_SU=postgres

# other pg users and setup are the responsibility of the application
PG_U=testpguser
PG_DB=testdb

run_sql() {
    sql_str=$1
    my_pg_port=$2
    if [[ -z $my_pg_port ]]; then
        my_pg_port=$OUTPUT_PG_PORT
    fi
    print -- "$sql_str" | psql -h localhost -p $my_pg_port -U ${PG_U} ${PG_DB}
}

# aside: postgres defaults to UNIX (filesystem)
# socket at /var/run/postgresql/*,
# not IP (network) socket, so it's the client's responsibility
# to ensure that the conn is opened at the network location

print -- "CREATE ROLE ${PG_U} WITH LOGIN CREATEDB;
CREATE DATABASE ${PG_DB} WITH OWNER ${PG_U};" | \
    psql -h localhost -p $OUTPUT_PG_PORT -U $PG_SU

run_sql 'CREATE TABLE tabla_e (
  id serial PRIMARY KEY,
  an_idx integer
);'

get_tabla_idx_vals_with_const() {
    print 'select an_idx from tabla_e;' | \
        psql -h localhost -p $INPUT_PG_PORT -U $PG_U $PG_DB | \
        awk '
BEGIN { on_row = 0 }
$0 ~ /^\(/ {if(on_row) {on_row = 0}}
{if(on_row) {print $1;}}
$0 ~ /^----/ { on_row = 1 }
' | \
        awk "{print "'$1'" + $SOME_CONST }"
}

# previously to set input bundle
initdb() {
    for idx in $(seq 1 2 9); do
        run_sql "INSERT INTO tabla_e (an_idx) VALUES (${idx}) RETURNING id;"
    done
}

for idx in $(get_tabla_idx_vals_with_const); do
    print -- "adding index $idx to output table"
    run_sql "INSERT INTO tabla_e (an_idx) VALUES (${idx}) RETURNING id;"
done
