#!/usr/bin/env bash

# beware: this script is only good up to sf1000, beyond we need more query streams and more update sets

set -euxo pipefail

DIR=gen/sf$SF
DBGEN_PREFIX=tpch_tools_3.0.1/dbgen
STREAMS=$(python3 -c "print(max(round($SF * 0.1), 1))")
rm -rf $DIR
mkdir -p $DIR

(cd $DBGEN_PREFIX && \
./qgen -s $SF -p 1 > queries1.sql && \
./qgen -s $SF -p 2 > queries2.sql && \
./qgen -s $SF -p 3 > queries3.sql && \
./qgen -s $SF -p 4 > queries4.sql && \
./qgen -s $SF -p 5 > queries5.sql && \
./qgen -s $SF -p 6 > queries6.sql && \
./qgen -s $SF -p 7 > queries7.sql && \
./qgen -s $SF -p 8 > queries8.sql && \
./qgen -s $SF -p 9 > queries9.sql && \
./qgen -s $SF -p 10 > queries10.sql && \
./qgen -s $SF -p 11 > queries11.sql)
mv $DBGEN_PREFIX/queries*.sql $DIR

rm -f $DBGEN_PREFIX/*.tbl $DBGEN_PREFIX/*.tbl.u* $DBGEN_PREFIX/delete.*
(cd $DBGEN_PREFIX && ./dbgen -s $SF)
mv $DBGEN_PREFIX/*.tbl $DIR

(cd $DBGEN_PREFIX && ./dbgen -s $SF -U $STREAMS)

mv $DBGEN_PREFIX/*.tbl.u* $DBGEN_PREFIX/delete.* $DIR

(cd $DIR; python3 ../../convert.py)
