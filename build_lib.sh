#!/bin/bash

NAME=tmp

# ./clean.sh
# mkdir -p jiffy
# mkdir -p asyncreflex
mkdir -p build

if [[ "$1" == "use-pocket" ]]; then
mkdir -p pocket
# Get Pocket Lib
POCKET_ROOT=$NAME:/pocket/client
LIB_ROOT=$NAME:/usr/lib/x86_64-linux-gnu
# LIB_ROOT=/usr/lb64
docker build -f lib_docker/libpocket.dockerfile -t esbench:libpocket .
docker create --name $NAME esbench:libpocket
docker cp $POCKET_ROOT/pocket.py lib/pocket/
docker cp $POCKET_ROOT/build/client/libcppcrail.so lib/pocket/
docker cp $POCKET_ROOT/build/pocket/libpocket.so lib/pocket/
docker cp $POCKET_ROOT/build/iobench/iobench lib/pocket/
docker cp $LIB_ROOT/libboost_python38.so.1.71.0 lib/pocket/
# TODO: are these two soft links needed?
docker cp $LIB_ROOT/libc.so.6 lib/pocket/
docker cp $LIB_ROOT/libstdc++.so.6 lib/pocket/
docker rm -v $NAME
fi

# Get AsyncReFlex Lib
docker build -f lib_docker/asyncreflex.dockerfile -t esbench:asyncreflex .
docker create --name $NAME esbench:asyncreflex

# Create the asyncreflex directory if it doesn't exist
mkdir -p lib/asyncreflex

# Copy only .py and .so files
docker cp $NAME:/asyncreflex/asyncreflex/. build/asyncreflex
find build/asyncreflex -type f ! -name "*.py" ! -name "*.so" -delete

# Clean up
docker rm -v $NAME


# Get Jiffy Lib
# JIFFY_ROOT=$NAME:/jiffy/build/pyjiffy
JIFFY_ROOT=$NAME:/jiffy/pyjiffy
docker build -f lib_docker/libjiffy.dockerfile -t esbench:libjiffy .
docker create --name $NAME esbench:libjiffy
docker cp $JIFFY_ROOT/jiffy build/
docker rm -v $NAME
