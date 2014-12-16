#!/bin/bash -xe
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# Tests that the Kudu client sample code can be built out-of-tree and runs
# properly.

NUM_PROCS=$(cat /proc/cpuinfo | grep processor | wc -l)

# Install the client library to a temporary directory.
LIBRARY_DIR=$(mktemp -d)
PREFIX_DIR=$LIBRARY_DIR/usr/local
SAMPLES_DIR=$PREFIX_DIR/share/doc/kuduClient/samples

ROOT=$(readlink -f $(dirname "$BASH_SOURCE"))/../../..
pushd $ROOT
make -j$NUM_PROCS DESTDIR=$LIBRARY_DIR install
popd

# Build the client samples using the client library.
pushd $SAMPLES_DIR
CMAKE_PREFIX_PATH=$PREFIX_DIR cmake .
make -j$NUM_PROCS
popd

# Start master+ts
BASE_DIR=$(mktemp -d)
$ROOT/build/latest/kudu-master \
  --log_dir=$BASE_DIR \
  --master_base_dir=$BASE_DIR/master \
  --use_hybrid_clock=true \
  --max_clock_sync_error_usec=10000000 &
MASTER_PID=$!
$ROOT/build/latest/kudu-tablet_server \
  --log_dir=$BASE_DIR \
  --tablet_server_base_dir=$BASE_DIR/ts \
  --use_hybrid_clock=true \
  --max_clock_sync_error_usec=10000000 &
TS_PID=$!

# Let them run for a bit.
sleep 5

# Run the samples (temporarily disabling exit-on-error).
set +e
$SAMPLES_DIR/sample
RESULT=$?
set -e

# Clean up.
kill -9 $TS_PID || :
kill -9 $MASTER_PID || :
rm -rf $BASE_DIR
rm -rf $LIBRARY_DIR

if [ $RESULT != 0 ]; then
  echo "Sample test failed!"
  exit $RESULT
fi