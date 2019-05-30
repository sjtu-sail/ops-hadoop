#!/bin/bash
ulimit -n 20000
./ops.sh worker start &>/expose/ops-worker.log &
