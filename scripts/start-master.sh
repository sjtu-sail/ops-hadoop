#!/bin/bash
ulimit -n 20000
./ops.sh master start &>/expose/ops-master.log &
