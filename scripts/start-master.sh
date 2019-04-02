#!/bin/bash
ulimit -n 20000
./ops.sh master start &>master.log &
