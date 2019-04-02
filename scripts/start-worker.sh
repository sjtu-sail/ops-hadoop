#!/bin/bash
ulimit -n 20000
./ops.sh worker start &>worker.log &
