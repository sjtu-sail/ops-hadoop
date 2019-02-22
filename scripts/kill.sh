#!/bin/bash
./ops.sh worker stop &&
./ops.sh master stop &&
ssh root@192.168.2.12 "cd /root/OPS/scripts \n ./ops.sh worker stop"
