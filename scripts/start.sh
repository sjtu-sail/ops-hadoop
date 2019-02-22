#!/bin/bash
./ops.sh master start &>master.log &
./ops.sh worker start &>worker.log &
ssh root@192.168.2.12 "cd /root/OPS/scripts \n ./ops.sh worker start &>worker.log &"
