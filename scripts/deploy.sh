#!/bin/bash

if [ ! -d OPS ]; then
    git clone https://github.com/sjtu-ist/OPS.git -b master
else
    git pull origin master
fi

cd OPS

mvn test
if [ $? -ne 0 ]; then
    exit 1
else
    mvn clean package
fi

if [ -f target/ops.jar ]; then
    java -cp target/ops.jar cn.edu.sjtu.ist.ops.OpsMaster
fi
