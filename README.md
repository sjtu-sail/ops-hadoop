# OPS

[![Build Status](https://travis-ci.org/sjtu-ist/OPS.svg?branch=master)](https://travis-ci.org/sjtu-ist/OPS)
[![GitHub](https://img.shields.io/github/license/sjtu-ist/OPS.svg)](https://github.com/sjtu-ist/OPS/blob/master/LICENSE)

**OP**timized **S**huffle is a distributed shuffle management system which focuses on optimizing the shuffle phase of DAG computing frameworks. The name [**OPS**](https://en.wikipedia.org/wiki/Ops) is originated from the ancient Roman religion, she was a fertility deity and earth goddess of Sabine origin. 

- [OPS](#ops)
  - [System Overview](#system-overview)
    - [Architecture](#architecture)
    - [Internal View](#internal-view)
    - [LifeCycle](#lifecycle)
    - [ETCD File Structure](#etcd-file-structure)
    - [Compatibility](#compatibility)
  - [Build](#build)
  - [Run](#run)
  - [License](#license)

## System Overview

### Architecture

<p align="center"><img src="https://github.com/sjtu-ist/OPS/blob/master/fig/ops-Architecture.png" width="60%"/></p>
<p align="center">Figure 1: OPS Architecture</p>

### Internal View

<p align="center"><img src="https://github.com/sjtu-ist/OPS/blob/master/fig/ops-InternalView.png" width="80%"/></p>
<p align="center">Figure 2:  Internal View Comparison between Legacy Hadoop and Hadoop with OPS</p>

### LifeCycle

<p align="center"><img src="https://github.com/sjtu-ist/OPS/blob/master/fig/ops-LifeCycle.png" width="80%"/></p>
<p align="center">Figure 3:  Lifecyle of OPS</p>

### ETCD File Structure

- ops/
  - jobs/
    - job-\${jobId}: JobConf
  - shuffle/
    - reduceNum/
      - reduceNum-\${nodeIp}-\${jobId}-\${reduceId}: num
    - mapCompleted/
      - mapCompleted-\${nodeIp}-\${jobId}-\${mapId}: MapConf
    - shuffleCompleted/
      - shuffleCompleted-\${dstNodeIp}-\${jobId}-\${num}-\${mapId}: path
  - tasks/
    - reduceTasks/
      - reduceTask-\${nodeIp}-\${jobId}-\${reduceId}: ReduceConf

### Compatibility

In order to use OPS, the modification on DAG computing frameworks is inevitable.
For now, we only implement OPS on Hadoop MapReduce. Our customized Hadoop is available in [here](https://github.com/sjtu-ist/hadoop).
We believe that the costs of enabling OPS on other DAG computing frameworks are also very low.

## Build

```bash
$ mvn clean install
```

## Run

```bash
OPS: Optimized Shuffle

Usage:
  ops.sh [command]

Commands:
  master         OpsMaster
  worker         OpsWorker

Options:
  -h, --help     Show usage

Use ops.sh [command] --help for more information about a command.
```

## License

[Apache License 2.0](https://github.com/sjtu-ist/OPS/blob/master/LICENSE)
