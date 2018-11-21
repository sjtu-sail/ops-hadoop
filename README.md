# OPS

[![Build Status](https://travis-ci.org/sjtu-ist/OPS.svg?branch=master)](https://travis-ci.org/sjtu-ist/OPS)
[![GitHub](https://img.shields.io/github/license/sjtu-ist/OPS.svg)](https://github.com/sjtu-ist/OPS/blob/master/LICENSE)


**OP(timized) S(huffle)** is a distributed shuffle management system which focuses on optimizing the shuffle phase of DAG computing frameworks. The name [**OPS**](https://en.wikipedia.org/wiki/Ops) is originated from the ancient Roman religion, she was a fertility deity and earth goddess of Sabine origin. 

- [System Overview](#system-overview)
  - [Architecture](#architecture)
  - [Internal View](#internal-view)
  - [Compatibility](#compatibility)
- [Build](#build)

## System Overview

### Architecture

<p align="center"><img src="https://github.com/sjtu-ist/OPS/blob/master/pic/ops-Architecture.png" width="60%"/></p>
<p align="center">Figure 1: OPS Architecture</p>

### Internal View

<p align="center"><img src="https://github.com/sjtu-ist/OPS/blob/master/pic/ops-InternalView.png" width="80%"/></p>
<p align="center">Figure 2:  Internal View Comparison between Legacy Hadoop and Hadoop with OPS</p>

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
