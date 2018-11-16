# OPS

[![Build Status](https://travis-ci.org/sjtu-ist/OPS.svg?branch=master)](https://travis-ci.org/sjtu-ist/OPS)
[![GitHub](https://img.shields.io/github/license/sjtu-ist/OPS.svg)](https://github.com/sjtu-ist/OPS/blob/master/LICENSE)


**OP(timized)S(huffle)** is a distributed shuffle management system which focuses on optimizing the shuffle phase of DAG computing frameworks.

- [System Overview](#system-overview)
  - [Architecture](#architecture)
  - [Internal View](#internal-view)
  - [Compatibility](#compatibility)
- [Build](#build)

## System Overview

### Architecture

<center width="60%">

![OPS Architecture](https://github.com/sjtu-ist/OPS/blob/master/pic/ops-Architecture.png)

Figure 1: OPS Architecture

</center>

### Internal View

<center width="80%">

![OPS Architecture](https://github.com/sjtu-ist/OPS/blob/master/pic/ops-InternalView.png)

Figure 2: Internal View Comparison between Legacy Hadoop and Hadoop with OPS

</center>

### Compatibility

In order to use OPS, the modification on DAG computing frameworks is inevitable.
For now, we only implement OPS on Hadoop MapReduce. Our customized Hadoop is available in [here](https://github.com/sjtu-ist/hadoop).
We believe that the costs of enabling OPS on other DAG computing frameworks are also very low.

## Build

```bash
$ mvn install compile
```

## License

[Apache License 2.0](https://github.com/sjtu-ist/OPS/blob/master/LICENSE)
