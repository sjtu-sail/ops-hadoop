# OPS

**OP(timized)S(huffle)** is a distributed shuffle management system which focuses on optimizing the shuffle phase of DAG computing frameworks.

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
$ mvn compile
```

## License

Apache License 2.0
