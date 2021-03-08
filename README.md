# ArrowKV

> 本项目设计的目的是使用Rust设计一套现代化的KV存储引擎

## Features

* 只支持PM(非易失性内存)
* 使用Bw-Tree优化并发索引
* 以8Bytes区分大小对象的写入，保证原子性
* 并发Compaction
* [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)
* Epoch && LSA && Work-stealing
* 优化Level-0层的seach操作，优先搜索最新的sst，利用sst前缀压缩机制优化搜索速度

## Design

> 简化LSM-Tree中多层Level的设计，采用单层Level，进行Major Compaction后，直接对Level-0层中的数据进行选择性合并，落盘在PM或者SSD中的数据直接为全局有序Block，再使用Bw-Tree(下图以B+Tree代替)对落盘的数据进行索引，提高查询速度。

![Single-Level KV Store with Persistent Memory](https://pic4.zhimg.com/v2-84981aa8d374873f5df4ebf1f7be23ec_1440w.jpg?source=172ae18b) 

### Put

* key-value insert Persistent MemTable中
* 如果MemTable达到一定的大小，转换成Immutable MemTable
* 如果Immutable MemTable个数到达一定数量，那么flush到disk中，且将相应key的索引结构更新的Global Bw-tree中

### Get

* 在MemTable查
* 如果MemTable不存在，在Immutable MemTable查
* 如果上面两个都不存在，在Global Bw-Tree查

### RangeQuery

* 查询MemTable相应的range的数据
* 查询Immutable MemTable相应的range的数据
* 查询Bw-Tree查询Disk上相应range的数据