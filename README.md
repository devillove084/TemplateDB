# Evolution of KeyValue System

> This project is currently for personal study and research purposes only, so please use caution for commercial use.

## What is TemplateKV

TemplateKV is an open-source, future-oriented, key-value (KV) infrastructure for databases. It offers dynamic data structure adjustment, elasticity to adapt to various levels of distribution, and the ability to perceive and operate on multiple dimensions, such as system, hardware, cluster, and load, in an asynchronous manner. Its focus is on providing high abstraction and high-performance solutions to meet the diverse underlying storage needs of different databases.

- High performance

  TemplateKV uses the latest concurrency technology and the latest popular hardware to make reading and writing silky smooth.

- Ultra Elasticity

  Use dynamic combination of cell-level operators to allow the runtime system to scale at a finer granularity.

- Multi-dimensional perception

  Dynamically senses multiple dimensions of operating system, hardware, workload, and user configuration to squeeze every machine dry.

- Asynchronous Arithmetic Manipulation

  Design different arithmetic logic for different layers, request, read/write, plan generation in the whole life cycle.

- Multiple protocol support

  Support mainstream data read/write protocols for community integration.

## Architecture(pre)

![kv](assets/kv.png)

## Try TemplateKV

``` bash
# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# Clone the repo
git clone https://github.com/devillove084/TemplateKV.git && cd TemplateKV
# Compile
cargo build --release
# Test
cargo install nextest # (Optional)Use next test framework
cargo test # (Optional)cargo nextest test
```

## Getting Started

<details>
<summary>Deploying TemplateKV</summary>


- [Understanding Deployment Modes](docs/deploy/how_to_deploy.md)

- [Deploying a Standalone TemplateKV](docs/deploy/standalone.md)

- [Deploying a Distribute TemplateKV](docs/deploy/distributed.md)

  </details>

<details>
<summary>Connecting to TemplateKV</summary>


- [How to Connect TemplateKV with MySQL Client](docs/connect/mysql.md)
- [How to Connect TemplateKV with ClickHouse HTTP Handler](docs/connect/clickhouse.md)
- [How to Execute Queries in Python](docs/develop/py.md)
  </details>

<details>
<summary>Loading Data into TemplateKV</summary>


- [How to Load Data from Local File System](docs/loaddata/fs.md)
- [How to Load Data from Remote Files](docs/loaddata/remote.md)
- [How to Load Data from Amazon S3](docs/loaddata/s3.md)
- [How to Unload Data from TemplateKV](docs/loaddata/unload.md)
  </details>

<details>
<summary>Learning TemplateKV</summary>


- Design of Dynamic Data Structure

- Design of Stream Operators 

- Design of Unified Consensus Protocol Abstraction Layer

  </details>

<details>
<summary>Performance</summary>


- [How to Benchmark TemplateKV using TPC-H](docs/bench/tpch.md)
  </details>

## Contributors

[@devillove084](https://github.com/devillove084)

[@Sisphyus](https://github.com/Sisphyus)

## Contribution

Welcome! [Post Issues](https://github.com/devillove084/TemplateKV/issues/new) or submit a Pull Request, refer to the[CONTRIBUTING.md](https://github.com/devillove084/TemplateKV/main/docs/CONTRIBUTING.md).

TemplateKV follow the [Contributor Covenant](http://contributor-covenant.org/version/1/3/0/) code of conduct.

## Roadmap

- [Roadmap v0.2](https://github.com/users/devillove084/projects/1/views/2?layout=table)

## License

TemplateKV is released under the [Apache License 2.0](LICENSE)

When contributing to Databend, you can find the relevant license header in each code file.

