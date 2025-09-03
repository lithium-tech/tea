# Contributing

All members of our community are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md). Please make sure you are
welcoming and friendly in all of our spaces.

## Technical Information

Tea uses general PostgreSQL extentions API to connect to Greenplum DBMS.
You could think about Tea as C++ adaptation for [PXF](https://github.com/greenplum-db/pxf-archive) with Apache Iceberg support.
We use our [iceberg-cxx](https://github.com/lithium-tech/iceberg-cxx) library to access [Apache Iceberg](https://iceberg.apache.org/spec/) data.
Tea activelly uses [Apache Arrow](https://arrow.apache.org/) primitives including [Gandiva Expression Compiler](https://arrow.apache.org/docs/cpp/gandiva.html) inside.

Tea has parts of code coppied from PortgreSQL/Greenplum and PFX coding base. It's some sort of `PostgreSQL to C++` bridge we got with minimal changes.
Don't blame us for these parts of code :)

## Pull Request Process

1. In order to make your contribution please make a fork of the repository
2. Make changes in feature branche in your fork
3. We recommend to use [Conventional Commits](https://www.conventionalcommits.org/) in commit messages
4. Make sure you've covered all the code changes with tests
5. Create Pull Request into original repository into main branch
