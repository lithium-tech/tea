<div align=center>

[![Apache 2.0 License](https://img.shields.io/badge/license-Apache%202.0-blueviolet?style=for-the-badge)](https://www.apache.org/licenses/LICENSE-2.0)

</div>

Tea is an open-source extension for Greenplum Database that allows it to access Apache Iceberg™ data from S3-compatible storage written in Apache Parquet format.
Tea adopts [PXF](https://github.com/greenplum-db/pxf-archive) logic to standard PostgreSQL/Greenplum interfaces such as
[Foreign data](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-greenplum/6/greenplum-database/ref_guide-sql_commands-CREATE_FOREIGN_TABLE.html) and
[External tables](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-greenplum/6/greenplum-database/ref_guide-sql_commands-CREATE_EXTERNAL_TABLE.html)

## Restrictions

Greenplum 6 has restricted support of `FOREIGN TABLE`. Especially you cannot use [GP Orca](https://www.vmware.com/docs/white-paper-orca-a-modular-query-optimizer-architecture-for-big-data) with them.

## Setup

Edit $GPHOME/tea/tea-config.json. At least you have to set access and secret keys to object storage
```
{
    "common": {
        "s3": {
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "endpoint_override": "127.0.0.1:9000",
            "scheme": "http"
        }
    }
}
```
You are able to set additional profiles sections that override `common` settings.
More configuration fields you can find in [tea-config.json](test/config/tea-config.json) and [tea-config-schema.json](test/config/tea-config-schema.json).

## How to use

To access data from Apache Iceberg you should register `tea` extention and create `FOREIGN TABLE` to every Iceberg table you want to read.

#### FDW
```
CREATE EXTENSION tea;

CREATE FOREIGN TABLE table_name (...)
SERVER tea_server
OPTIONS(location 'tea://icebergs3://s3:// ... .metadata.json');
```
It creates a `FOREIGN TABLE` linked to an Iceberg table described in JSON from S3.
Created table is accessible for reading.

## Update Tea

Extract a new version
```
export GPHOME=/path/to/greenplum
tar xzf tea-linux-1.71.0.tar.gz --strip-components=1 -C $GPHOME
```
Update extension in PostgreSQL's way
```
ALTER EXTENSION tea UPDATE TO 1.71.0;
SELECT installed_version FROM pg_available_extensions WHERE name='tea';
```
Downgrade extension if needed
```
ALTER EXTENSION tea UPDATE TO 1.70.0;
```

## How to build on Ubuntu 22.04

Install GCC 13
```
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt-get install gcc-13 g++-13
```

Create a directory for source and binaries
```
mkdir $HOME/compile
COMPILE_DIR=$HOME/compile
```

Get CMake 3 (>=3.25)
```
cd $COMPILE_DIR
wget https://github.com/Kitware/CMake/releases/download/v3.31.11/cmake-3.31.11-linux-x86_64.tar.gz
tar xf cmake-3.31.11-linux-x86_64.tar.gz
PATH=$COMPILE_DIR/cmake-3.31.11-linux-x86_64/bin:$PATH
```

Get TEA source
```
git clone https://github.com/lithium-tech/tea -b OPENGPDB_GP6
```

Build and install Arrow
```
git clone https://github.com/apache/arrow.git -b maint-15.0.2
cd arrow
git apply $COMPILE_DIR/tea/vendor/arrow/fix_c-ares_url.patch
./cpp/thirdparty/download_dependencies.sh $COMPILE_DIR/arrow-thirdparty
mkdir cpp/build
cd $_
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$COMPILE_DIR/bin \
  -DCMAKE_C_COMPILER=gcc-13 -DCMAKE_CXX_COMPILER=g++-13 \
  -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF \
  -DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_NO_DEPRECTATED_API=ON \
  -DARROW_LLVM_USE_SHARED=OFF -DARROW_FILESYSTEM=ON -DARROW_PARQUET=ON \
  -DARROW_S3=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_LZ4=ON \
  -DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=ON -DARROW_IPC=ON -DARROW_CSV=ON \
  -DARROW_WITH_RAPIDJSON=ON -DARROW_GANDIVA=ON -DARROW_COMPUTE=ON ..
make -j`nproc`
make install
```

Build and install gRPC
```
cd $COMPILE_DIR
git clone https://github.com/grpc/grpc.git -b v1.62.3
cd grpc
git submodule update --init --single-branch --depth 1
mkdir build
cd $_
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$COMPILE_DIR/bin \
  -DCMAKE_C_COMPILER=gcc-13 -DCMAKE_CXX_COMPILER=g++-13 \
  -DgRPC_BUILD_SHARED_LIBS=OFF -DgRPC_BUILD_STATIC_LIBS=ON \
  -DgRPC_BUILD_TESTS=OFF -DgRPC_BUILD_EXAMPLES=OFF \
  -DgRPC_BUILD_CSHARP_EXT=OFF -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF \
  -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF \
  -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF \
  -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF  -DgRPC_SSL_PROVIDER:STRING=package ..
make -j`nproc`
make install
```

Build TEA. Replace `$COMPILE_DIR/gpdb_bin` with your OpenGPDB root.
```
cd $COMPILE_DIR/tea
mkdir -p build/arrow-thirdparty
cd build
cp $COMPILE_DIR/arrow-thirdparty/* arrow-thirdparty/
cmake .. -GNinja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=$COMPILE_DIR/bin \
  -DCMAKE_C_COMPILER=gcc-13 -DCMAKE_CXX_COMPILER=g++-13 \
  -DGreenplum_ROOT=$COMPILE_DIR/gpdb_bin/  
ninja
ninja install
```
