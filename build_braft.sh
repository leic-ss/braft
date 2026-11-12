#!/bin/bash

work_home=`cd $(dirname $0); pwd`

export CMAKE_PREFIX_PATH=${work_home}/brpc_third_lib

cd bld
cmake ..
make -j3

# 编译braft的依赖库（将brpc库拷贝到编译brpc的依赖库）
# ls ../braft/brpc_third_lib/include/
# brpc     butil  gflags  google            idl_options.proto  leveldb    openssl     snappy.h             snappy-stubs-public.h
# bthread  bvar   glog    idl_options.pb.h  json2pb            mcpack2pb  snappy-c.h  snappy-sinksource.h


# 编译brpc脚本
# cat build_brpc.sh
# #!/bin/bash
# work_home=`cd $(dirname $0); pwd`
# export LD_LIBRARY_PATH=$LD_LIBRARY_PAT:${work_home}/brpc_third_lib/lib
# ./config_brpc.sh --headers=${work_home}/brpc_third_lib/include --libs=${work_home}/brpc_third_lib/lib
# make


# 编译brpc的依赖库
# ls ../brpc/brpc_third_lib/include/
# gflags  glog  google  leveldb  openssl  snappy-c.h  snappy.h  snappy-sinksource.h  snappy-stubs-public.h




# tair依赖库版本信息：
# brpc          glog-0.3.5    lz4-1.9.3           lz4-1.9.3.tar.gz    openssl-1.1.1l  snappy-1.1.7   zlib-1.2.11
# gflags-2.2.0  leveldb-1.20  lz4-1.9.3_1.tar.gz  mariadb-conn-1.1.6  protobuf-3.8.0  spdlog-1.15.1  zookeeper-3.5.9


# braft依赖库版本信息，1.5.0
#[root@localhost braft]# ls brpc_third_lib/*
#brpc_third_lib/bin:
#protoc

#brpc_third_lib/include:
#brpc     butil  gflags  google            idl_options.proto  leveldb    openssl     snappy.h             snappy-stubs-public.h
#bthread  bvar   glog    idl_options.pb.h  json2pb            mcpack2pb  snappy-c.h  snappy-sinksource.h

#brpc_third_lib/lib:
#cmake         libcrypto.so.1.1       libglog.so.0        libprotobuf.a           libprotobuf-lite.so.19.0.0  libprotoc.so         libssl.so.1.1
#engines-1.1   libgflags.a            libglog.so.0.0.0    libprotobuf.la          libprotobuf.so              libprotoc.so.19      pkgconfig
#libbrpc.a     libgflags_nothreads.a  libleveldb.a        libprotobuf-lite.a      libprotobuf.so.19           libprotoc.so.19.0.0
#libbrpc.so    libglog.a              libleveldb.so       libprotobuf-lite.la     libprotobuf.so.19.0.0       libsnappy.a
#libcrypto.a   libglog.la             libleveldb.so.1     libprotobuf-lite.so     libprotoc.a                 libssl.a
#libcrypto.so  libglog.so             libleveldb.so.1.20  libprotobuf-lite.so.19  libprotoc.la                libssl.so


# brpc依赖库版本信息，v1.1.2
#[root@localhost brpc]# ls brpc_third_lib/*
#brpc_third_lib/include:
#gflags  glog  google  leveldb  openssl  snappy-c.h  snappy.h  snappy-sinksource.h  snappy-stubs-public.h

#brpc_third_lib/lib:
#cmake             libgflags_nothreads.a  libleveldb.a        libprotobuf-lite.a          libprotobuf.so.19      libprotoc.so.19.0.0  protoc
#engines-1.1       libglog.a              libleveldb.so       libprotobuf-lite.la         libprotobuf.so.19.0.0  libsnappy.a
#libcrypto.a       libglog.la             libleveldb.so.1     libprotobuf-lite.so         libprotoc.a            libssl.a
#libcrypto.so      libglog.so             libleveldb.so.1.20  libprotobuf-lite.so.19      libprotoc.la           libssl.so
#libcrypto.so.1.1  libglog.so.0           libprotobuf.a       libprotobuf-lite.so.19.0.0  libprotoc.so           libssl.so.1.1
#libgflags.a       libglog.so.0.0.0       libprotobuf.la      libprotobuf.so              libprotoc.so.19        pkgconfig
