// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "kvstore.h"

DEFINE_bool(allow_absent_key, false, "Cas succeeds if the key is absent while "
                                     " exptected value is exactly 0");
DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(map_capacity, 1024, "Initial capicity of value map");
DEFINE_int32(port, 8100, "Listen port of this peer");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(group, "KvStore", "Id of the replication group");

namespace example {

// Implements example::AtomicService if you are using brpc.
class KvStoreServiceImpl : public KvStoreService {
public:
    explicit KvStoreServiceImpl(KvStore* kvstore) : _kvstore(kvstore) {}
    ~KvStoreServiceImpl() {}

    void get(::google::protobuf::RpcController* controller,
             const ::example::GetRequest* request,
             ::example::GetResponse* response,
             ::google::protobuf::Closure* done) {
        return _kvstore->get(request, response, done);
    }
    void put(::google::protobuf::RpcController* controller,
                  const ::example::PutRequest* request,
                  ::example::PutResponse* response,
                  ::google::protobuf::Closure* done) {
        return _kvstore->put(request, response, done);
    }

private:
    KvStore* _kvstore;
};

}  // namespace example

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Generally you only need one Server.
    example::KvStore kvstore(FLAGS_port, FLAGS_group);
    kvstore.set_conf(FLAGS_conf);
    kvstore.set_election_timeout_ms(FLAGS_election_timeout_ms);
    kvstore.set_snapshot_interval_sec(FLAGS_snapshot_interval);
    kvstore.set_data_path(FLAGS_data_path);
    kvstore.set_disable_cli(FLAGS_disable_cli);
    kvstore.set_check_term(FLAGS_check_term);
    kvstore.start();

    brpc::Server server;
    example::KvStoreServiceImpl service(&kvstore);

    // Add your service into RPC rerver
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    // raft can share the same RPC server. Notice the second parameter, because
    // adding services into a running server is not allowed and the listen
    // address of this server is impossible to get before the server starts. You
    // have to specify the address of the server.
    if (braft::add_service(&server, FLAGS_port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // It's recommended to start the server before KvStore is started to avoid
    // the case that it becomes the leader while the service is unreacheable by
    // clients.
    // Notice the default options of server is used here. Check out details from
    // the doc of brpc if you would like change some options;
    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    // It's ok to start KvStore;
    if (kvstore.start() != 0) {
        LOG(ERROR) << "Fail to start KvStore";
        return -1;
    }

    LOG(INFO) << "KvStore service is running on " << server.listen_address();
    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    LOG(INFO) << "KvStore service is going to quit";

    // Stop counter before server
    kvstore.shutdown();
    server.Stop(0);

    // Wait until all the processing tasks are over.
    kvstore.join();
    server.Join();
    return 0;
}
