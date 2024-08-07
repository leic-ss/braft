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

#include <fstream>
#include <bthread/bthread.h>
#include <gflags/gflags.h>
#include <butil/containers/flat_map.h>
#include <butil/logging.h>
#include <bthread/bthread.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/storage.h>

#include "kvstore.pb.h"

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

class KvStore;
// Implements Closure which encloses RPC stuff
class KvStoreClosure : public braft::Closure {
public:
    KvStoreClosure(KvStore* kvstore,
                  const google::protobuf::Message* request,
                  google::protobuf::Message* response,
                  google::protobuf::Closure* done)
        : _kvstore(kvstore)
        , _request(request)
        , _response(response)
        , _done(done)
    {}

    void Run() {
        std::unique_ptr<KvStoreClosure> self_guard(this);
        brpc::ClosureGuard done_guard(_done);
        if (status().ok()) {
            return;
        }
        // Try redirect if this request failed.
        // _kvstore->redirect(_response);
    }

    const google::protobuf::Message* request() const { return _request; }
    google::protobuf::Message* response() const { return _response; }

private:
    KvStore* _kvstore;
    int _type;
    const google::protobuf::Message* _request;
    google::protobuf::Message* _response;
    google::protobuf::Closure* _done;
};

// Implementation of example::Atomic as a braft::StateMachine.
class KvStore : public braft::StateMachine {
public:
    // Define types for different operation
    enum AtomicOpType {
        OP_UNKNOWN = 0,
        OP_GET = 1,
        OP_PUT = 2
    };

    KvStore()
        : _node(NULL)
        , _leader_term(-1)
    {
        CHECK_EQ(0, _value_map.init(FLAGS_map_capacity));
    }

    ~KvStore() {
        delete _node;
    }

    // Starts this node
    int start() {
        butil::EndPoint addr(butil::my_ip(), FLAGS_port);
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(FLAGS_conf) != 0) {
            LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
            return -1;
        }
        node_options.election_timeout_ms = FLAGS_election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = FLAGS_snapshot_interval;
        std::string prefix = "local://" + FLAGS_data_path;
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = FLAGS_disable_cli;
        braft::Node* node = new braft::Node(FLAGS_group, braft::PeerId(addr));
        if (node->init(node_options) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        _node = node;
        return 0;
    }

    void put(const ::example::PutRequest* request,
                  ::example::PutResponse* response,
                  ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        // Serialize request to the replicated write-ahead-log so that all the
        // peers in the group receive this request as well.
        // Notice that _value can't be modified in this routine otherwise it
        // will be inconsistent with others in this group.
        
        // Serialize request to IOBuf
        const int64_t term = _leader_term.load(butil::memory_order_relaxed);
        if (term < 0) {
            // return redirect(response);
        }
        butil::IOBuf log;
        log.push_back((uint8_t)OP_PUT);
        butil::IOBufAsZeroCopyOutputStream wrapper(&log);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Fail to serialize request";
            response->set_success(false);
            return;
        }
        // Apply this log as a braft::Task
        braft::Task task;
        task.data = &log;
        // This callback would be iovoked when the task actually excuted or
        // fail
        task.done = new KvStoreClosure(this, request, response,
                                        done_guard.release());
        if (FLAGS_check_term) {
            // ABA problem can be avoid if expected_term is set
            task.expected_term = term;
        }
        // Now the task is applied to the group, waiting for the result.
        return _node->apply(task);
    }

    void get(const ::example::GetRequest* request,
             ::example::GetResponse* response,
             ::google::protobuf::Closure* done)
    {
        brpc::ClosureGuard done_guard(done);
        if (!is_leader()) {
            // This node is a follower or it's not up-to-date. Redirect to
            // the leader if possible.
            // return redirect(response);
        }

        // This is the leader and is up-to-date. It's safe to respond client
        response->set_success(true);
        // response->set_value(_value.load(butil::memory_order_relaxed));
    }

    bool is_leader() const {
        return _leader_term.load(butil::memory_order_acquire) > 0;
    }

    // Shut this node down.
    void shutdown() {
        if (_node) {
            _node->shutdown(NULL);
        }
    }

    // Blocking this thread until the node is eventually down.
    void join() {
        if (_node) {
            _node->join();
        }
    }

    // void redirect(KvStoreResponse* response) {
    //     response->set_success(false);
    //     if (_node) {
    //         braft::PeerId leader = _node->leader_id();
    //         if (!leader.is_empty()) {
    //             response->set_redirect(leader.to_string());
    //         }
    //     }
    // }

private:
    // @braft::StateMachine
    void on_apply(braft::Iterator& iter) {
        // A batch of tasks are committed, which must be processed through 
        // |iter|
        for (; iter.valid(); iter.next()) {
            // This guard helps invoke iter.done()->Run() asynchronously to
            // avoid that callback blocks the StateMachine.
            braft::AsyncClosureGuard done_guard(iter.done());

            // Parse data
            butil::IOBuf data = iter.data();
            // Fetch the type of operation from the leading byte.
            uint8_t type = OP_UNKNOWN;
            data.cutn(&type, sizeof(uint8_t));

            KvStoreClosure* c = NULL;
            if (iter.done()) {
                c = dynamic_cast<KvStoreClosure*>(iter.done());
            }

            const google::protobuf::Message* request = c ? c->request() : NULL;
            PutResponse r;
            PutResponse* response = c ? dynamic_cast<PutResponse*>(c->response()) : &r;
            const char* op = NULL;
            // Execute the operation according to type
            switch (type) {
            case OP_PUT:
                op = "put";
                do_put(data, request, response);
                break;
            default:
                CHECK(false) << "Unknown type=" << static_cast<int>(type);
                break;
            }

            // The purpose of following logs is to help you understand the way
            // this StateMachine works.
            // Remove these logs in performance-sensitive servers.
            LOG_IF(INFO, FLAGS_log_applied_task) 
                    << "Handled operation " << op 
                    << " at log_index=" << iter.index()
                    << " success=" << response->success();
        }
    }

    void do_put(const butil::IOBuf& data,
                  const google::protobuf::Message* request,
                  PutResponse* response) {
        std::string key;
        std::string value;
        if (request) {
            // This task is applied by this node, get value from this
            // closure to avoid additional parsing.
            const PutRequest* req
                    = dynamic_cast<const PutRequest*>(request);
            key = req->key();
            value = req->value();
        } else {
            butil::IOBufAsZeroCopyInputStream wrapper(data);
            PutRequest req;
            CHECK(req.ParseFromZeroCopyStream(&wrapper));
            key = req.key();
            value = req.value();
        }
        
        LOG(INFO) << "do put, key = " << key << " value = " << value;
        _value_map[key] = value;

        response->set_success(true);
        response->set_rc(0);
    }

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {

        // Save current StateMachine in memory and starts a new bthread to avoid
        // blocking StateMachine since it's a bit slow to write data to disk
        // file.
        SnapshotClosure* sc = new SnapshotClosure;
        sc->values.reserve(_value_map.size());
        sc->writer = writer;
        sc->done = done;
        for (ValueMap::const_iterator 
                it = _value_map.begin(); it != _value_map.end(); ++it) {
            sc->values.push_back(std::make_pair(it->first, it->second));
        }

        bthread_t tid;
        bthread_start_urgent(&tid, NULL, save_snapshot, sc);
    }

    int on_snapshot_load(braft::SnapshotReader* reader) {
        CHECK_EQ(-1, _leader_term) << "Leader is not supposed to load snapshot";
        _value_map.clear();
        std::string snapshot_path = reader->get_path();
        snapshot_path.append("/data");
        std::ifstream is(snapshot_path.c_str());
        std::string key;
        std::string value;

        while (is >> key >> value) {
            _value_map[key] = value;
        }
        return 0;
    }

    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
    }

    void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void on_shutdown() {
        LOG(INFO) << "This node is down";
    }
    void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Met raft error " << e;
    }
    void on_configuration_committed(const ::braft::Configuration& conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }
    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }
    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node start following " << ctx;
    }

    static void* save_snapshot(void* arg) {
        SnapshotClosure* sc = (SnapshotClosure*)arg;
        std::unique_ptr<SnapshotClosure> sc_guard(sc);
        brpc::ClosureGuard done_guard(sc->done);
        std::string snapshot_path = sc->writer->get_path();
        snapshot_path.append("/data");
        std::ofstream os(snapshot_path.c_str());
        for (size_t i = 0; i < sc->values.size(); ++i) {
            os << sc->values[i].first << ' ' << sc->values[i].second << '\n';
        }
        CHECK_EQ(0, sc->writer->add_file("data"));
        return NULL;
    }

    typedef butil::FlatMap<std::string, std::string> ValueMap;

    struct SnapshotClosure {
        std::vector<std::pair<std::string, std::string> > values;
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };

    braft::Node* volatile _node;
    butil::atomic<int64_t> _leader_term;
    ValueMap _value_map;
};

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
    brpc::Server server;
    example::KvStore atomic;
    example::KvStoreServiceImpl service(&atomic);

    // Add your service into RPC rerver
    if (server.AddService(&service, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
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
    if (atomic.start() != 0) {
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
    atomic.shutdown();
    server.Stop(0);

    // Wait until all the processing tasks are over.
    atomic.join();
    server.Join();
    return 0;
}
