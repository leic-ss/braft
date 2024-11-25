
#pragma once

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

namespace example {

class KvStore;
class KvStoreClosure : public braft::Closure
{
public:
    KvStoreClosure(KvStore* kvstore, const google::protobuf::Message* request,
                   google::protobuf::Message* response, google::protobuf::Closure* done)
        : _kvstore(kvstore), _request(request), _response(response), _done(done)
    {}

    void Run();

    const google::protobuf::Message* request() const { return _request; }
    google::protobuf::Message* response() const { return _response; }

private:
    KvStore* _kvstore;
    int _type;
    const google::protobuf::Message* _request;
    google::protobuf::Message* _response;
    google::protobuf::Closure* _done;
};


class KvStore : public braft::StateMachine
{
public:
    // Define types for different operation
    enum AtomicOpType {
        OP_UNKNOWN = 0,
        OP_GET = 1,
        OP_PUT = 2
    };

    KvStore(int32_t port, std::string group_id);
    ~KvStore();

    void set_conf(butil::StringPiece conf) { _conf = conf; }
    void set_election_timeout_ms(uint32_t timeout_ms) { _election_timeout_ms = timeout_ms; }
    void set_snapshot_interval_sec(uint32_t interval_sec) { _snapshot_interval_sec = interval_sec; }
    void set_data_path(const std::string& path) { _data_path = path; }
    void set_disable_cli(bool cli) { _disable_cli = cli; }
    void set_check_term(bool check_term) { _check_term = check_term; }

    // Starts this node
    int32_t start();

    void put(const ::example::PutRequest* request, ::example::PutResponse* response, ::google::protobuf::Closure* done);
    void get(const ::example::GetRequest* request, ::example::GetResponse* response, ::google::protobuf::Closure* done);

    bool is_leader() const;
    // Shut this node down.
    void shutdown();
    // Blocking this thread until the node is eventually down.
    void join();

protected:
    // @braft::StateMachine
    void on_apply(braft::Iterator& iter);

    void do_put(const butil::IOBuf& data, const google::protobuf::Message* request, PutResponse* response);
    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done);
    int32_t on_snapshot_load(braft::SnapshotReader* reader);

    void on_leader_start(int64_t term);
    void on_leader_stop(const butil::Status& status);
    void on_shutdown();

    void on_error(const ::braft::Error& e);
    void on_configuration_committed(const ::braft::Configuration& conf);
    void on_stop_following(const ::braft::LeaderChangeContext& ctx);
    void on_start_following(const ::braft::LeaderChangeContext& ctx);

    static void* save_snapshot(void* arg);

    typedef butil::FlatMap<std::string, std::string> ValueMap;

    struct SnapshotClosure {
        std::vector<std::pair<std::string, std::string> > values;
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };

private:
    braft::Node* volatile _node;
    butil::atomic<int64_t> _leader_term;
    ValueMap _value_map;

    uint32_t _listen_port{0};
    std::string _group_id;

    butil::StringPiece _conf;
    uint32_t _election_timeout_ms;
    uint32_t _snapshot_interval_sec;
    std::string _data_path;
    bool _disable_cli;
    bool _check_term;
};

}
