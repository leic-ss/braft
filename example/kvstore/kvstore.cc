#include "kvstore.h"

namespace example {

void KvStoreClosure::Run()
{
    std::unique_ptr<KvStoreClosure> self_guard(this);
    brpc::ClosureGuard done_guard(_done);
    if (status().ok()) {
        return;
    }

    // Try redirect if this request failed.
    // _kvstore->redirect(_response);
    return ;
}

KvStore::KvStore(int32_t port, std::string group_id): _node(nullptr), _leader_term(-1)
{
    CHECK_EQ(0, _value_map.init(1000000));
    _listen_port = port;
    _group_id = group_id;
}

KvStore::~KvStore()
{
    delete _node;
}

int32_t KvStore::start() 
{
    butil::EndPoint addr(butil::my_ip(), _listen_port);
    braft::NodeOptions node_options;
    if (node_options.initial_conf.parse_from(_conf) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << _conf << '\'';
        return -1;
    }
    node_options.election_timeout_ms = _election_timeout_ms;
    node_options.fsm = this;
    node_options.node_owns_fsm = false;
    node_options.snapshot_interval_s = _snapshot_interval_sec;
    std::string prefix = "local://" + _data_path;
    node_options.log_uri = prefix + "/log";
    node_options.raft_meta_uri = prefix + "/raft_meta";
    node_options.snapshot_uri = prefix + "/snapshot";
    node_options.disable_cli = _disable_cli;
    braft::Node* node = new braft::Node(_group_id, braft::PeerId(addr));
    if (node->init(node_options) != 0) {
        LOG(ERROR) << "Fail to init raft node";
        delete node;
        return -1;
    }
    _node = node;
    return 0;
}

void KvStore::put(const ::example::PutRequest* request, ::example::PutResponse* response, ::google::protobuf::Closure* done)
{
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
    task.done = new KvStoreClosure(this, request, response, done_guard.release());
    if (_check_term) {
        // ABA problem can be avoid if expected_term is set
        task.expected_term = term;
    }
    // Now the task is applied to the group, waiting for the result.
    return _node->apply(task);
}

void KvStore::get(const ::example::GetRequest* request, ::example::GetResponse* response, ::google::protobuf::Closure* done)
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

bool KvStore::is_leader() const
{
    return _leader_term.load(butil::memory_order_acquire) > 0;
}

// Shut this node down.
void KvStore::shutdown()
{
    if (_node) {
        _node->shutdown(NULL);
    }
}

void KvStore::join()
{
    if (_node) {
        _node->join();
    }
}

void KvStore::on_apply(braft::Iterator& iter)
{
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

        KvStoreClosure* c = nullptr;
        if (iter.done()) {
            c = dynamic_cast<KvStoreClosure*>(iter.done());
        }

        const google::protobuf::Message* request = c ? c->request() : nullptr;
        PutResponse r;
        PutResponse* response = c ? dynamic_cast<PutResponse*>(c->response()) : &r;
        const char* op = "unknow";
        // Execute the operation according to type
        switch (type)
        {
            case OP_PUT:
                op = "put";
                do_put(data, request, response);
                break;
            default:
                CHECK(false) << "Unknown type=" << static_cast<int>(type);
                break;
        }

        // LOG_IF(INFO, FLAGS_log_applied_task) << "Handled operation " << op << " at log_index="
        //         << iter.index() << " success=" << response->success();
    }

    return ;
}

void KvStore::do_put(const butil::IOBuf& data, const google::protobuf::Message* request, PutResponse* response)
{
    std::string key;
    std::string value;
    if (request) {
        // This task is applied by this node, get value from this
        // closure to avoid additional parsing.
        const PutRequest* req = dynamic_cast<const PutRequest*>(request);
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

void KvStore::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done)
{
    // Save current StateMachine in memory and starts a new bthread to avoid
    // blocking StateMachine since it's a bit slow to write data to disk
    // file.
    SnapshotClosure* sc = new SnapshotClosure;
    sc->values.reserve(_value_map.size());
    sc->writer = writer;
    sc->done = done;
    for (ValueMap::const_iterator it = _value_map.begin(); it != _value_map.end(); ++it) {
        sc->values.push_back(std::make_pair(it->first, it->second));
    }

    bthread_t tid;
    bthread_start_urgent(&tid, NULL, save_snapshot, sc);
}

int32_t KvStore::on_snapshot_load(braft::SnapshotReader* reader)
{
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

void KvStore::on_leader_start(int64_t term)
{
    _leader_term.store(term, butil::memory_order_release);
    LOG(INFO) << "Node becomes leader";
}

void KvStore::on_leader_stop(const butil::Status& status)
{
    _leader_term.store(-1, butil::memory_order_release);
    LOG(INFO) << "Node stepped down : " << status;
}

void KvStore::on_shutdown()
{
    LOG(INFO) << "This node is down";
}

void KvStore::on_error(const ::braft::Error& e)
{
    LOG(ERROR) << "Met raft error " << e;
}

void KvStore::on_configuration_committed(const ::braft::Configuration& conf)
{
    LOG(INFO) << "Configuration of this group is " << conf;
}

void KvStore::on_stop_following(const ::braft::LeaderChangeContext& ctx)
{
    LOG(INFO) << "Node stops following " << ctx;
}

void KvStore::on_start_following(const ::braft::LeaderChangeContext& ctx)
{
    LOG(INFO) << "Node start following " << ctx;
}

void* KvStore::save_snapshot(void* arg)
{
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

}
