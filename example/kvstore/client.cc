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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#include <map>                  // std::map
#include <gflags/gflags.h>      // google::ParseCommandLineFlags
#include <butil/string_printf.h>
#include <braft/cli.h>          // raft::cli::*

#include <brpc/channel.h>          // brpc::Channel
#include <brpc/controller.h>       // brpc::Controller
#include "braft/cli.pb.h"                // CliService_Stub
#include "braft/util.h"

#include "kvstore.pb.h"

namespace braft {
namespace cli {

DEFINE_int32(timeout_ms, -1, "Timeout (in milliseconds) of the operation");
DEFINE_int32(max_retry, 3, "Max retry times of each operation");
DEFINE_string(conf, "", "Current configuration of the raft group");
DEFINE_string(peer, "", "Id of the operating peer");
DEFINE_string(new_peers, "", "Peers that the group is going to consists of");
DEFINE_string(group, "", "Id of the raft group");
DEFINE_string(key, "", "key of kvstore");
DEFINE_string(value, "", "value of kvstore");

#define CHECK_FLAG(flagname)                                            \
    do {                                                                \
        if ((FLAGS_ ## flagname).empty()) {                             \
            LOG(ERROR) << __FUNCTION__ << " requires --" # flagname ;   \
            return -1;                                                  \
        }                                                               \
    } while (0);                                                        \

static butil::Status get_leader(const GroupId& group_id, const Configuration& conf,
                        PeerId* leader_id) {
    if (conf.empty()) {
        return butil::Status(EINVAL, "Empty group configuration");
    }
    // Construct a brpc naming service to access all the nodes in this group
    butil::Status st(-1, "Fail to get leader of group %s", group_id.c_str());
    leader_id->reset();
    for (Configuration::const_iterator
            iter = conf.begin(); iter != conf.end(); ++iter) {
        brpc::Channel channel;
        if (channel.Init(iter->addr, NULL) != 0) {
            return butil::Status(-1, "Fail to init channel to %s",
                                     iter->to_string().c_str());
        }
        CliService_Stub stub(&channel);
        GetLeaderRequest request;
        GetLeaderResponse response;
        brpc::Controller cntl;
        request.set_group_id(group_id);
        request.set_peer_id(iter->to_string());
        stub.get_leader(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            if (st.ok()) {
                st.set_error(cntl.ErrorCode(), "[%s] %s",
                            butil::endpoint2str(cntl.remote_side()).c_str(),
                            cntl.ErrorText().c_str());
            } else {
                std::string saved_et = st.error_str();
                st.set_error(cntl.ErrorCode(), "%s, [%s] %s",  saved_et.c_str(),
                            butil::endpoint2str(cntl.remote_side()).c_str(),
                            cntl.ErrorText().c_str());
                }
            continue;
        }
        leader_id->parse(response.leader_id());
    }
    if (leader_id->is_empty()) {
        return st;
    }
    return butil::Status::OK();
}

int add_peer()
{
    CHECK_FLAG(conf);
    CHECK_FLAG(peer);
    CHECK_FLAG(group);
    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf << '\'';
        return -1;
    }
    PeerId new_peer;
    if (new_peer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer<< '\'';
        return -1;
    }
    CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = add_peer(FLAGS_group, conf, new_peer, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to add_peer : " << st;
        return -1;
    }
    return 0;
}

int do_put()
{
    CHECK_FLAG(conf);
    CHECK_FLAG(key);
    CHECK_FLAG(value);
    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf << '\'';
        return -1;
    }

    PeerId leader_id;
    butil::Status st = get_leader(FLAGS_group, conf, &leader_id);
    if (!st.ok()) {
        LOG(ERROR) << st.error_str();
        return -1;
    }
    // BRAFT_RETURN_IF(!st.ok(), st);
    brpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to " << leader_id.to_string().c_str();
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str()).error_code();
    }

    example::PutRequest request;
    request.set_key( FLAGS_key );
    request.set_value( FLAGS_value );
    example::PutResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);
    cntl.set_max_retry(FLAGS_max_retry);

    example::KvStoreService_Stub stub(&channel);
    stub.put(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText()).error_code();
    }

    LOG(INFO) << "do put key=" << FLAGS_key
              << " value=" << FLAGS_value
              << " rc=" << response.rc();
    return butil::Status::OK().error_code();
}

int do_get()
{
    CHECK_FLAG(conf);
    CHECK_FLAG(key);

    return 0;
}

int remove_peer()
{
    CHECK_FLAG(conf);
    CHECK_FLAG(peer);
    CHECK_FLAG(group);
    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf << '\'';
        return -1;
    }
    PeerId removing_peer;
    if (removing_peer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer<< '\'';
        return -1;
    }
    CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = remove_peer(FLAGS_group, conf, removing_peer, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to remove_peer : " << st;
        return -1;
    }
    return 0;
}

int change_peers()
{
    CHECK_FLAG(new_peers);
    CHECK_FLAG(conf);
    CHECK_FLAG(group);
    Configuration new_peers;
    if (new_peers.parse_from(FLAGS_new_peers) != 0) {
        LOG(ERROR) << "Fail to parse --new_peers=`" << FLAGS_new_peers << '\'';
        return -1;
    }
    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf<< '\'';
        return -1;
    }
    CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = change_peers(FLAGS_group, conf, new_peers, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to change_peers : " << st;
        return -1;
    }
    return 0;
}

int reset_peer()
{
    CHECK_FLAG(new_peers);
    CHECK_FLAG(peer);
    CHECK_FLAG(group);
    Configuration new_peers;
    if (new_peers.parse_from(FLAGS_new_peers) != 0) {
        LOG(ERROR) << "Fail to parse --new_peers=`" << FLAGS_new_peers << '\'';
        return -1;
    }
    PeerId target_peer;
    if (target_peer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer<< '\'';
        return -1;
    }
    CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = reset_peer(FLAGS_group, target_peer, new_peers, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to reset_peer : " << st;
        return -1;
    }
    return 0;
}

int get_cluster()
{
    CHECK_FLAG(conf);
    CHECK_FLAG(group);
    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf << '\'';
        return -1;
    }

    PeerId leader_id;
    butil::Status st = get_leader(FLAGS_group, conf, &leader_id);
    if (!st.ok()) {
        LOG(ERROR) << st.error_str();
        return -1;
    }
    // BRAFT_RETURN_IF(!st.ok(), st);
    brpc::Channel channel;
    if (channel.Init(leader_id.addr, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to " << leader_id.to_string().c_str();
        return butil::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str()).error_code();
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);
    cntl.set_max_retry(FLAGS_max_retry);
    GetClusterRequest request;
    request.set_group_id(FLAGS_group);
    request.set_leader_id(leader_id.to_string());

    GetClusterResponse response;
    CliService_Stub stub(&channel);
    stub.get_cluster(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText()).error_code();
    }

    LOG(INFO) << "\n" << response.DebugString();
    LOG(INFO) << "\n" << "leader: " << leader_id.to_string();
    return 0;
}

int snapshot()
{
    CHECK_FLAG(peer);
    CHECK_FLAG(group);
    PeerId target_peer;
    if (target_peer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer<< '\'';
        return -1;
    }
    CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = snapshot(FLAGS_group, target_peer, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to make snapshot : " << st;
        return -1;
    }
    return 0;
}

int transfer_leader()
{
    CHECK_FLAG(conf);
    CHECK_FLAG(peer);
    CHECK_FLAG(group);
    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf << '\'';
        return -1;
    }
    PeerId target_peer;
    if (target_peer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer<< '\'';
        return -1;
    }
    CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = transfer_leader(FLAGS_group, conf, target_peer, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to transfer_leader: " << st;
        return -1;
    }
    return 0;
}

int run_command(const std::string& cmd)
{
    if (cmd == "add_peer") {
        return add_peer();
    }
    if (cmd == "remove_peer") {
        return remove_peer();
    }
    if (cmd == "change_peers") {
        return change_peers();
    }
    if (cmd == "reset_peer") {
        return reset_peer();
    }
    if (cmd == "snapshot") {
        return snapshot();
    }
    if (cmd == "transfer_leader") {
        return transfer_leader();
    }
    if (cmd == "getcluster") {
        return get_cluster();
    }
    if (cmd == "put") {
        return do_put();
    }
    if (cmd == "get") {
        return do_get();
    }

    LOG(ERROR) << "Unknown command `" << cmd << '\'';
    return -1;
}

}  // namespace cli
}  // namespace raft

int main(int argc , char* argv[])
{
    const char* proc_name = strrchr(argv[0], '/');
    if (proc_name == NULL) {
        proc_name = argv[0];
    } else {
        ++proc_name;
    }
    std::string help_str;
    butil::string_printf(&help_str,
                        "Usage: %s [Command] [OPTIONS...]\n"
                        "Command:\n"
                        "  add_peer --group=$group_id "
                                    "--peer=$adding_peer --conf=$current_conf\n"
                        "  remove_peer --group=$group_id "
                                      "--peer=$removing_peer --conf=$current_conf\n"
                        "  change_peers --group=$group_id "
                                       "--conf=$current_conf --new_peers=$new_peers\n"
                        "  reset_peer --group=$group_id "
                                     "--peer==$target_peer --new_peers=$new_peers\n"
                        "  snapshot --group=$group_id --peer=$target_peer\n"
                        "  transfer_leader --group=$group_id --peer=$target_leader --conf=$current_conf\n"
                        "  getcluster   --group=$group_id --conf=$current_conf\n"
                        "  put     --group=$group_id --key=$key --value=$value --conf=$current_conf\n"
                        "  get     --group=$group_id --key=$key --conf=$current_conf\n",
                        proc_name);
    GFLAGS_NS::SetUsageMessage(help_str);
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 2) {
        std::cerr << help_str;
        return -1;
    }
    return braft::cli::run_command(argv[1]);
}
