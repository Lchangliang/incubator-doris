// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "gen_cpp/BackendService.h"

namespace doris {

enum class DownloadType {
    BE,
    S3,
};

struct JobMeta {
    JobMeta(const TJobMeta& meta);
    DownloadType download_type;
    std::string be_ip;
    int32_t brpc_port;
    std::vector<int64_t> tablet_ids;
};

class CloudWarmUpManager {
public:
    static CloudWarmUpManager* instance();

    CloudWarmUpManager();
    ~CloudWarmUpManager();

    Status check_and_set_job_id(int64_t job_id);

    Status check_and_set_batch_id(int64_t job_id, int64_t batch_id, bool* retry = nullptr);

    void add_job(const std::vector<TJobMeta>& job_metas);

#ifdef BE_TEST
    void consumer_job();
#endif

    std::tuple<int64_t, int64_t, int64_t, int64_t> get_current_job_state();

    Status clear_job(int64_t job_id);

private:
    std::mutex _mtx;
    std::condition_variable _cond;
    int64_t _cur_job_id {0};
    int64_t _cur_batch_id {-1};
    std::deque<JobMeta> _pending_job_metas;
    std::vector<JobMeta> _finish_job;
    std::thread _download_thread;
    bool _closed {false};
    void handle_jobs();
};

} // namespace doris
