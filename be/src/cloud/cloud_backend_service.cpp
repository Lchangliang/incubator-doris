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

#include "cloud/cloud_backend_service.h"

#include "cloud/cloud_tablet_hotspot.h"
#include "cloud/cloud_warm_up_manager.h"
#include "common/config.h"
#include "util/thrift_server.h"

namespace doris {

CloudBackendService::CloudBackendService(CloudStorageEngine& engine, ExecEnv* exec_env)
        : BaseBackendService(exec_env), _engine(engine) {}

CloudBackendService::~CloudBackendService() = default;

Status CloudBackendService::create_service(CloudStorageEngine& engine, ExecEnv* exec_env, int port,
                                           std::unique_ptr<ThriftServer>* server) {
    auto service = std::make_shared<CloudBackendService>(engine, exec_env);
    service->_agent_server->cloud_start_workers(engine, exec_env);
    // TODO: do we want a BoostThreadFactory?
    // TODO: we want separate thread factories here, so that fe requests can't starve
    // be requests
    // std::shared_ptr<TProcessor> be_processor = std::make_shared<BackendServiceProcessor>(service);
    auto be_processor = std::make_shared<BackendServiceProcessor>(service);

    *server = std::make_unique<ThriftServer>("backend", be_processor, port,
                                             config::be_service_threads);

    LOG(INFO) << "Doris CloudBackendService listening on " << port;

    return Status::OK();
}

void CloudBackendService::get_top_n_hot_partitions(TGetTopNHotPartitionsResponse& response,
                                                   const TGetTopNHotPartitionsRequest& request) {
    TabletHotspot::instance()->get_top_n_hot_partition(&response.hot_tables);
    //     response.file_cache_size = io::FileCacheFactory::instance().get_total_cache_size();
    response.__isset.hot_tables = !response.hot_tables.empty();
}

void CloudBackendService::warm_up_tablets(TWarmUpTabletsResponse& response,
                                          const TWarmUpTabletsRequest& request) {
    Status st;
    auto* manager = CloudWarmUpManager::instance();
    switch (request.type) {
    case TWarmUpTabletsRequestType::SET_JOB: {
        LOG_INFO("receive the warm up request.")
                .tag("request_type", "SET_JOB")
                .tag("job_id", request.job_id);
        st = manager->check_and_set_job_id(request.job_id);
        if (!st) {
            LOG_WARNING("SET_JOB failed.").error(st);
            break;
        }
        [[fallthrough]];
    }
    case TWarmUpTabletsRequestType::SET_BATCH: {
        LOG_INFO("receive the warm up request.")
                .tag("request_type", "SET_BATCH")
                .tag("job_id", request.job_id)
                .tag("batch_id", request.batch_id)
                .tag("jobs size", request.job_metas.size());
        bool retry = false;
        st = manager->check_and_set_batch_id(request.job_id, request.batch_id, &retry);
        if (!retry && st) {
            manager->add_job(request.job_metas);
        } else {
            if (retry) {
                LOG_WARNING("retry the job.")
                        .tag("job_id", request.job_id)
                        .tag("batch_id", request.batch_id);
            } else {
                LOG_WARNING("SET_BATCH failed.").error(st);
            }
        }
        break;
    }
    case TWarmUpTabletsRequestType::GET_CURRENT_JOB_STATE_AND_LEASE: {
        LOG_INFO("receive the warm up request.")
                .tag("request_type", "GET_CURRENT_JOB_STATE_AND_LEASE");
        auto [job_id, batch_id, pending_job_size, finish_job_size] =
                manager->get_current_job_state();
        response.__set_job_id(job_id);
        response.__set_batch_id(batch_id);
        response.__set_pending_job_size(pending_job_size);
        response.__set_finish_job_size(finish_job_size);
        break;
    }
    case TWarmUpTabletsRequestType::CLEAR_JOB: {
        LOG_INFO("receive the warm up request.")
                .tag("request_type", "CLEAR_JOB")
                .tag("job_id", request.job_id);
        st = manager->clear_job(request.job_id);
        break;
    }
    default:
        DCHECK(false);
    };
    st.to_thrift(&response.status);
}

} // namespace doris
