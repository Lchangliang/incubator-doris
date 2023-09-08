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

#include "io/cache/block/block_file_cache_settings.h"

#include "common/config.h"

namespace doris::io {

FileCacheSettings calc_settings(size_t total_size, size_t max_query_cache_size) {
    io::FileCacheSettings settings;
    settings.total_size = total_size;
    settings.max_file_block_size = config::file_cache_max_file_block_size;
    settings.max_query_cache_size = max_query_cache_size;
    size_t per_size = settings.total_size / io::percentage[3];
    settings.disposable_queue_size = per_size * io::percentage[1];
    settings.disposable_queue_elements =
            std::max(settings.disposable_queue_size / settings.max_file_block_size,
                     io::REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);

    settings.index_queue_size = per_size * io::percentage[2];
    settings.index_queue_elements =
            std::max(settings.index_queue_size / settings.max_file_block_size,
                     io::REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);

    settings.query_queue_size =
            settings.total_size - settings.disposable_queue_size - settings.index_queue_size;
    settings.query_queue_elements =
            std::max(settings.query_queue_size / settings.max_file_block_size,
                     io::REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);
    return settings;
}

} // namespace doris::io