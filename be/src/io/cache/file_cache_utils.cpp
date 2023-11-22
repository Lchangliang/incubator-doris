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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCache_fwd.h
// and modified by Doris

#include "io/cache/file_cache_utils.h"

#include "vec/common/hex.h"

namespace doris::io {

FileCacheSettings calc_settings(size_t total_size, size_t max_query_cache_size) {
    FileCacheSettings settings;
    settings.total_size = total_size;
    settings.max_file_block_size = FILE_CACHE_MAX_FILE_BLOCK_SIZE;
    settings.max_query_cache_size = max_query_cache_size;
    size_t per_size = settings.total_size / percentage[3];
    settings.disposable_queue_size = per_size * percentage[1];
    settings.disposable_queue_elements =
            std::max(settings.disposable_queue_size / settings.max_file_block_size,
                     REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);

    settings.index_queue_size = per_size * percentage[2];
    settings.index_queue_elements =
            std::max(settings.index_queue_size / settings.max_file_block_size,
                     REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);

    settings.query_queue_size =
            settings.total_size - settings.disposable_queue_size - settings.index_queue_size;
    settings.query_queue_elements =
            std::max(settings.query_queue_size / settings.max_file_block_size,
                     REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);
    return settings;
}

std::string UInt128Wrapper::to_string() const {
    return vectorized::get_hex_uint_lowercase(value_);
}

} // namespace doris::io