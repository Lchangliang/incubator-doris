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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCacheFactory.cpp
// and modified by Doris

#include "io/cache/block/block_file_cache_factory.h"

#include <cstddef>

#include "common/config.h"
#include "io/cache/block/block_file_cache.h"
#include "io/cache/block/block_lru_file_cache.h"
#include "io/fs/local_file_system.h"

namespace doris {
namespace io {

FileCacheFactory& FileCacheFactory::instance() {
    static FileCacheFactory ret;
    return ret;
}

Status FileCacheFactory::create_file_cache(const std::string& cache_base_path,
                                           const FileCacheSettings& file_cache_settings) {
    if (config::clear_file_cache) {
        auto fs = global_local_filesystem();
        bool res = false;
        fs->exists(cache_base_path, &res);
        if (res) {
            fs->delete_directory(cache_base_path);
        }
    }

    std::unique_ptr<IFileCache> cache =
            std::make_unique<LRUFileCache>(cache_base_path, file_cache_settings);
    RETURN_IF_ERROR(cache->initialize());
    _caches.push_back(std::move(cache));
    LOG(INFO) << "[FileCache] path: " << cache_base_path
              << " total_size: " << file_cache_settings.total_size;
    return Status::OK();
}

CloudFileCachePtr FileCacheFactory::get_by_path(const IFileCache::Key& key) {
    return _caches[KeyHash()(key) % _caches.size()].get();
}

std::vector<IFileCache::QueryFileCacheContextHolderPtr> FileCacheFactory::get_query_context_holders(
        const TUniqueId& query_id) {
    std::vector<IFileCache::QueryFileCacheContextHolderPtr> holders;
    for (const auto& cache : _caches) {
        holders.push_back(cache->get_query_context_holder(query_id));
    }
    return holders;
}

} // namespace io
} // namespace doris
