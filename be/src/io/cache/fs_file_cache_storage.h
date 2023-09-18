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

#include "io/cache/file_cache_storage.h"

namespace doris::io {

class FSFileCacheStorage : public FileCacheStorage {
public:
    FSFileCacheStorage(BlockFileCacheManager* file_cache_manager)
            : FileCacheStorage(file_cache_manager) {}
    ~FSFileCacheStorage() override = default;
    Status init() override;
    Status put(const Key& key, size_t offset, const Slice&, const KeyMeta&) override;
    Status get(const Key& key, size_t offset, const KeyMeta& key_meta, Slice value, size_t value_offset) override;
    Status remove(const Key& key, size_t offset) override;
    Status change_key_meta(const KeyMeta& meta) override;

private:
    [[nodiscard]] std::string get_path_in_local_cache(const std::string& dir, size_t offset,
                                                      FileCacheType type,
                                                      bool is_tmp = false) const;

    [[nodiscard]] std::string get_path_in_local_cache(const Key& key,
                                                      int64_t expiration_time) const;
};

} // namespace doris::io