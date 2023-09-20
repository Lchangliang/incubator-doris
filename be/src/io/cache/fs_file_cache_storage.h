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
    FSFileCacheStorage() = default;
    ~FSFileCacheStorage() override = default;
    Status init(BlockFileCacheManager* _mgr) override;
    Status put(const FileCacheKey& key, const Slice& value) override;
    Status get(const FileCacheKey& key, size_t value_offset, Slice buffer) override;
    Status remove(const FileCacheKey& key) override;
    Status change_key_meta(const FileCacheKey& key, const KeyMeta& new_meta) override;

private:
    [[nodiscard]] std::string get_path_in_local_cache(const std::string& dir, size_t offset,
                                                      FileCacheType type,
                                                      bool is_tmp = false) const;

    [[nodiscard]] std::string get_path_in_local_cache(const UInt128Wrapper& key,
                                                      int64_t expiration_time) const;

    std::string _cache_base_path;
};

} // namespace doris::io