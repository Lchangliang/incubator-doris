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

#include "io/cache/file_cache_utils.h"

namespace doris::io {

class FileCacheManager;

class FileCacheStorage {
public:
    FileCacheStorage(FileCacheManager* file_cache_manager)
            : _file_cache_manager(file_cache_manager) {}
    virtual ~FileCacheStorage() = default;
    virtual Status init() = 0;
    virtual Status put(const Key& key, int64_t offset, const std::string_view& value,
                       const KeyMeta* = nullptr) = 0;
    virtual Status get(const std::string_view& file_path, int64_t offset, std::string* value) = 0;
    virtual Status remove(const std::string_view& file_path, int64_t offset) = 0;
    virtual Status change_key_meta(const KeyMeta& meta) = 0;

private:
    FileCacheManager* _file_cache_manager {nullptr};
};

} // namespace doris::io
