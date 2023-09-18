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
#include "util/slice.h"

namespace doris::io {

class BlockFileCacheManager;

class FileCacheStorage {
public:
    FileCacheStorage(BlockFileCacheManager* file_cache_manager) : _mgr(file_cache_manager) {}
    virtual ~FileCacheStorage() = default;
    virtual Status init() = 0;
    virtual Status put(const Key& key, size_t offset, const Slice& value, const KeyMeta&) = 0;
    virtual Status get(const Key& key, size_t offset, const KeyMeta& key_meta, Slice value,
                       size_t value_offset) = 0;
    virtual Status remove(const Key& key, size_t offset) = 0;
    virtual Status change_key_meta(const KeyMeta& meta) = 0;

protected:
    BlockFileCacheManager* _mgr {nullptr};
};

} // namespace doris::io
