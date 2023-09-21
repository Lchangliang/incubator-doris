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

#pragma once
#include "io/io_common.h"
#include "vec/common/uint128.h"

namespace doris::io {

inline static constexpr size_t REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS = 100 * 1024;

using uint128_t = vectorized::UInt128;
using UInt128Hash = vectorized::UInt128Hash;

// default 1 : 17 : 2
enum FileCacheType {
    INDEX,
    NORMAL,
    DISPOSABLE,
    TTL,
};

struct UInt128Wrapper {
    uint128_t value_;
    [[nodiscard]] std::string to_string() const;

    UInt128Wrapper() = default;
    explicit UInt128Wrapper(const uint128_t& value) : value_(value) {}

    bool operator==(const UInt128Wrapper& other) const { return value_ == other.value_; }
};

struct KeyHash {
    std::size_t operator()(const UInt128Wrapper& w) const { return UInt128Hash()(w.value_); }
};

using AccessKeyAndOffset = std::pair<UInt128Wrapper, size_t>;
struct KeyAndOffsetHash {
    std::size_t operator()(const AccessKeyAndOffset& key) const {
        return UInt128Hash()(key.first.value_) ^ std::hash<uint64_t>()(key.second);
    }
};

struct KeyMeta {
    int64_t expiration_time; // 绝对时间
    FileCacheType type;
};

struct FileCacheKey {
    UInt128Wrapper hash;
    size_t offset;
    KeyMeta meta;
};

// [query:disposable:index:total]
constexpr std::array<size_t, 4> percentage {17, 2, 1, 20};
static_assert(percentage[0] + percentage[1] + percentage[2] == percentage[3]);
struct FileCacheSettings {
    size_t total_size {0};
    size_t disposable_queue_size {0};
    size_t disposable_queue_elements {0};
    size_t index_queue_size {0};
    size_t index_queue_elements {0};
    size_t query_queue_size {0};
    size_t query_queue_elements {0};
    size_t max_file_block_size {0};
    size_t max_query_cache_size {0};
};

FileCacheSettings calc_settings(size_t total_size, size_t max_query_cache_size);

struct CacheContext {
    CacheContext(const IOContext* io_context) {
        if (io_context->read_segment_index) {
            cache_type = FileCacheType::INDEX;
        } else if (io_context->is_disposable) {
            cache_type = FileCacheType::DISPOSABLE;
        } else if (io_context->expiration_time != 0) {
            cache_type = FileCacheType::TTL;
            expiration_time = io_context->expiration_time;
        } else {
            cache_type = FileCacheType::NORMAL;
        }
        query_id = io_context->query_id ? *io_context->query_id : TUniqueId();
    }
    CacheContext() = default;
    TUniqueId query_id;
    FileCacheType cache_type;
    int64_t expiration_time {0};
    bool is_cold_data {false};
};

} // namespace doris::io