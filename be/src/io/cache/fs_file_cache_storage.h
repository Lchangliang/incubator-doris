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
#include "io/cache/file_cache_utils.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "util/lock.h"

namespace doris::io {

class FDCache {
public:
    static FDCache* instance() {
        static FDCache fd_cache;
        return &fd_cache;
    }

    void set_read_only(bool read_only) {
        std::lock_guard wlock(_mtx);
        _read_only = read_only;
        if (read_only) {
            _file_reader_list.clear();
            _file_name_to_reader.clear();
        }
    };

    bool is_read_only() const {
        std::shared_lock rlock(_mtx);
        return _read_only;
    }

    std::shared_ptr<FileReader> get_file_reader(const AccessKeyAndOffset& key);

    void insert_file_reader(const AccessKeyAndOffset& key, std::shared_ptr<FileReader> file_reader);

    void remove_file_reader(const AccessKeyAndOffset& key);

    // use for test
    bool contains_file_reader(const AccessKeyAndOffset& key);
    size_t file_reader_cache_size();

private:
    std::list<std::pair<AccessKeyAndOffset, std::shared_ptr<FileReader>>> _file_reader_list;
    std::unordered_map<AccessKeyAndOffset, decltype(_file_reader_list.begin()), KeyAndOffsetHash>
            _file_name_to_reader;
    mutable doris::SharedMutex _mtx;
    bool _read_only {false};
};

class FSFileCacheStorage : public FileCacheStorage {
public:
    /// use version 2 when USE_CACHE_VERSION2 = true, while use version 1 if false
    /// version 1.0: cache_base_path / key / offset
    /// version 2.0: cache_base_path / key_prefix / key / offset
    static constexpr bool USE_CACHE_VERSION2 = true;
    static constexpr int KEY_PREFIX_LENGTH = 3;

    FSFileCacheStorage() = default;
    ~FSFileCacheStorage() override;
    [[nodiscard]] Status init(BlockFileCacheManager* _mgr) override;
    [[nodiscard]] Status put(const FileCacheKey& key, const Slice& value) override;
    [[nodiscard]] Status get(const FileCacheKey& key, size_t value_offset, Slice buffer) override;
    [[nodiscard]] Status remove(const FileCacheKey& key) override;
    [[nodiscard]] Status change_key_meta(const FileCacheKey& key, const KeyMeta& new_meta) override;
    // use when lazy load cache
    void load_blocks_directly_unlocked(BlockFileCacheManager* _mgr, const FileCacheKey& key,
                                       std::lock_guard<doris::Mutex>& cache_lock) override;

private:
    [[nodiscard]] std::string get_path_in_local_cache(const std::string& dir, size_t offset,
                                                      FileCacheType type,
                                                      bool is_tmp = false) const;

    [[nodiscard]] std::string get_path_in_local_cache(const UInt128Wrapper&,
                                                      int64_t expiration_time) const;

    [[nodiscard]] Status rebuild_data_structure() const;

    [[nodiscard]] Status read_file_cache_version(std::string* buffer) const;

    [[nodiscard]] Status write_file_cache_version() const;

    [[nodiscard]] std::string get_version_path() const;

    void load_cache_info_into_memory(BlockFileCacheManager* _mgr) const;

    std::string _cache_base_path;
    std::thread _cache_background_load_thread;
    const std::shared_ptr<LocalFileSystem>& fs = global_local_filesystem();

    struct BatchLoadArgs {
        UInt128Wrapper hash;
        CacheContext ctx;
        uint64_t offset;
        size_t size;
        std::string key_path;
        std::string offset_path;
        bool is_tmp;
    };
};

} // namespace doris::io