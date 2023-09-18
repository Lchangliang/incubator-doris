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

#include "io/cache/fs_file_cache_storage.h"

#include <system_error>

#include "io/cache/block_file_cache_manager.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/fs/local_file_writer.h"

namespace doris::io {

Status FSFileCacheStorage::init() {
    return Status::OK();
}

Status FSFileCacheStorage::put(const Key& key, size_t offset, const Slice& value,
                               const KeyMeta& key_meta) {
    std::string dir = get_path_in_local_cache(key, key_meta.expiration_time);
    bool exists {false};
    RETURN_IF_ERROR(global_local_filesystem()->exists(dir, &exists));
    if (!exists) {
        RETURN_IF_ERROR(global_local_filesystem()->create_directory(dir, false));
    }
    std::string tmp_file = get_path_in_local_cache(dir, offset, key_meta.file_cache_type, true);
    FileWriterPtr file_writer;
    RETURN_IF_ERROR(global_local_filesystem()->create_file(tmp_file, &file_writer));
    RETURN_IF_ERROR(file_writer->append(value));
    RETURN_IF_ERROR(file_writer->close());
    std::string true_file = get_path_in_local_cache(dir, offset, key_meta.file_cache_type, false);
    RETURN_IF_ERROR(global_local_filesystem()->rename(tmp_file, true_file));
    return Status::OK();
}

Status FSFileCacheStorage::get(const Key& key, size_t offset, const KeyMeta& key_meta, Slice value,
                               size_t value_offset) {
    std::string true_file =
            get_path_in_local_cache(get_path_in_local_cache(key, key_meta.expiration_time), offset,
                                    key_meta.file_cache_type, false);
    FileReaderSPtr file_reader;
    RETURN_IF_ERROR(global_local_filesystem()->open_file(true_file, &file_reader));
    size_t bytes_read = 0;
    RETURN_IF_ERROR(file_reader->read_at(value_offset, value, &bytes_read));
    DCHECK(bytes_read == value.get_size());
    return Status::OK();
}

Status FSFileCacheStorage::remove(const Key& key, size_t offset) {
    
    return Status::OK();
}

Status FSFileCacheStorage::change_key_meta(const KeyMeta& meta) {
    return Status::OK();
}

std::string FSFileCacheStorage::get_path_in_local_cache(const std::string& dir, size_t offset,
                                                        FileCacheType type, bool is_tmp) const {
    return dir + (std::to_string(offset) +
                  (is_tmp ? "_tmp" : BlockFileCacheManager::cache_type_to_string(type)));
}

std::string FSFileCacheStorage::get_path_in_local_cache(const Key& key,
                                                        int64_t expiration_time) const {
    auto key_str = key.to_string();
    try {
        if constexpr (BlockFileCacheManager::USE_CACHE_VERSION2) {
            return _mgr->_cache_base_path +
                   key_str.substr(0, BlockFileCacheManager::KEY_PREFIX_LENGTH) +
                   (key_str + "_" + std::to_string(expiration_time));
        } else {
            return _mgr->_cache_base_path + (key_str + "_" + std::to_string(expiration_time));
        }
    } catch (std::filesystem::filesystem_error& e) {
        LOG(WARNING) << "fail to get_path_in_local_cache=" << e.what();
        return "";
    }
}

} // namespace doris::io