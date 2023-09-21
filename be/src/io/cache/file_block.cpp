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

#include "io/cache/file_block.h"

#include <glog/logging.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <sstream>
#include <string>
#include <thread>

#include "common/status.h"
#include "io/cache/block_file_cache_manager.h"

namespace doris {
namespace io {

FileBlock::FileBlock(const FileCacheKey& key, size_t size, BlockFileCacheManager* mgr,
                     State download_state)
        : _block_range(key.offset, key.offset + size - 1),
          _download_state(download_state),
          _mgr(mgr),
          _key(key) {
    /// On creation, file block state can be EMPTY, DOWNLOADED, SKIP_CACHE.
    switch (_download_state) {
    case State::DOWNLOADING: {
        DCHECK(false) << "Can create cell with either EMPTY, DOWNLOADED, SKIP_CACHE ";
        break;
    }
    default: {
        break;
    }
    }
}

FileBlock::State FileBlock::state() const {
    std::lock_guard block_lock(_mutex);
    return _download_state;
}

uint64_t FileBlock::get_caller_id() {
    uint64_t id = 0;
    id = bthread_self() == 0 ? static_cast<uint64_t>(pthread_self()) : bthread_self();
    DCHECK(id != 0);
    return id;
}

uint64_t FileBlock::get_or_set_downloader() {
    std::lock_guard block_lock(_mutex);

    if (_downloader_id == 0) {
        DCHECK(_download_state != State::DOWNLOADING);
        _downloader_id = get_caller_id();
        _download_state = State::DOWNLOADING;
    } else if (_downloader_id == get_caller_id()) {
        LOG(INFO) << "Attempt to set the same downloader for block " << range().to_string()
                  << " for the second time";
    }

    return _downloader_id;
}

void FileBlock::reset_downloader(std::lock_guard<std::mutex>& block_lock) {
    DCHECK(_downloader_id != 0) << "There is no downloader";

    DCHECK(get_caller_id() == _downloader_id) << "Downloader can be reset only by downloader";

    reset_downloader_impl(block_lock);
}

void FileBlock::reset_downloader_impl(std::lock_guard<std::mutex>& block_lock) {
    _download_state = State::EMPTY;
    _downloader_id = 0;
}

uint64_t FileBlock::get_downloader() const {
    std::lock_guard block_lock(_mutex);
    return _downloader_id;
}

bool FileBlock::is_downloader() const {
    std::lock_guard block_lock(_mutex);
    return get_caller_id() == _downloader_id;
}

bool FileBlock::is_downloader_impl(std::lock_guard<std::mutex>& /* block_lock */) const {
    return get_caller_id() == _downloader_id;
}

Status FileBlock::put(Slice data) {
    DCHECK(data.size != 0) << "Writing zero size is not allowed";
    RETURN_IF_ERROR(_mgr->_storage->put(_key, data));
    std::lock_guard lock(_mutex);
    if (data.size != _block_range.size()) {
        std::lock_guard cache_lock(_mgr->_mutex);
        size_t old_size = _block_range.size();
        _block_range.right = _block_range.left + data.size - 1;
        size_t new_size = _block_range.size();
        DCHECK(new_size < old_size);
        _mgr->reset_range(_key.hash, _block_range.left, old_size, new_size, cache_lock);
    }
    std::lock_guard block_lock(_mutex);
    _download_state = State::DOWNLOADED;
    _downloader_id = 0;
    _cv.notify_all();
    return Status::OK();
}

Status FileBlock::read_at(Slice buffer, size_t read_offset) {
    return _mgr->_storage->get(_key, read_offset, buffer);
}

Status FileBlock::change_cache_type_by_mgr(FileCacheType new_type) {
    std::lock_guard block_lock(_mutex);
    if (new_type == _key.meta.type) {
        return Status::OK();
    }
    if (_download_state == State::DOWNLOADED) {
        KeyMeta new_meta;
        new_meta.expiration_time = _key.meta.expiration_time;
        new_meta.type = new_type;
        RETURN_IF_ERROR(_mgr->_storage->change_key_meta(_key, new_meta));
    }
    _key.meta.type = new_type;
    return Status::OK();
}

Status FileBlock::change_cache_type_self(FileCacheType new_type) {
    std::lock_guard cache_lock(_mgr->_mutex);
    std::lock_guard block_lock(_mutex);
    if (_key.meta.type == FileCacheType::TTL || new_type == _key.meta.type) {
        return Status::OK();
    }
    KeyMeta new_meta;
    new_meta.expiration_time = _key.meta.expiration_time;
    new_meta.type = new_type;
    RETURN_IF_ERROR(_mgr->_storage->change_key_meta(_key, new_meta));
    _key.meta.type = new_type;
    _mgr->change_cache_type(_key.hash, _block_range.left, new_type, cache_lock);
    return Status::OK();
}

Status FileBlock::update_expiration_time(int64_t expiration_time) {
    std::lock_guard block_lock(_mutex);
    KeyMeta new_meta;
    new_meta.expiration_time = expiration_time;
    new_meta.type = _key.meta.type;
    RETURN_IF_ERROR(_mgr->_storage->change_key_meta(_key, new_meta));
    _key.meta.expiration_time = expiration_time;
    return Status::OK();
}

FileBlock::State FileBlock::wait() {
    std::unique_lock block_lock(_mutex);

    if (_downloader_id == 0) {
        return _download_state;
    }

    if (_download_state == State::DOWNLOADING) {
        DCHECK(_downloader_id != 0 && _downloader_id != get_caller_id());
#if !defined(USE_BTHREAD_SCANNER)
        _cv.wait_for(block_lock, std::chrono::seconds(1));
#else
        _cv.wait_for(block_lock, 1000000);
#endif
    }

    return _download_state;
}

void FileBlock::complete_unlocked(std::lock_guard<std::mutex>& block_lock) {
    if (is_downloader_impl(block_lock)) {
        reset_downloader(block_lock);
        _cv.notify_all();
    }
}

std::string FileBlock::get_info_for_log() const {
    std::lock_guard block_lock(_mutex);
    return get_info_for_log_impl(block_lock);
}

std::string FileBlock::get_info_for_log_impl(std::lock_guard<doris::Mutex>& block_lock) const {
    std::stringstream info;
    info << "File block: " << range().to_string() << ", ";
    info << "state: " << state_to_string(_download_state) << ", ";
    info << "size: " << _block_range.size() << ", ";
    info << "downloader id: " << _downloader_id << ", ";
    info << "caller id: " << get_caller_id();

    return info.str();
}

FileBlock::State FileBlock::state_unlock(std::lock_guard<std::mutex>&) const {
    return _download_state;
}

std::string FileBlock::state_to_string(FileBlock::State state) {
    switch (state) {
    case FileBlock::State::DOWNLOADED:
        return "DOWNLOADED";
    case FileBlock::State::EMPTY:
        return "EMPTY";
    case FileBlock::State::DOWNLOADING:
        return "DOWNLOADING";
    case FileBlock::State::SKIP_CACHE:
        return "SKIP_CACHE";
    default:
        DCHECK(false);
        return "";
    }
}

bool FileBlock::has_finalized_state() const {
    return _download_state == State::DOWNLOADED;
}

FileBlocksHolder::~FileBlocksHolder() {
    for (auto file_block_it = file_blocks.begin(); file_block_it != file_blocks.end();) {
        auto current_file_block_it = file_block_it;
        auto& file_block = *current_file_block_it;
        BlockFileCacheManager* _mgr = file_block->_mgr;
        {
            std::lock_guard cache_lock(_mgr->_mutex);
            std::lock_guard block_lock(file_block->_mutex);
            file_block->complete_unlocked(block_lock);
            if (file_block.use_count() == 2) {
                DCHECK(file_block->state_unlock(block_lock) != FileBlock::State::DOWNLOADING);
                // one in cache, one in here
                if (file_block->state_unlock(block_lock) == FileBlock::State::EMPTY) {
                    _mgr->remove(file_block, cache_lock, block_lock);
                }
            }
        }
        file_block_it = file_blocks.erase(current_file_block_it);
    }
}

std::string FileBlocksHolder::to_string() {
    std::string ranges;
    for (const auto& file_block : file_blocks) {
        if (!ranges.empty()) {
            ranges += ", ";
        }
        ranges += file_block->range().to_string();
    }
    return ranges;
}

} // namespace io
} // namespace doris
