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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/Fileblock.cpp
// and modified by Doris

#include "io/cache/block/file_block.h"

#include <glog/logging.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <sstream>
#include <string>
#include <thread>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/cache/block/block_file_cache.h"

namespace doris {
namespace io {

FileBlock::FileBlock(size_t offset, size_t size, const Key& key, BlockFileCache* cache,
                     State download_state, FileCacheType cache_type, int64_t expiration_time)
        : _block_range(offset, offset + size - 1),
          _download_state(download_state),
          _file_key(key),
          _cache(cache),
          _cache_type(cache_type) {
    /// On creation, file block state can be EMPTY, DOWNLOADED, DOWNLOADING.
    switch (_download_state) {
    /// EMPTY is used when file block is not in cache and
    /// someone will _potentially_ want to download it (after calling getOrSetDownloader()).
    case State::EMPTY:
    case State::SKIP_CACHE: {
        break;
    }
    /// DOWNLOADED is used either on initial cache metadata load into memory on server startup
    /// or on reduceSizeToDownloaded() -- when file block object is updated.
    case State::DOWNLOADED: {
        _downloaded_size = size;
        _is_downloaded = true;
        break;
    }
    default: {
        DCHECK(false) << "Can create cell with either EMPTY, DOWNLOADED, SKIP_CACHE ";
    }
    }
}

FileBlock::~FileBlock() {
    std::shared_ptr<FileReader> reader;
    if ((reader = _cache_reader.lock())) {
        BlockFileCache::remove_file_reader(std::make_pair(_file_key, offset()));
    }
}

FileBlock::State FileBlock::state() const {
    std::lock_guard block_lock(_mutex);
    return _download_state;
}

size_t FileBlock::get_download_offset() const {
    std::lock_guard block_lock(_mutex);
    return range().left + get_downloaded_size(block_lock);
}

size_t FileBlock::get_downloaded_size() const {
    std::lock_guard block_lock(_mutex);
    return get_downloaded_size(block_lock);
}

size_t FileBlock::get_downloaded_size(std::lock_guard<std::mutex>& /* block_lock */) const {
    if (_download_state == State::DOWNLOADED) {
        return _downloaded_size;
    }

    std::lock_guard download_lock(_download_mutex);
    return _downloaded_size;
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
    if (_downloaded_size == range().size()) {
        set_downloaded(block_lock);
    } else {
        _downloaded_size = 0;
        _download_state = State::EMPTY;
        _downloader_id = 0;
        _cache_writer.reset();
    }
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

Status FileBlock::append(Slice data) {
    DCHECK(data.size != 0) << "Writing zero size is not allowed";
    Status st = Status::OK();
    if (!_cache_writer) {
        auto download_path = get_path_in_local_cache(true);
        st = global_local_filesystem()->create_file(download_path, &_cache_writer);
        if (!st) {
            _cache_writer.reset();
            return st;
        }
    }

    RETURN_IF_ERROR(_cache_writer->append(data));

    std::lock_guard download_lock(_download_mutex);

    _downloaded_size += data.size;
    return st;
}

std::string FileBlock::get_path_in_local_cache(bool is_tmp) const {
    return _cache->get_path_in_local_cache(key(), _expiration_time, offset(), _cache_type, is_tmp);
}

Status FileBlock::read_at(Slice buffer, size_t read_offset) {
    Status st = Status::OK();
    std::shared_ptr<FileReader> reader;
    if (!(reader = _cache_reader.lock())) {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!(reader = _cache_reader.lock())) {
            auto download_path = get_path_in_local_cache();
            RETURN_IF_ERROR(global_local_filesystem()->open_file(download_path, &reader));
            _cache_reader =
                    BlockFileCache::cache_file_reader(std::make_pair(_file_key, offset()), reader);
        }
    }
    size_t bytes_reads = buffer.size;
    RETURN_IF_ERROR(reader->read_at(read_offset, buffer, &bytes_reads));
    DCHECK(bytes_reads == buffer.size);
    return st;
}

bool FileBlock::change_cache_type(FileCacheType new_type) {
    std::unique_lock block_lock(_mutex);
    if (new_type == _cache_type) {
        return true;
    }
    if (_download_state == State::DOWNLOADED) {
        std::error_code ec;
        std::filesystem::rename(
                get_path_in_local_cache(),
                _cache->get_path_in_local_cache(key(), _expiration_time, offset(), new_type), ec);
        if (ec) {
            LOG(ERROR) << ec.message();
            return false;
        }
    }
    _cache_type = new_type;
    return true;
}

void FileBlock::change_cache_type_self(FileCacheType new_type) {
    std::lock_guard cache_lock(_cache->_mutex);
    std::unique_lock block_lock(_mutex);
    if (_cache_type == FileCacheType::TTL || new_type == _cache_type) {
        return;
    }
    _cache_type = new_type;
    _cache->change_cache_type(_file_key, _block_range.left, new_type, cache_lock);
}

Status FileBlock::finalize_write() {
    if (_downloaded_size != 0 && _downloaded_size != _block_range.size()) {
        std::lock_guard cache_lock(_cache->_mutex);
        size_t old_size = _block_range.size();
        _block_range.right = _block_range.left + _downloaded_size - 1;
        size_t new_size = _block_range.size();
        DCHECK(new_size < old_size);
        _cache->reset_range(_file_key, _block_range.left, old_size, new_size, cache_lock);
    }

    std::lock_guard block_lock(_mutex);
    RETURN_IF_ERROR(set_downloaded(block_lock));
    _cv.notify_all();
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

Status FileBlock::set_downloaded(std::lock_guard<std::mutex>& /* block_lock */) {
    Status status = Status::OK();
    if (_is_downloaded) {
        return status;
    }

    if (_downloaded_size == 0) {
        _download_state = State::EMPTY;
        _downloader_id = 0;
        _cache_writer.reset();
        return status;
    }

    if (_cache_writer) {
        RETURN_IF_ERROR(_cache_writer->close());
        _cache_writer.reset();
    }

    std::error_code ec;
    std::filesystem::rename(get_path_in_local_cache(true), get_path_in_local_cache(), ec);
    if (ec) {
        LOG(ERROR) << fmt::format("failed to rename {} to {} : {}", get_path_in_local_cache(true),
                                  get_path_in_local_cache(), ec.message());
        status = Status::IOError(ec.message());
    }

    if (status.ok()) [[likely]] {
        _is_downloaded = true;
        _download_state = State::DOWNLOADED;
    } else {
        _download_state = State::EMPTY;
    }
    _downloader_id = 0;
    return status;
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
    info << "downloaded size: " << get_downloaded_size(block_lock) << ", ";
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
    /// In CacheableReadBufferFromRemoteFS file block's downloader removes file blocks from
    /// FileBlocksHolder right after calling file_block->complete(), so on destruction here
    /// remain only uncompleted file blocks.

    BlockFileCachePtr cache = nullptr;

    for (auto file_block_it = file_blocks.begin(); file_block_it != file_blocks.end();) {
        auto current_file_block_it = file_block_it;
        auto& file_block = *current_file_block_it;

        if (!cache) {
            cache = file_block->_cache;
        }

        {
            std::lock_guard cache_lock(cache->_mutex);
            std::lock_guard block_lock(file_block->_mutex);
            file_block->complete_unlocked(block_lock);
            if (file_block.use_count() == 2) {
                DCHECK(file_block->state_unlock(block_lock) != FileBlock::State::DOWNLOADING);
                // one in cache, one in here
                if (file_block->state_unlock(block_lock) == FileBlock::State::EMPTY) {
                    cache->remove(file_block, cache_lock, block_lock);
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
