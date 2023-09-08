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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/tests/gtest_lru_file_cache.cpp
// and modified by Doris

#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <condition_variable>
#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "io/cache/block/block_file_cache.h"
#include "io/cache/block/block_file_cache_settings.h"
#include "io/cache/block/file_block.h"
#include "io/fs/path.h"
#include "olap/options.h"
#include "util/slice.h"

namespace doris::io {

namespace fs = std::filesystem;

fs::path caches_dir = fs::current_path() / "lru_cache_test";
std::string cache_base_path = caches_dir / "cache1" / "";

void assert_range([[maybe_unused]] size_t assert_n, io::FileBlockSPtr file_block,
                  const io::FileBlock::Range& expected_range, io::FileBlock::State expected_state) {
    auto range = file_block->range();

    ASSERT_EQ(range.left, expected_range.left);
    ASSERT_EQ(range.right, expected_range.right);
    ASSERT_EQ(file_block->state(), expected_state);
}

std::vector<io::FileBlockSPtr> fromHolder(const io::FileBlocksHolder& holder) {
    return std::vector<io::FileBlockSPtr>(holder.file_blocks.begin(), holder.file_blocks.end());
}

std::string getFileBlockPath(const std::string& base_path, const io::Key& key, size_t offset) {
    auto key_str = key.to_string();
    if constexpr (io::BlockFileCache::USE_CACHE_VERSION2) {
        return fs::path(base_path) / key_str.substr(0, 3) / key_str / std::to_string(offset);
    } else {
        return fs::path(base_path) / key_str / std::to_string(offset);
    }
}

void download(io::FileBlockSPtr file_block, size_t size = 0) {
    const auto& key = file_block->key();
    if (size == 0) {
        size = file_block->range().size();
    }

    auto key_str = key.to_string();
    auto subdir = io::BlockFileCache::USE_CACHE_VERSION2
                          ? fs::path(cache_base_path) / key_str.substr(0, 3) /
                                    (key_str + "_" + std::to_string(file_block->expiration_time()))
                          : fs::path(cache_base_path) /
                                    (key_str + "_" + std::to_string(file_block->expiration_time()));
    ASSERT_TRUE(fs::exists(subdir));

    std::string data(size, '0');
    Slice result(data.data(), size);
    file_block->append(result);
    file_block->finalize_write();
}

void complete(const io::FileBlocksHolder& holder) {
    for (const auto& file_block : holder.file_blocks) {
        ASSERT_TRUE(file_block->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(file_block);
    }
}

TEST(BlockFileCache, init) {
    std::string string = std::string(R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "total_size" : 193273528320,
            "query_limit" : 38654705664
        },
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "total_size" : 193273528320,
            "query_limit" : 38654705664
        }
        ]
        )");
    config::enable_file_cache_query_limit = true;
    std::vector<CachePath> cache_paths;
    EXPECT_TRUE(parse_conf_cache_paths(string, cache_paths));
    EXPECT_EQ(cache_paths.size(), 2);
    for (const auto& cache_path : cache_paths) {
        io::FileCacheSettings settings = cache_path.init_settings();
        EXPECT_EQ(settings.total_size, 193273528320);
        EXPECT_EQ(settings.max_query_cache_size, 38654705664);
    }

    // err normal
    std::string err_string = std::string(R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "total_size" : "193273528320",
            "query_limit" : 38654705664
        }
        ]
        )");
    cache_paths.clear();
    EXPECT_FALSE(parse_conf_cache_paths(err_string, cache_paths));

    // err query_limit
    err_string = std::string(R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "normal" : 193273528320,
            "persistent" : 193273528320,
            "query_limit" : "38654705664"
        }
        ]
        )");
    cache_paths.clear();
    EXPECT_FALSE(parse_conf_cache_paths(err_string, cache_paths));
}

void test_file_cache(io::FileCacheType cache_type) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;

    TUniqueId other_query_id;
    other_query_id.hi = 2;
    other_query_id.lo = 2;

    io::FileCacheSettings settings;
    switch (cache_type) {
    case io::FileCacheType::INDEX:
        settings.index_queue_elements = 5;
        settings.index_queue_size = 30;
        break;
    case io::FileCacheType::NORMAL:
        settings.query_queue_size = 30;
        settings.query_queue_elements = 5;
        break;
    case io::FileCacheType::DISPOSABLE:
        settings.disposable_queue_size = 30;
        settings.disposable_queue_elements = 5;
        break;
    default:
        break;
    }
    settings.total_size = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context;
    context.cache_type = other_context.cache_type = cache_type;
    context.query_id = query_id;
    other_context.query_id = other_query_id;
    auto key = io::BlockFileCache::hash("key1");
    {
        io::BlockFileCache cache(cache_base_path, settings);
        ASSERT_TRUE(cache.initialize());
        while (true) {
            if (cache.get_lazy_open_success()) {
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        {
            auto holder = cache.get_or_set(key, 0, 10, context); /// Add range [0, 9]
            auto blocks = fromHolder(holder);
            /// Range was not present in cache. It should be added in cache as one while file block.
            ASSERT_EQ(blocks.size(), 1);
            assert_range(1, blocks[0], io::FileBlock::Range(0, 9), io::FileBlock::State::EMPTY);
            ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            assert_range(2, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADING);
            download(blocks[0]);
            assert_range(3, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
        }
        /// Current cache:    [__________]
        ///                   ^          ^
        ///                   0          9
        ASSERT_EQ(cache.get_file_blocks_num(cache_type), 1);
        ASSERT_EQ(cache.get_used_cache_size(cache_type), 10);
        {
            /// Want range [5, 14], but [0, 9] already in cache, so only [10, 14] will be put in cache.
            auto holder = cache.get_or_set(key, 5, 10, context);
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 2);

            assert_range(4, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(5, blocks[1], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);

            ASSERT_TRUE(blocks[1]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(blocks[1]);
            assert_range(6, blocks[1], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
        }

        /// Current cache:    [__________][_____]
        ///                   ^          ^^     ^
        ///                   0          910    14
        ASSERT_EQ(cache.get_file_blocks_num(cache_type), 2);
        ASSERT_EQ(cache.get_used_cache_size(cache_type), 15);

        {
            auto holder = cache.get_or_set(key, 9, 1, context); /// Get [9, 9]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            assert_range(7, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
        }
        {
            auto holder = cache.get_or_set(key, 9, 2, context); /// Get [9, 10]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 2);
            assert_range(8, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(9, blocks[1], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
        }
        {
            auto holder = cache.get_or_set(key, 10, 1, context); /// Get [10, 10]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            assert_range(10, blocks[0], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
        }
        complete(cache.get_or_set(key, 17, 4, context)); /// Get [17, 20]
        complete(cache.get_or_set(key, 24, 3, context)); /// Get [24, 26]

        /// Current cache:    [__________][_____]   [____]    [___]
        ///                   ^          ^^     ^   ^    ^    ^   ^
        ///                   0          910    14  17   20   24  26
        ///
        ASSERT_EQ(cache.get_file_blocks_num(cache_type), 4);
        ASSERT_EQ(cache.get_used_cache_size(cache_type), 22);
        {
            auto holder = cache.get_or_set(key, 0, 31, context); /// Get [0, 25]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 7);
            assert_range(11, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(12, blocks[1], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
            /// Missing [15, 16] should be added in cache.
            assert_range(13, blocks[2], io::FileBlock::Range(15, 16), io::FileBlock::State::EMPTY);
            ASSERT_TRUE(blocks[2]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(blocks[2]);

            assert_range(14, blocks[3], io::FileBlock::Range(17, 20),
                         io::FileBlock::State::DOWNLOADED);

            assert_range(15, blocks[4], io::FileBlock::Range(21, 23), io::FileBlock::State::EMPTY);

            assert_range(16, blocks[5], io::FileBlock::Range(24, 26),
                         io::FileBlock::State::DOWNLOADED);

            assert_range(16, blocks[6], io::FileBlock::Range(27, 30),
                         io::FileBlock::State::SKIP_CACHE);
            /// Current cache:    [__________][_____][   ][____________]
            ///                   ^                       ^            ^
            ///                   0                        20          26
            ///

            /// Range [27, 30] must be evicted in previous getOrSet [0, 25].
            /// Let's not invalidate pointers to returned blocks from range [0, 25] and
            /// as max elements size is reached, next attempt to put something in cache should fail.
            /// This will also check that [27, 27] was indeed evicted.

            auto holder1 = cache.get_or_set(key, 27, 4, context);
            auto blocks_1 = fromHolder(holder1); /// Get [27, 30]
            ASSERT_EQ(blocks_1.size(), 1);
            assert_range(17, blocks_1[0], io::FileBlock::Range(27, 30),
                         io::FileBlock::State::SKIP_CACHE);
        }
        /// Current cache:    [__________][_____][_][____]    [___]
        ///                   ^          ^^     ^   ^    ^    ^   ^
        ///                   0          910    14  17   20   24  26
        ///
        {
            auto holder = cache.get_or_set(key, 12, 10, context); /// Get [12, 21]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 4);

            assert_range(18, blocks[0], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(19, blocks[1], io::FileBlock::Range(15, 16),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(20, blocks[2], io::FileBlock::Range(17, 20),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(21, blocks[3], io::FileBlock::Range(21, 21), io::FileBlock::State::EMPTY);

            ASSERT_TRUE(blocks[3]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(blocks[3]);
            ASSERT_TRUE(blocks[3]->state() == io::FileBlock::State::DOWNLOADED);
        }
        /// Current cache:    [__________][_____][_][____][_]    [___]
        ///                   ^          ^^     ^   ^    ^       ^   ^
        ///                   0          910    14  17   20      24  26

        ASSERT_EQ(cache.get_file_blocks_num(cache_type), 6);
        {
            auto holder = cache.get_or_set(key, 23, 5, context); /// Get [23, 28]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 3);

            assert_range(22, blocks[0], io::FileBlock::Range(23, 23), io::FileBlock::State::EMPTY);
            assert_range(23, blocks[1], io::FileBlock::Range(24, 26),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(24, blocks[2], io::FileBlock::Range(27, 27), io::FileBlock::State::EMPTY);

            ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            ASSERT_TRUE(blocks[2]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(blocks[0]);
            download(blocks[2]);
        }
        /// Current cache:    [__________][_____][_][____][_]  [_][___][_]
        ///                   ^          ^^     ^   ^    ^        ^   ^
        ///                   0          910    14  17   20       24  26

        ASSERT_EQ(cache.get_file_blocks_num(cache_type), 8);
        {
            auto holder5 = cache.get_or_set(key, 2, 3, context); /// Get [2, 4]
            auto s5 = fromHolder(holder5);
            ASSERT_EQ(s5.size(), 1);
            assert_range(25, s5[0], io::FileBlock::Range(0, 9), io::FileBlock::State::DOWNLOADED);

            auto holder1 = cache.get_or_set(key, 30, 2, context); /// Get [30, 31]
            auto s1 = fromHolder(holder1);
            ASSERT_EQ(s1.size(), 1);
            assert_range(26, s1[0], io::FileBlock::Range(30, 31), io::FileBlock::State::EMPTY);

            ASSERT_TRUE(s1[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(s1[0]);

            /// Current cache:    [__________][_____][_][____][_]  [_][___][_]    [__]
            ///                   ^          ^^     ^   ^    ^        ^   ^  ^    ^  ^
            ///                   0          910    14  17   20       24  26 27   30 31

            auto holder2 = cache.get_or_set(key, 23, 1, context); /// Get [23, 23]
            auto s2 = fromHolder(holder2);
            ASSERT_EQ(s2.size(), 1);

            auto holder3 = cache.get_or_set(key, 24, 3, context); /// Get [24, 26]
            auto s3 = fromHolder(holder3);
            ASSERT_EQ(s3.size(), 1);

            auto holder4 = cache.get_or_set(key, 27, 1, context); /// Get [27, 27]
            auto s4 = fromHolder(holder4);
            ASSERT_EQ(s4.size(), 1);

            /// All cache is now unreleasable because pointers are still hold
            auto holder6 = cache.get_or_set(key, 0, 40, context);
            auto f = fromHolder(holder6);
            ASSERT_EQ(f.size(), 12);

            assert_range(29, f[9], io::FileBlock::Range(28, 29), io::FileBlock::State::SKIP_CACHE);
            assert_range(30, f[11], io::FileBlock::Range(32, 39), io::FileBlock::State::SKIP_CACHE);
        }
        {
            auto holder = cache.get_or_set(key, 2, 3, context); /// Get [2, 4]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            assert_range(31, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
        }
        /// Current cache:    [__________][_____][_][____][_]  [_][___][_]    [__]
        ///                   ^          ^^     ^   ^    ^        ^   ^  ^    ^  ^
        ///                   0          910    14  17   20       24  26 27   30 31

        {
            auto holder = cache.get_or_set(key, 25, 5, context); /// Get [25, 29]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 3);

            assert_range(32, blocks[0], io::FileBlock::Range(24, 26),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(33, blocks[1], io::FileBlock::Range(27, 27),
                         io::FileBlock::State::DOWNLOADED);

            assert_range(34, blocks[2], io::FileBlock::Range(28, 29), io::FileBlock::State::EMPTY);
            ASSERT_TRUE(blocks[2]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            ASSERT_TRUE(blocks[2]->state() == io::FileBlock::State::DOWNLOADING);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&] {
                auto holder_2 =
                        cache.get_or_set(key, 25, 5, other_context); /// Get [25, 29] once again.
                auto blocks_2 = fromHolder(holder_2);
                ASSERT_EQ(blocks.size(), 3);

                assert_range(35, blocks_2[0], io::FileBlock::Range(24, 26),
                             io::FileBlock::State::DOWNLOADED);
                assert_range(36, blocks_2[1], io::FileBlock::Range(27, 27),
                             io::FileBlock::State::DOWNLOADED);
                assert_range(37, blocks_2[2], io::FileBlock::Range(28, 29),
                             io::FileBlock::State::DOWNLOADING);

                ASSERT_TRUE(blocks[2]->get_or_set_downloader() != io::FileBlock::get_caller_id());
                ASSERT_TRUE(blocks[2]->state() == io::FileBlock::State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                while (blocks_2[2]->wait() == io::FileBlock::State::DOWNLOADING) {
                }
                ASSERT_TRUE(blocks_2[2]->state() == io::FileBlock::State::DOWNLOADED);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&] { return lets_start_download; });
            }

            download(blocks[2]);
            ASSERT_TRUE(blocks[2]->state() == io::FileBlock::State::DOWNLOADED);

            other_1.join();
        }
        ASSERT_EQ(cache.get_file_blocks_num(cache_type), 5);
        /// Current cache:    [__________] [___][_][__][__]
        ///                   ^          ^ ^   ^  ^    ^  ^
        ///                   0          9 24  26 27   30 31

        {
            /// Now let's check the similar case but getting ERROR state after block->wait(), when
            /// state is changed not manually via block->complete(state) but from destructor of holder
            /// and notify_all() is also called from destructor of holder.

            std::optional<io::FileBlocksHolder> holder;
            holder.emplace(cache.get_or_set(key, 3, 23, context)); /// Get [3, 25]

            auto blocks = fromHolder(*holder);
            ASSERT_EQ(blocks.size(), 3);

            assert_range(38, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);

            assert_range(39, blocks[1], io::FileBlock::Range(10, 23), io::FileBlock::State::EMPTY);
            ASSERT_TRUE(blocks[1]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            ASSERT_TRUE(blocks[1]->state() == io::FileBlock::State::DOWNLOADING);
            assert_range(38, blocks[2], io::FileBlock::Range(24, 26),
                         io::FileBlock::State::DOWNLOADED);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&] {
                auto holder_2 =
                        cache.get_or_set(key, 3, 23, other_context); /// Get [3, 25] once again
                auto blocks_2 = fromHolder(*holder);
                ASSERT_EQ(blocks_2.size(), 3);

                assert_range(41, blocks_2[0], io::FileBlock::Range(0, 9),
                             io::FileBlock::State::DOWNLOADED);
                assert_range(42, blocks_2[1], io::FileBlock::Range(10, 23),
                             io::FileBlock::State::DOWNLOADING);
                assert_range(43, blocks_2[2], io::FileBlock::Range(24, 26),
                             io::FileBlock::State::DOWNLOADED);

                ASSERT_TRUE(blocks_2[1]->get_downloader() != io::FileBlock::get_caller_id());
                ASSERT_TRUE(blocks_2[1]->state() == io::FileBlock::State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                while (blocks_2[1]->wait() == io::FileBlock::State::DOWNLOADING) {
                }
                ASSERT_TRUE(blocks_2[1]->state() == io::FileBlock::State::EMPTY);
                ASSERT_TRUE(blocks_2[1]->get_or_set_downloader() == io::FileBlock::get_caller_id());
                download(blocks_2[1]);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&] { return lets_start_download; });
            }

            holder.reset();
            other_1.join();
            ASSERT_TRUE(blocks[1]->state() == io::FileBlock::State::DOWNLOADED);
        }
    }
    /// Current cache:    [__________][___][___][_][__]
    ///                   ^          ^      ^    ^  ^ ^
    ///                   0          9      24  26 27  29
    {
        /// Test LRUCache::restore().

        io::BlockFileCache cache2(cache_base_path, settings);
        cache2.initialize();
        while (true) {
            if (cache2.get_lazy_open_success()) {
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        auto holder1 = cache2.get_or_set(key, 2, 28, context); /// Get [2, 29]

        auto blocks1 = fromHolder(holder1);
        ASSERT_EQ(blocks1.size(), 5);

        assert_range(44, blocks1[0], io::FileBlock::Range(0, 9), io::FileBlock::State::DOWNLOADED);
        assert_range(45, blocks1[1], io::FileBlock::Range(10, 23),
                     io::FileBlock::State::DOWNLOADED);
        assert_range(45, blocks1[2], io::FileBlock::Range(24, 26),
                     io::FileBlock::State::DOWNLOADED);
        assert_range(46, blocks1[3], io::FileBlock::Range(27, 27),
                     io::FileBlock::State::DOWNLOADED);
        assert_range(47, blocks1[4], io::FileBlock::Range(28, 29),
                     io::FileBlock::State::DOWNLOADED);
    }

    {
        /// Test max file block size
        auto settings2 = settings;
        settings2.index_queue_elements = 5;
        settings2.index_queue_size = 30;
        settings2.disposable_queue_size = 0;
        settings2.disposable_queue_elements = 0;
        settings2.query_queue_size = 0;
        settings2.query_queue_elements = 0;
        settings2.max_file_block_size = 10;
        io::BlockFileCache cache2(caches_dir / "cache2", settings2);
        cache2.initialize();
        while (true) {
            if (cache2.get_lazy_open_success()) {
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        auto holder1 = cache2.get_or_set(key, 0, 25, context); /// Get [0, 24]
        auto blocks1 = fromHolder(holder1);

        ASSERT_EQ(blocks1.size(), 3);
        assert_range(48, blocks1[0], io::FileBlock::Range(0, 9), io::FileBlock::State::EMPTY);
        assert_range(49, blocks1[1], io::FileBlock::Range(10, 19), io::FileBlock::State::EMPTY);
        assert_range(50, blocks1[2], io::FileBlock::Range(20, 24), io::FileBlock::State::EMPTY);
    }
}

TEST(BlockFileCache, normal) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::DISPOSABLE);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }

    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::INDEX);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }

    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, resize) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::INDEX);
    /// Current cache:    [__________][___][___][_][__]
    ///                   ^          ^      ^    ^  ^ ^
    ///                   0          9      24  26 27  29
    io::FileCacheSettings settings;
    settings.index_queue_elements = 5;
    settings.index_queue_size = 10;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 0;
    settings.query_queue_elements = 0;
    settings.max_file_block_size = 100;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, query_limit_heap_use_after_free) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    context.query_id = query_id;
    auto query_context_holder = cache.get_query_context_holder(query_id);
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(5, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADED);
    }
    {
        auto holder = cache.get_or_set(key, 15, 1, context); /// Add range [15, 15]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(6, blocks[0], io::FileBlock::Range(15, 15), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(7, blocks[0], io::FileBlock::Range(15, 15), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 16, 9, context); /// Add range [16, 24]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(8, blocks[0], io::FileBlock::Range(16, 24), io::FileBlock::State::SKIP_CACHE);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, query_limit_dcheck) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    context.query_id = query_id;
    auto query_context_holder = cache.get_query_context_holder(query_id);
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(5, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADED);
    }
    {
        auto holder = cache.get_or_set(key, 15, 1, context); /// Add range [15, 15]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(6, blocks[0], io::FileBlock::Range(15, 15), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(7, blocks[0], io::FileBlock::Range(15, 15), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    // double add
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 30, 5, context); /// Add range [30, 34]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(30, 34), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(30, 34), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 40, 5, context); /// Add range [40, 44]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(40, 44), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(40, 44), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 50, 5, context); /// Add range [50, 54]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 54), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(50, 54), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, reset_range) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0], 6);
    }
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 2);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 5), io::FileBlock::State::DOWNLOADED);
        assert_range(2, blocks[1], io::FileBlock::Range(6, 8), io::FileBlock::State::EMPTY);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, change_cache_type) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 5;
    settings.index_queue_size = 15;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 30;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        size_t size = blocks[0]->range().size();
        std::string data(size, '0');
        Slice result(data.data(), size);
        blocks[0]->append(result);
        blocks[0]->change_cache_type_self(io::FileCacheType::INDEX);
        blocks[0]->finalize_write();
        auto key_str = key.to_string();
        auto subdir = io::BlockFileCache::USE_CACHE_VERSION2
                          ? fs::path(cache_base_path) / key_str.substr(0, 3) /
                                    (key_str + "_" + std::to_string(blocks[0]->expiration_time()))
                          : fs::path(cache_base_path) /
                                    (key_str + "_" + std::to_string(blocks[0]->expiration_time()));
        ASSERT_TRUE(fs::exists(subdir / "0_idx"));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, fd_cache_remove) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(9);
        blocks[0]->read_at(Slice(buffer.get(), 9), 0);
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 0)));
    }
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(1);
        blocks[0]->read_at(Slice(buffer.get(), 1), 0);
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 9)));
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(5);
        blocks[0]->read_at(Slice(buffer.get(), 5), 0);
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 10)));
    }
    {
        auto holder = cache.get_or_set(key, 15, 10, context); /// Add range [15, 24]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(15, 24), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(15, 24), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(10);
        blocks[0]->read_at(Slice(buffer.get(), 10), 0);
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 15)));
    }
    EXPECT_FALSE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 0)));
    EXPECT_EQ(io::BlockFileCache::file_reader_cache_size(), 2);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST(BlockFileCache, fd_cache_evict) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.total_size = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    while (true) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    config::file_cache_max_file_reader_cache_size = 2;
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id())
                << blocks[0]->get_or_set_downloader() << " " << io::FileBlock::get_caller_id();
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(9);
        blocks[0]->read_at(Slice(buffer.get(), 9), 0);
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 0)));
    }
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(1);
        blocks[0]->read_at(Slice(buffer.get(), 1), 0);
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 9)));
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(5);
        blocks[0]->read_at(Slice(buffer.get(), 5), 0);
        EXPECT_TRUE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 10)));
    }
    EXPECT_FALSE(io::BlockFileCache::contains_file_reader(std::make_pair(key, 0)));
    EXPECT_EQ(io::BlockFileCache::file_reader_cache_size(), 2);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

} // namespace doris::io
