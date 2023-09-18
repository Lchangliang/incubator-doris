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

#include <memory>

#include "io/cache/file_cache_storage.h"

namespace doris::io {

class FSFileCacheStorage;

class BlockFileCacheManager {
    friend class FSFileCacheStorage;

public:
    /// use version 2 when USE_CACHE_VERSION2 = true, while use version 1 if false
    /// version 1.0: cache_base_path / key / offset
    /// version 2.0: cache_base_path / key_prefix / key / offset
    static constexpr bool USE_CACHE_VERSION2 = true;
    static constexpr int KEY_PREFIX_LENGTH = 3;

    static std::string cache_type_to_string(FileCacheType type);
private:
    std::string _cache_base_path;
    std::unique_ptr<FileCacheStorage> storage;
};

} // namespace doris::io
