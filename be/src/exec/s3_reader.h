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

#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "exec/file_reader.h"
#include "util/s3_uri.h"

constexpr int MB = 1 << 20;

namespace Aws {
namespace S3 {
class S3Client;
}
} // namespace Aws

namespace doris {
class S3Reader : public FileReader {
public:
    S3Reader(const std::map<std::string, std::string>& properties, const std::string& path,
             int64_t start_offset);
    ~S3Reader() override = default;
    Status open() override;
    // Read content to 'buf', 'buf_len' is the max size of this buffer.
    // Return ok when read success, and 'buf_len' is set to size of read content
    // If reach to end of file, the eof is set to true. meanwhile 'buf_len'
    // is set to zero.
    Status read(uint8_t* buf, int64_t buf_len, int64_t* bytes_read, bool* eof) override;
    Status readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) override;

    /**
     * This interface is used read a whole message, For example: read a message from kafka.
     *
     * if read eof then return Status::OK and length is set 0 and buf is set nullptr,
     *  other return readed bytes.
     */
    Status read_one_message(std::unique_ptr<uint8_t[]>* buf, int64_t* length) override;
    int64_t size() override;
    Status seek(int64_t position) override;
    Status tell(int64_t* position) override;
    void close() override;
    bool closed() override;

private:
    void start_perfetch_worker();
    void perfetch_worker(int index);

private:
    const std::map<std::string, std::string>& _properties;
    std::string _path;
    S3URI _uri;
    int64_t _cur_offset;
    int64_t _file_size;
    bool _closed;
    std::shared_ptr<Aws::S3::S3Client> _client;

private:
    inline static constexpr int64_t _prefetch_worker_num = 4;
    inline static constexpr int64_t _buffer_size = 1 * MB;
    inline static constexpr int64_t _skip_size = _prefetch_worker_num * _buffer_size;
    inline static constexpr int64_t _max_prefetch_size = 100 * MB;
    inline static constexpr int64_t _capacity =
            _max_prefetch_size / _buffer_size / _prefetch_worker_num;
    size_t _cur_index = 0;
    std::vector<Status> _worker_status;
    std::vector<std::thread> _perfetch_threads;
    using spsc_queue_type =
            boost::lockfree::spsc_queue<std::vector<char>, boost::lockfree::capacity<_capacity>>;
    std::vector<std::shared_ptr<spsc_queue_type>> _prefetch_queues;
};
} // end namespace doris
