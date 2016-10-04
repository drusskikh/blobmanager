#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import os
import unittest

from numpy import uint32, uint64
import redis

import blobmanager


class TestFlaskManagerFunctional(unittest.TestCase):
    """Functional test case for testing FlaskBlobManager."""

    def setUp(self):
        redis_client = redis.StrictRedis()
        redis_client.flushdb()
        test_dir = '/tmp/blobmanager_tests'

        try:
            os.stat(test_dir)
        except Exception:
            os.mkdir(test_dir)
        else:
            os.removedirs(test_dir)
            os.mkdir(test_dir)

        self.block_size = uint64(8)
        self.blob_size = uint32(2)

    def test_init(self):
        b_manager = blobmanager.RedisBlobManager()
        ret_value = b_manager.init(self.block_size, self.blob_size)
        self.assertEqual(ret_value, 0)

    def test_init_wrong_block_size_type(self):
        b_manager = blobmanager.RedisBlobManager()
        ret_value = b_manager.init(8, self.blob_size)
        self.assertEqual(ret_value, 1)

    def test_init_wrong_blob_size_type(self):
        b_manager = blobmanager.RedisBlobManager()
        ret_value = b_manager.init(self.block_size, 2)
        self.assertEqual(ret_value, 1)

    def test_put_and_get_block(self):
        b_manager = blobmanager.RedisBlobManager()
        b_manager.init(self.block_size, self.blob_size)
        data_1 = bytearray([1 for x in range(8)])
        block_id = uint64(1)
        ret_value = b_manager.put_block(block_id, data_1)
        self.assertEqual(ret_value, 0)
        data_2 = bytearray()
        ret_value = b_manager.get_block(block_id, data_2)
        self.assertEqual(ret_value, 0)
        self.assertListEqual(list(data_1), list(data_2))

    def test_put_wrong_block_id_type(self):
        b_manager = blobmanager.RedisBlobManager()
        b_manager.init(self.block_size, self.blob_size)
        data = bytearray([1 for x in range(8)])
        ret_value = b_manager.put_block(1, data)
        self.assertEqual(ret_value, 1)

    def test_put_wrong_block_data_type(self):
        b_manager = blobmanager.RedisBlobManager()
        b_manager.init(self.block_size, self.blob_size)
        block_id = uint64(1)
        ret_value = b_manager.put_block(block_id, [1 for x in range(8)])
        self.assertEqual(ret_value, 1)

    def test_put_exist_id(self):
        b_manager = blobmanager.RedisBlobManager()
        b_manager.init(self.block_size, self.blob_size)
        data_1 = bytearray([1 for x in range(8)])
        data_2 = bytearray([2 for x in range(8)])
        block_id = uint64(1)
        ret_value = b_manager.put_block(block_id, data_1)
        self.assertEqual(ret_value, 0)
        ret_value = b_manager.put_block(block_id, data_2)
        self.assertEqual(ret_value, 3)

    def test_put_wrong_data_size(self):
        b_manager = blobmanager.RedisBlobManager()
        b_manager.init(self.block_size, self.blob_size)
        data_1 = bytearray([1 for x in range(4)])
        block_id = uint64(1)
        ret_value = b_manager.put_block(block_id, data_1)
        self.assertEqual(ret_value, 2)

    def test_get_wrong_block_id_type(self):
        b_manager = blobmanager.RedisBlobManager()
        b_manager.init(self.block_size, self.blob_size)
        data_1 = bytearray([1 for x in range(8)])
        block_id = uint64(1)
        ret_value = b_manager.put_block(block_id, data_1)
        self.assertEqual(ret_value, 0)
        data_2 = bytearray()
        ret_value = b_manager.get_block(1, data_2)
        self.assertEqual(ret_value, 1)

    def test_get_wrong_block_data_type(self):
        b_manager = blobmanager.RedisBlobManager()
        b_manager.init(self.block_size, self.blob_size)
        data_1 = bytearray([1 for x in range(8)])
        block_id = uint64(1)
        ret_value = b_manager.put_block(block_id, data_1)
        self.assertEqual(ret_value, 0)
        data_2 = []
        ret_value = b_manager.get_block(block_id, data_2)
        self.assertEqual(ret_value, 1)

    def test_get_unexistant_block(self):
        b_manager = blobmanager.RedisBlobManager()
        b_manager.init(self.block_size, self.blob_size)
        data_1 = bytearray()
        block_id = uint64(1)
        ret_value = b_manager.get_block(block_id, data_1)
        self.assertEqual(ret_value, 3)

    def test_deduplication(self):
        b_manager = blobmanager.RedisBlobManager()
        b_manager.init(self.block_size, self.blob_size)
        data_1 = bytearray([1 for x in range(8)])
        data_2 = bytearray([1 for x in range(8)])
        block_id_1 = uint64(1)
        block_id_2 = uint64(2)
        ret_value = b_manager.put_block(block_id_1, data_1)
        self.assertEqual(ret_value, 0)
        ret_value = b_manager.put_block(block_id_2, data_2)
        self.assertEqual(ret_value, 0)
        self.assertEqual(b_manager.redis_client.get('block:' + str(block_id_1)),
                         b_manager.redis_client.get('block:' + str(block_id_2)))
