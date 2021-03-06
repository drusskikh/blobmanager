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

"""This module provides interface and implementations of BLOB managers. BLOB
managers provides logic for storing data blocks with fixed length in BLOB files.
"""

import abc
import functools
import hashlib
import os

from numpy import uint32, uint64
import redis


def return_handler(f):
    """Decorator. Retruns non-zero numbers in the case of called function raises
    anexception and returns function's return in the case of success.
    """
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ValueError:
            return 1
        except AttributeError:
            return 2
        except IndexError:
            return 3
        except Exception:
            return 100
    return wrapped


class BaseBlobManager(object, metaclass=abc.ABCMeta):
    """Base class for BLOB managers. Represents interface for all BLOB manager
    classes.

    To implement this interface implement all three its' methods:
    init, put_block, get_block.

    All methods should return 0 in the case of success and non-zero in other
    cases.
    """

    @abc.abstractmethod
    def init(self, block_size: uint64, blob_size: uint32) -> int:
        """Abstract method. Initializes BLOB manager.

        Args:
            block_size (numpy.uint64): Block size.
            blob_size (numpy.uint32): Number of blocks in a BLOB.

        Returns:
            int: Returns 0 in the case of success and non-zero in other cases.
        """
        if not isinstance(block_size, uint64):
            raise ValueError("Wrong 'block_size' parameter type.")
        if not isinstance(blob_size, uint32):
            raise ValueError("Wrong 'blob_size' parameter type.")
        self.block_size = block_size
        self.blob_size = blob_size

    @abc.abstractmethod
    def put_block(self, block_id: uint64, block_data: bytearray) -> int:
        """Abstract method. Puts block with specified ID to BLOB storage.

        Args:
            block_id (numpy.uint64): ID of block to put.
            block_data (numpy.uint32): Stored data to put.

        Returns:
            int: Returns 0 in the case of success and non-zero in other cases.
        """
        if not isinstance(block_id, uint64):
            raise ValueError("Wrong 'block_id' parameter type.")
        if not isinstance(block_data, bytearray):
            raise ValueError("Wrong 'block_data' parameter type.")
        if len(block_data) != self.block_size:
            raise AttributeError("Incorrect block size.")

    @abc.abstractmethod
    def get_block(self, block_id: uint64, block_data: bytearray) -> int:
        """Abstract method. Gets block with specified ID from BLOB storage
        and write received data to block_data parameter.

        Args:
            block_id (numpy.uint64): ID of block to get.
            block_data (numpy.uint64): Object where received data will be
                written.

        Returns:
            int: Returns 0 in the case of success and non-zero in other cases.
        """
        if not isinstance(block_id, uint64):
            raise ValueError("Wrong 'block_id' parameter type.")
        if not isinstance(block_data, bytearray):
            raise ValueError("Wrong 'block_data' parameter type.")


class RedisBlobManager(BaseBlobManager):
    """Redis BLOB manager.

    Uses Redis database to store metadata. BLOBs are stored on the filesysem.

    Args:
        redis_host (str): Hostname of redis server.
        redis_port (int): Port of redis server.
        redis_db (int): Database ID to use.
        bob_dir (str): Directory name where BLOB files stored.

    Examples:

        >>> blob_manager = blobmanager.RedisBlobManager()
        >>> blob_manager.init(block_size, blob_size)
        0
        >>> blob_manager.put_block(block_id, block_data)
        0
        >>> received_block = bytearray()
        >>> blob_manager.get_block(block_id, received_block)
        0
    """

    def __init__(self, redis_host='127.0.0.1', redis_port=6379, redis_db=0,
                 blob_dir='/tmp/blob'):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.blob_dir = blob_dir

    @return_handler
    def init(self, block_size: uint64, blob_size: uint32) -> int:
        """Initialize method which set up BLOB manager.

        Args:
            block_size (numpy.uint64): Block size.
            blob_size (numpy.uint32): Number of blocks in a BLOB.

        Returns:
            int: Returns 0 in the case of success and non-zero in other cases.

        """
        super().init(block_size, blob_size)
        connection_pool = redis.ConnectionPool(host=self.redis_host,
                                               port=self.redis_port,
                                               db=self.redis_db)
        self.redis_client = redis.StrictRedis(connection_pool=connection_pool)
        self.redis_client.setnx('next_blob', 0)
        self.redis_client.setnx('next_blob_index', 0)
        return 0

    @return_handler
    def put_block(self, block_id: uint64, block_data: bytearray) -> int:
        """Puts block with specified ID to BLOB storage.

        Args:
            block_id (numpy.uint64): ID of block to put.
            block_data (numpy.uint32): Stored data to put.

        Returns:
            int: Returns 0 in the case of success and non-zero in other cases.
        """
        super().put_block(block_id, block_data)
        if self.redis_client.exists('block:' + str(block_id)):
            raise IndexError('Block with specified ID not found.')
        block_hash = hashlib.sha1(block_data).hexdigest()
        if self.redis_client.sismember('hashes', block_hash):
            self.redis_client.set('block:' + str(block_id), block_hash)
        else:
            self._put_block_to_blob(block_id, block_data, block_hash)
        return 0

    def _put_block_to_blob(self, block_id, block_data, block_hash):
        blob = blob_index = None

        def put_block_transaction(pipe):
            nonlocal blob, blob_index
            blob = int(pipe.get('next_blob'))
            blob_index = int(pipe.get('next_blob_index'))

            if blob_index + 1 >= self.blob_size:
                pipe.incr('next_blob')
                pipe.set('next_blob_index', 0)
            else:
                pipe.incr('next_blob_index')

            pipe.set('block:' + str(block_id), block_hash)
            pipe.sadd('hashes', block_hash)
            pipe.rpush('hash:' + block_hash, blob, blob_index)

        self.redis_client.transaction(put_block_transaction, 'next_blob',
                                      'next_blob_index')
        with open(self._get_file_path(blob), 'ba') as fd:
            fd.write(block_data)

    @return_handler
    def get_block(self, block_id: uint64, block_data: bytearray) -> int:
        """Gets block with specified ID from BLOB storage
        and write received data to block_data parameter.

        Args:
            block_id (numpy.uint64): ID of block to get.
            block_data (numpy.uint64): Object where received data will be
                written.

        Returns:
            int: Returns 0 in the case of success and non-zero in other cases.
        """
        super().get_block(block_id, block_data)
        block_hash = self.redis_client.get('block:' + str(block_id))
        if not block_hash:
            raise IndexError('Block with specified ID already exists.')
        pipe = self.redis_client.pipeline()
        pipe.lindex('hash:' + block_hash.decode(), 0)
        pipe.lindex('hash:' + block_hash.decode(), 1)
        blob, blob_index = pipe.execute()
        blob = int(blob)
        blob_index = int(blob_index)

        with open(self._get_file_path(blob), 'br') as fd:
            fd.seek(int(self.block_size * blob_index))
            ba = bytearray(fd.read(self.block_size))
        block_data.extend(ba)
        return 0

    def _get_file_path(self, blob):
        """Helper function. Returns path to BLOB file."""
        return os.path.join(self.blob_dir, str(blob))
