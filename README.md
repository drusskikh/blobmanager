# BLOBManager

## Overall

BLOBManager module provides interface and implementations of BLOB managers.
BLOB managers provides logic for storing data blocks with fixed length in BLOB
files. Foe now is only implemented manager is `RedisBlobManager`

## Installation

BLOBManager is tested for Python 3.5 and Ubuntu 16.04.

1. First install requirements:

```
sudo apt intall python3 python-tox
sudo apt intall redis-server
```

2. Download and unpack BLOBManager egg.
3. Install requirements from `requirements.txt` file.
4. Install package
```
python3 setup.py install
```

## Run functional tests

```
tox -e py35
```

## How to use

```python
import blobmanager
import numpy

# create an instance of RedisBlobManager
bm = blobmanager.RedisBlobManager()

# specify block_size and blob_size
block_size = numpy.uint64(4096)
blob_size = numpy.uint32(128)

# initialize blob manager
bm.init(block_size, blob_size)

# put block to blob
block_data = bytearray([1 for x in range(4096)])
bm.put_block(numpy.uint64(1), block_data)

# get block from blob
block_data = bytearray()
bm.get_block(numpy.uint64(1), block_data)
```
