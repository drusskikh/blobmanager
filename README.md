# BLOBManager

## Overall

BLOBManager module provides interface and implementations of BLOB managers.
BLOB managers provides logic for storing data blocks with fixed length in BLOB
files. Foe now is only implemented manager is `RedisBlobManager`

## Installation

BLOBManager is tested for Python 3.5 and Ubuntu 16.04.

1. First install requirements:

> sudo apt intall python3 python-tox
> sudo apt intall redis-server

2. Download and unpack BLOBManager egg.
3. Install requirements from `requirements.txt` file.

## Run functional tests

> tox -e py35

## How to use

pass
