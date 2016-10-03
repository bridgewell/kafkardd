#coding=utf-8

import pytest

from kafkardd.util import generate_chunk, split_chunks_by_parallelism

@pytest.fixture(scope='session')
def offset_range(kafka_partition_count):
    offset_range = {p: (p * 100, p * 200 + 100) for p in range(0, kafka_partition_count)}
    return offset_range

def test_chunk(offset_range, kafka_partition_count):
    cid = 0
    for chunks in generate_chunk(offset_range, 100):
        assert len(chunks) == kafka_partition_count - cid
        for (p, s, e) in chunks:
            assert s == (p + cid) * 100
            assert e == s + 100
        cid = cid + 1

def test_not_chunk(offset_range, kafka_partition_count):
    cid = 0
    chunks = generate_chunk(offset_range, 0).next()
    assert len(chunks) == kafka_partition_count
    for (p, s, e) in chunks:
        assert s == p * 100
        assert e == p * 200 + 100

def test_split():
    chunks = [(0, 0, 10), (2, 5, 20)]
    parallelism = 5
    expect = [(0, 0, 5), (0, 5, 10), (2, 5, 13), (2, 13, 20)]
    assert split_chunks_by_parallelism(chunks, parallelism) == expect

def test_not_split():
    chunks = [(0, 0, 10), (2, 5, 20)]
    parallelism = 0
    expect = chunks
    assert split_chunks_by_parallelism(chunks, parallelism) == expect

