# -*- coding: utf-8 -*-

def generate_chunk(ranges, chunk_size):
    """Split ranges into chunks with proper size.
    Args:
        ranges (dict): ranges[partition] = (start, end)
        chunk_size (int): the max size of each chunk,
            if less than zero, it won't chunk
    Yields:
        list of (partition, start, end)
    Examples:
        >>> list(generate_chunk({0: (0, 10), 1: (5, 5), 2: (5, 20)}, 5))
        [[(0, 0, 5), (2, 5, 10)], [(0, 5, 10), (2, 10, 15)], [(2, 15, 20)]]
        >>> list(generate_chunk({0: (0, 10), 1: (5, 5), 2: (5, 20)}, -1))
        [[(0, 0, 10), (2, 5, 20)]]
    """
    if chunk_size <= 0:
        yield [(p, s, e) for p, (s, e) in ranges.iteritems()]
        return

    ends = {p: e for p, (s, e) in ranges.iteritems() if s != e}
    curs = {p: s for p, (s, e) in ranges.iteritems() if s != e}

    while ends:
        pool = []
        for partition, end in ends.items():
            x = curs[partition]
            y = x + chunk_size
            if y < end:
                pool.append((partition, x, y))
                curs[partition] = y
            else:
                pool.append((partition, x, end))
                del ends[partition]
        yield pool


def _split_single_chunk(chunk_range, split_factor):
    """Split one chunk into smaller chunks according to split_factor
    Args:
        chunk_range (tuple): (partition, start, end)
        split_factor (int): factor to split
    Returns:
        list of (partition, start, end)
    Examples:
        >>> _split_single_chunk((0, 0, 9), 4)
        [(0, 0, 3), (0, 3, 6), (0, 6, 9)]
    """
    (part, start, end) = chunk_range
    split_step_size = (end - start + split_factor - 1) // split_factor
    # split_step_size, identical to ceil((end - start) / float(split_factor))
    # use // (Floor Division) instead of / (Division)
    # to make sure the expression is identical to ceil
    split_start = range(start, end, split_step_size)
    split_end = map(lambda x: min(x + split_step_size, end), split_start)

    return zip([part] * len(split_start), split_start, split_end)


def split_chunks_by_parallelism(chunks, parallelism):
    """Split chunks into smaller chunks according to parallelism
    Args:
        chunks (list): list of (partition, start, end)
        parallelism (int): number for parallel processing
    Returns:
        list of (partition, start, end)
    Examples:
        >>> split_chunks_by_parallelism([(0, 0, 10), (2, 5, 20)], 5)
        [(0, 0, 5), (0, 5, 10), (2, 5, 13), (2, 13, 20)]
        >>> split_chunks_by_parallelism([(0, 0, 10), (2, 5, 20)], 0)
        [(0, 0, 10), (2, 5, 20)]
    """
    part_num = len(chunks)

    if parallelism <= 0 or part_num <= 0:
        return chunks

    split_factor = int(parallelism / part_num)
    chunks_list = map(lambda c: _split_single_chunk(c, split_factor), chunks)

    return [sp for ch in chunks_list for sp in ch]
