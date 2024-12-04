from math import log2

IEC_UNITS = ["KiB", "MiB", "GiB", "TiB"]


def size2human(size: int) -> str:
    if size == 0:
        return "0B"
    p = int(log2(size) // 10.0)

    if p < 1:
        return f"{size}B"
    if p > len(IEC_UNITS):
        p = len(IEC_UNITS)
    converted_size = size / 1024**p
    return f"{converted_size:.1f}{IEC_UNITS[p - 1]}"  # noqa
