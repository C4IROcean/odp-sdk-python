# Ref is a reference to a piece of data in a big file, stored separately
# example: "foo~12345:17:12"
# means that the data starts with "foo", and the full data can be fetched in bigfile 12345 at position 17, with size 12


# def encode_str(bid: str, data: str, pos: int) -> str:
#    prefix = data[0:STR_MIN]
#    size = len(data)
#    return f"{prefix}~{bid}:{pos}:{size}"
#
#
# def encode_bin(bid: str, data: bytes, pos: int) -> bytes:
#    prefix = data[0:STR_MIN]
#    size = len(data)
#    return prefix + f"~{bid}:{pos}:{size}".encode("utf-8")
#
#
# def decode_str(ref: str | None, downloader: Callable[[str], bytes]) -> Optional[str]:
#    if ref is None:
#        return None
#    prefix, ptr = ref.rsplit("~", 1)
#    if ptr == "":
#        return prefix
#    bid, pos, size = ptr.split(b":")
#    pos = int(pos)
#    size = int(size)
#    data = downloader(bid.decode("utf-8"))
#    return data[pos: pos + size].decode("utf-8")
#
#
# def decode_bin(s: bytes | None, downloader: Callable[[str], bytes]) -> Optional[bytes]:
#    if s is None:
#        return None
#    prefix, ptr = s.rsplit(b"~", 1)
#    if ptr == b"":
#        return prefix
#    bid, pos, size = ptr.split(b":")
#    pos = int(pos)
#    size = int(size)
#    data = downloader(bid.decode("utf-8"))
#    return data[pos: pos + size]
