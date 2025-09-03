import argparse
import lz4.block
import redis
import lz4
from enum import Enum
import tea.samovar.proto.samovar_pb2 as samovar_proto
import struct
import lz4.block

class MetaType(Enum):
    QUEUE = 1
    META = 2
    FILE_LIST = 3
    CHECKPOINT_CELL = 4

metadata_prefix = "/samovar_meta"
file_list_prefix = "/file_list"
checkpoint_prefix = "/checkpoint"

def classify_query(cell : str) -> MetaType:
    if cell[:len(metadata_prefix)] == metadata_prefix:
        return MetaType.META
    if cell[:len(file_list_prefix)] == file_list_prefix:
        return MetaType.FILE_LIST
    if cell[:len(checkpoint_prefix)] == checkpoint_prefix:
        return MetaType.CHECKPOINT_CELL
    return MetaType.QUEUE

def stringify_type(type : MetaType) -> str:
    if type == MetaType.QUEUE:
        return "queue"
    if type == MetaType.META:
        return "meta"
    if type == MetaType.CHECKPOINT_CELL:
        return "checkpoint"
    if type == MetaType.FILE_LIST:
        return "file_list"

def get_all_redis_keys(client : redis.Redis, pattern : str) -> list[str]:
    cursor = 0
    keys = []
    while True:
        cursor, batch = client.scan(cursor, match=pattern, count=100)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys

def get_memory_usage(pattern : str, host : str, port : int) -> int:
    r = redis.Redis(host=host, port=port, decode_responses=True)

    cursor = 0
    total_memory = 0

    while True:
        cursor, keys = r.scan(cursor, match=pattern, count=100)
        for key in keys:
            size = r.memory_usage(key) or 0
            total_memory += size
        if cursor == 0:
            break
    return total_memory

def get_queue_len(queue : str, host : str, port :str) -> int:
    client = redis.Redis(host=host, port=port, decode_responses=True)
    return client.llen(queue)
    
def decode_meta(cell : str, host : str, port :str):
    if classify_query(cell) != MetaType.META:
        raise ValueError(f"Incorrect meta type {stringify_type(classify_query(cell))}")

    client = redis.Redis(host=host, port=port, decode_responses=False)
    proto_representation = client.get(cell)
    print(proto_representation)
    new_scan_metadata = samovar_proto.ScanMetadata()
    new_scan_metadata.ParseFromString(proto_representation)
    print(new_scan_metadata)

def decode_queue(cell : str, host : str, port : str):
    if classify_query(cell) != MetaType.QUEUE:
        raise ValueError(f"Incorrect meta type {stringify_type(classify_query(cell))}")
    client = redis.Redis(host=host, port=port, decode_responses=False)
    items = []
    while True:
        last_item = client.rpop(cell)
        if not last_item:
            break    
        items.append(last_item)

    for i, item in enumerate(items):
        data_entry = samovar_proto.AnnotatedDataEntry()
        data_entry.ParseFromString(item)
        print(f"[{i}/{len(items)}] : {data_entry}, layer_id: {data_entry.layer_id}, partition_id: {data_entry.partition_id}")

        client.lpush(cell, item)

def decompress_arrow_lz4(compressed_data: bytes) -> bytes:
    original_size = struct.unpack("<q", compressed_data[:8])[0]
    compressed_payload = compressed_data[8:] 

    decompressed_data = lz4.block.decompress(compressed_payload, uncompressed_size=original_size)
    return decompressed_data

def decode_file_list(cell : str, host : str, port : str):
    if classify_query(cell) != MetaType.FILE_LIST:
        raise ValueError(f"Incorrect meta type {stringify_type(classify_query(cell))}")

    client = redis.Redis(host=host, port=port, decode_responses=False)
    file_list_str = client.get(cell)
    file_list_str = decompress_arrow_lz4(file_list_str)
    file_list_decoded_proto = samovar_proto.FileList()
    file_list_decoded_proto.ParseFromString(file_list_str)
    print(file_list_decoded_proto)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI for samovar.")
    parser.add_argument("--host", default="0.0.0.0", type=str, help="redis host")
    parser.add_argument("--port", default=6379, type=int, help="redis port")

    subparsers = parser.add_subparsers(dest="mode", required=True, help="Choose mode.")

    parser_listing = subparsers.add_parser("list", help="mode for listing keys.")
    parser_listing.add_argument("--pattern", type=str, default="*", help="mode for listing keys.")

    parser_search = subparsers.add_parser("search", help="mode search related cells for query")
    parser_search.add_argument("--cluster_id", type=str, help="pattern to select keys.")
    parser_search.add_argument("--table_id", type=str, help="pattern to select keys.")
    parser_search.add_argument("--session_id", type=str, default="", help="pattern to select keys.")
    parser_search.add_argument("--limit", type=int, default=0, help="Limit of listed files. 0 means no limits.")

    parser_meta = subparsers.add_parser("memory", help="Parse memory count content.")
    parser_meta.add_argument("--cluster_id", type=str, default="", help="pattern to select keys.")
    parser_meta.add_argument("--table_id", type=str, default="", help="pattern to select keys.")
    parser_meta.add_argument("--session_id", type=str, default="", help="pattern to select keys.")

    parser_meta = subparsers.add_parser("queue_len", help="Parse memory count content.")
    parser_meta.add_argument("--queue_id", type=str, default="", help="pattern to select keys.")

    parser_meta = subparsers.add_parser("decode_meta", help="Parse queue metadata.")
    parser_meta.add_argument("--cell_id", type=str, default="", help="pattern to select keys.")

    parser_meta = subparsers.add_parser("decode_queue", help="Parse items in queue.")
    parser_meta.add_argument("--cell_id", type=str, default="", help="pattern to select keys.")

    parser_meta = subparsers.add_parser("decode_file_list", help="Parse file list and decompress lz4.")
    parser_meta.add_argument("--cell_id", type=str, default="", help="pattern to select keys.")

    args = parser.parse_args()
    client = redis.Redis(host=args.host, port=args.port, decode_responses=True)

    if args.mode == "list":
        keys = get_all_redis_keys(client, args.pattern)
        for i, key in enumerate(keys):
            print(f"[{i}] : {key}")

    if args.mode == "search":
        pattern = f"*/{args.cluster_id}/{args.table_id}"
        if len(args.session_id) != "":
            pattern = f"{pattern}/{args.session_id}*"
        else:
            pattern = pattern + "." 
        keys = get_all_redis_keys(client, pattern)
        limit = args.limit
        if limit == 0:
            limit = len(keys)
        for i, key in enumerate(keys[:limit]):
            print(f"[{i}] : {key}")
    
    if args.mode == "memory":
        pattern = f"*/{args.cluster_id}/{args.table_id}"
        if len(args.session_id) != "":
            pattern = f"{pattern}/{args.session_id}*" 
        else:
            pattern = pattern + "." 
        keys = get_all_redis_keys(client, pattern)
        for key in keys:
            print(f"Memory usage {key}: {get_memory_usage(key, args.host, args.port)}")

    if args.mode == "queue_len":
        print(f"len queue: {get_queue_len(args.queue_id)}")

    if args.mode == "decode_meta":
        decode_meta(args.cell_id, args.host, args.port)

    if args.mode == "decode_queue":
        decode_queue(args.cell_id, args.host, args.port)

    if args.mode == "decode_file_list":
        decode_file_list(args.cell_id, args.host, args.port)
