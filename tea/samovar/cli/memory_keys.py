import redis
import argparse

def get_memory_usage(pattern, host, port):
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

    print(f"Total memory used by keys matching '{pattern}': {total_memory} bytes")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate Redis memory usage for keys matching a pattern.")
    parser.add_argument("pattern", type=str, help="Key pattern (e.g., 'a*')")
    parser.add_argument("host", type=str, help="redis host")
    parser.add_argument("port", type=int, help="redis port")
    args = parser.parse_args()

    get_memory_usage(args.pattern, args.host, args.port)
