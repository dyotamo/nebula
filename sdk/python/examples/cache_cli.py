import argparse
import os
import ssl
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nebulacache import NebulaCache, NebulaCacheError


def non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("TTL must be an integer") from error
    if not 0 <= parsed <= 0xFFFFFFFFFFFFFFFF:
        raise argparse.ArgumentTypeError("TTL must be a non-negative 64-bit integer")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="nebulacache-example")
    parser.add_argument(
        "--address",
        default=os.environ.get("NEBULA_ADDR", "127.0.0.1:17171"),
        help="NebulaCache node address (default: %(default)s)",
    )
    parser.add_argument(
        "--tls-ca",
        default=os.environ.get("NEBULA_TLS_CA"),
        help="PEM CA file; enables TLS (default: NEBULA_TLS_CA)",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("ping", help="check node connectivity")

    set_command = commands.add_parser("set", help="store a text value")
    set_command.add_argument("key")
    set_command.add_argument("value")
    set_command.add_argument("--ttl", type=non_negative_int, default=0)

    get_command = commands.add_parser("get", help="retrieve a value")
    get_command.add_argument("key")

    delete_command = commands.add_parser("delete", help="delete a value")
    delete_command.add_argument("key")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    cache: NebulaCache | None = None

    try:
        ssl_context = ssl.create_default_context(cafile=args.tls_ca) if args.tls_ca else None
        cache = NebulaCache(
            args.address,
            min_pool_size=1,
            max_pool_size=10,
            ssl_context=ssl_context,
        )
        if args.command == "ping":
            cache.ping()
            print("PONG")
        elif args.command == "set":
            cache.set(args.key, args.value, ttl_secs=args.ttl)
            print("stored")
        elif args.command == "get":
            value = cache.get(args.key)
            if value is None:
                print("not found")
                return 1
            try:
                print(value.decode("utf-8"))
            except UnicodeDecodeError:
                print(value.hex())
        elif args.command == "delete":
            print("deleted" if cache.delete(args.key) else "not found")
        return 0
    except (NebulaCacheError, OSError, ValueError) as error:
        print(f"nebulacache-example: {error}", file=sys.stderr)
        return 1
    finally:
        if cache is not None:
            cache.close()


if __name__ == "__main__":
    raise SystemExit(main())
