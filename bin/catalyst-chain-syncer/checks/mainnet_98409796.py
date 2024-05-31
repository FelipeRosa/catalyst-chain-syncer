import asyncio
import json
import zstandard

from snapshot_checker import SnapshotChecker

MAX_SLOT = 98409796
WORKERS_COUNT = 12


async def main():
    with open("./mainnet_snapshot_tool_out-98409796.json.zst", "rb") as f:
        dec = zstandard.ZstdDecompressor()
        raw_snapshot = dec.decompress(f.read())
        snapshot_entries = json.loads(raw_snapshot)

    checker = SnapshotChecker(max_slot=MAX_SLOT, workers_count=WORKERS_COUNT)
    stats = await checker.check(snapshot_entries=snapshot_entries)

    print("Successes: {} | Failures: {}".format(stats.successes, stats.failures))


if __name__ == "__main__":
    asyncio.run(main())
