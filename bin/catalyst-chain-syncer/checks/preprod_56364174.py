import asyncio
import json

from snapshot_checker import SnapshotChecker

MAX_SLOT = 56364174
WORKERS_COUNT = 12


async def main():
    with open("snapshot_tool-56364174.json") as f:
        snapshot_entries = json.load(f)

    snapshot_checker = SnapshotChecker(max_slot=MAX_SLOT, workers_count=WORKERS_COUNT)
    stats = await snapshot_checker.check(snapshot_entries=snapshot_entries)

    print("Successes: {} | Failures: {}".format(stats.successes, stats.failures))


if __name__ == "__main__":
    asyncio.run(main())
