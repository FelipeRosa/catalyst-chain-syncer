import aiohttp
import asyncio
import hashlib
import json

MAX_SLOT = 56364174
WORKERS_COUNT = 12


class TestStats:
    def __init__(self):
        self.successes = 0
        self.failures = 0


async def worker(queue: asyncio.Queue) -> TestStats:
    stats = TestStats()

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                entry = await queue.get()

                stake_public_key = entry["stake_public_key"][2:]
                stake_public_key_bytes = bytes.fromhex(stake_public_key)

                hasher = hashlib.blake2b(stake_public_key_bytes, digest_size=28)
                stake_credential = hasher.hexdigest()

                req_url = "http://127.0.0.1:8080/api/v1/registrations?max_slot={}&stake_credential={}&allow_multi_delegations=false".format(
                    MAX_SLOT, stake_credential
                )

                async with session.get(req_url) as res:
                    try:
                        if res.status != 200:
                            raise Exception(
                                "Expected status=200 got {}".format(res.status)
                            )

                        raw_body = await res.text()
                        body = json.loads(raw_body)

                        if not isinstance(body, list):
                            raise Exception("Expected list")

                        reg_data = body[0]
                        if reg_data["voting_power"] != entry["voting_power"]:
                            raise Exception(
                                "Expected {} but got {}".format(
                                    entry["voting_power"],
                                    reg_data["voting_power"],
                                )
                            )

                        stats.successes += 1
                    except Exception as e:
                        print(
                            "Failed for stake_credential={}: {}".format(
                                stake_credential, e
                            )
                        )
                        stats.failures += 1

                queue.task_done()
    except asyncio.CancelledError:
        return stats


async def main():
    with open("snapshot_tool-56364174.json") as f:
        snapshot_contents = json.load(f)

    queue = asyncio.Queue()

    for entry in snapshot_contents:
        queue.put_nowait(entry)

    worker_tasks = []
    for _ in range(WORKERS_COUNT):
        worker_task = asyncio.create_task(worker(queue))
        worker_tasks.append(worker_task)

    await queue.join()

    for worker_task in worker_tasks:
        worker_task.cancel()

    workers_stats = await asyncio.gather(*worker_tasks)

    successes = 0
    failures = 0
    for stat in workers_stats:
        successes += stat.successes
        failures += stat.failures

    print("Successes: {} | Failures: {}".format(successes, failures))


if __name__ == "__main__":
    asyncio.run(main())
