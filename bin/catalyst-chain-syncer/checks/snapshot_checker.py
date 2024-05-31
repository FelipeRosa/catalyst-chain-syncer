from typing import Dict, List
import aiohttp
import asyncio
import hashlib
import json


class TestStats:
    def __init__(self):
        self.successes = 0
        self.failures = 0


class SnapshotChecker:
    def __init__(self, max_slot: int, workers_count: int = 12):
        self.max_slot = max_slot
        self.workers_count = workers_count

    async def check(self, snapshot_entries: List[Dict]) -> TestStats:
        queue = asyncio.Queue()

        for entry in snapshot_entries:
            queue.put_nowait(entry)

        worker_tasks = []
        for _ in range(self.workers_count):
            worker_task = asyncio.create_task(worker(queue, self.max_slot))
            worker_tasks.append(worker_task)

        await queue.join()

        for worker_task in worker_tasks:
            worker_task.cancel()

        workers_stats = await asyncio.gather(*worker_tasks)

        stats = TestStats()
        for worker_stats in workers_stats:
            stats.successes += worker_stats.successes
            stats.failures += worker_stats.failures
        return stats


async def worker(queue: asyncio.Queue, max_slot: int) -> TestStats:
    stats = TestStats()

    # TODO: Fix this. This is supposed to set the client's timeout value but doesn't seem
    # seem to work that way.
    client_timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=client_timeout) as session:
        while True:
            try:
                entry = await queue.get()

                stake_public_key = entry["stake_public_key"][2:]

                stake_public_key_bytes = bytes.fromhex(stake_public_key)

                hasher = hashlib.blake2b(stake_public_key_bytes, digest_size=28)
                stake_credential = hasher.hexdigest()

                if len(stake_public_key) != 64:
                    print(
                        "Skipping stake_credential={}: Wrong key stake credential key size".format(
                            stake_credential
                        )
                    )
                    queue.task_done()
                    continue

                delegations = entry["delegations"]
                if isinstance(delegations, list):
                    skip = False

                    for delegation in delegations:
                        voting_key = delegation[0][2:]
                        if len(voting_key) != 64:
                            print(
                                "Skipping stake_credential={}: Wrong CIP-36 delegation voting key size".format(
                                    stake_credential
                                )
                            )
                            skip = True
                            break

                    if skip:
                        queue.task_done()
                        continue
                else:
                    voting_key = delegations[2:]
                    if len(voting_key) != 64:
                        print(
                            "Skipping stake_credential={}: Wrong CIP-15 voting key size".format(
                                stake_credential
                            )
                        )
                        queue.task_done()
                        continue

                req_url = "http://127.0.0.1:8080/api/v1/registrations?max_slot={}&stake_credential={}".format(
                    max_slot, stake_credential
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
                break

    return stats
