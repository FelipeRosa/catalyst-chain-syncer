import hashlib
import json
from urllib.request import urlopen

MAX_SLOT = 56364174

with open("snapshot_tool-56364174.json") as f:
    snapshot_contents = json.load(f)

successes = 0
failures = 0

for entry in snapshot_contents:
    stake_public_key = entry["stake_public_key"][2:]
    stake_public_key_bytes = bytes.fromhex(stake_public_key)

    hasher = hashlib.blake2b(stake_public_key_bytes, digest_size=28)
    stake_credential = hasher.hexdigest()

    req_url = "http://127.0.0.1:8080/api/v1/registrations?max_slot={}&stake_credential={}&allow_multi_delegations=false".format(
        MAX_SLOT, stake_credential
    )

    try:
        with urlopen(req_url) as res:
            # Debug only
            # print("Requesting {}".format(req_url))

            raw_body = res.read()
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

        successes += 1
    except Exception as e:
        print("Failed for stake_credential={}: {}".format(stake_credential, e))
        failures += 1

print("Successes: {} | Failures: {}".format(successes, failures))
