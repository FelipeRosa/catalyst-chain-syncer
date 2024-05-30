WITH total_voting_powers AS (
    SELECT
        cr.stake_credential stake_credential,
        SUM(txo.value)::BIGINT voting_power
    FROM catalyst_registrations cr

    LEFT JOIN cardano_txo txo
    ON txo.stake_credential = cr.stake_credential

    LEFT JOIN cardano_transactions txs
    ON txs.hash = txo.transaction_hash

    LEFT JOIN cardano_blocks blks
    ON blks.block_no = txs.block_no

    LEFT JOIN cardano_spent_txo stxo
    ON  stxo.from_transaction_hash = txo.transaction_hash
    AND stxo.index = txo.index

    WHERE
        (stxo.to_transaction_hash IS NULL OR $1::BIGINT IS NULL OR blks.slot_no >= $1::BIGINT)  -- UTXO
    AND ($1::BIGINT IS NULL OR blks.slot_no <= $1::BIGINT) -- INSIDE SLOT RANGE

    GROUP BY cr.stake_credential
)

-- TODO: This should decide if a voter has unregistered
SELECT DISTINCT
    cr.stake_credential,
    COALESCE(tvp.voting_power, 0)
FROM catalyst_registrations cr
LEFT JOIN total_voting_powers tvp
ON tvp.stake_credential = cr.stake_credential
WHERE
    ($2::BYTEA IS NULL OR cr.stake_credential = $2) -- STAKE CREDENTIAL FILTER
AND ($3::BOOLEAN OR (NOT $3::BOOLEAN AND ARRAY_LENGTH(cr.voting_key, 1) = 1)) -- MULTIPLE DELEGATIONS FILTER
