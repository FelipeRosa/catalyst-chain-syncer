WITH txo_by_stake_credential AS (
    SELECT
        txo.stake_credential stake_credential,
        txo.value value,
        blks.slot_no slot_no,
        spent_blks.slot_no spent_slot_no
    FROM cardano_txo txo
    JOIN cardano_transactions txs
    ON txs.hash = txo.transaction_hash
    JOIN cardano_blocks blks
    ON blks.block_no = txs.block_no
    LEFT JOIN cardano_spent_txo stxo
    ON
        txo.transaction_hash = stxo.from_transaction_hash
    AND txo.index = stxo.index
    LEFT JOIN cardano_transactions spent_txs
    ON spent_txs.hash = stxo.to_transaction_hash
    LEFT JOIN cardano_blocks spent_blks
    ON spent_blks.block_no = spent_txs.block_no
),
total_voting_powers AS (
    SELECT
        cr.stake_credential stake_credential,
        SUM(txo.value)::BIGINT voting_power
    FROM (SELECT DISTINCT stake_credential FROM catalyst_registrations) cr
    JOIN txo_by_stake_credential txo
    ON txo.stake_credential = cr.stake_credential
    WHERE
        (txo.spent_slot_no IS NULL OR txo.spent_slot_no > $1::BIGINT) -- UTXO
    AND txo.slot_no <= $1::BIGINT -- IN RANGE
    GROUP BY cr.stake_credential
)

SELECT
    cr.stake_credential,
    COALESCE(tvp.voting_power, 0)
FROM (SELECT DISTINCT stake_credential FROM catalyst_registrations) cr
LEFT JOIN total_voting_powers tvp
ON tvp.stake_credential = cr.stake_credential
WHERE
    ($2::BYTEA IS NULL OR cr.stake_credential = $2) -- STAKE CREDENTIAL FILTER
