WITH block_no_diffs AS (
    SELECT
        block_no,
        LAG (block_no) OVER (ORDER BY block_no) - block_no AS diff
   FROM cardano_blocks
)
SELECT block_no, ABS(diff) FROM block_no_diffs WHERE diff < -1
