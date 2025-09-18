-- DX SQL Challenge Solution
-- Single-query, CTE-based answer targeting account 1726, snapshot 2849, factor 223
-- Filters to descendants (including roots) of snapshot squad IDs 57103 and 57101

WITH
params AS (
  SELECT
    1726::bigint AS account_id,
    2849::bigint AS snapshot_id,
    223::bigint  AS factor_id,
    1::bigint    AS default_benchmark_segment_id,
    ARRAY[57103, 57101]::bigint[] AS selected_snapshot_squad_ids
),

-- Limit to the snapshot, account, and factor of interest
base AS (
  SELECT s.id AS snapshot_id, s.account_id
  FROM snapshots s
  JOIN params p ON p.snapshot_id = s.id AND p.account_id = s.account_id
),

-- All descendant snapshot squads under the selected roots (including the roots themselves)
selected_descendants AS (
  SELECT DISTINCT ssh.descendant_id AS snapshot_squad_id
  FROM snapshot_squad_hierarchies ssh
  JOIN params p ON TRUE
  WHERE ssh.ancestor_id = ANY (p.selected_snapshot_squad_ids)
  UNION
  SELECT UNNEST(p.selected_snapshot_squad_ids) AS snapshot_squad_id
  FROM params p
),

-- Snapshot squads in scope for this snapshot and filter to selected descendants
squads_in_scope AS (
  SELECT ss.id AS snapshot_squad_id, ss.name AS snapshot_squad_name
  FROM snapshot_squads ss
  JOIN base b ON b.snapshot_id = ss.snapshot_id
  JOIN selected_descendants sd ON sd.snapshot_squad_id = ss.id
),

-- Build up to three ancestors for each squad for the display lineage
ancestors AS (
  SELECT
    ssh.descendant_id AS snapshot_squad_id,
    ssh.ancestor_id   AS ancestor_id,
    ssh.generations   AS gen
  FROM snapshot_squad_hierarchies ssh
  JOIN squads_in_scope sis ON sis.snapshot_squad_id = ssh.descendant_id
  WHERE ssh.generations BETWEEN 1 AND 3
),
lineage AS (
  SELECT a.snapshot_squad_id,
         string_agg(san.name, ' > ' ORDER BY a.gen DESC) AS parents
  FROM ancestors a
  JOIN snapshot_squads san ON san.id = a.ancestor_id
  GROUP BY a.snapshot_squad_id
),

-- Everyone assigned to the squad for the snapshot (team size)
squad_requests AS (
  SELECT sr.snapshot_squad_id, COUNT(*) AS team_size
  FROM snapshot_requests sr
  JOIN base b ON b.snapshot_id = sr.snapshot_id
  GROUP BY sr.snapshot_squad_id
),

-- Responses and factor answers (exclude NA by only counting values 1/2/3)
answers AS (
  SELECT
    sr.snapshot_squad_id,
    sri.value
  FROM snapshot_requests sr
  JOIN base b ON b.snapshot_id = sr.snapshot_id
  JOIN snapshot_responses sresp ON sresp.id = sr.snapshot_response_id
  JOIN snapshot_response_items sri ON sri.snapshot_response_id = sresp.id
  JOIN params p ON p.factor_id = sri.factor_id
  WHERE sri.value IN (1, 2, 3)
),

-- Counts of 1/2/3 and score
answer_counts AS (
  SELECT
    a.snapshot_squad_id,
    COUNT(*) FILTER (WHERE a.value = 1) AS one_count,
    COUNT(*) FILTER (WHERE a.value = 2) AS two_count,
    COUNT(*) FILTER (WHERE a.value = 3) AS three_count,
    COUNT(*)                              AS answered_count
  FROM answers a
  GROUP BY a.snapshot_squad_id
),
scores AS (
  SELECT
    ac.snapshot_squad_id,
    ac.one_count,
    ac.two_count,
    ac.three_count,
    ac.answered_count,
    CASE
      WHEN ac.answered_count = 0 THEN NULL
      ELSE ROUND(100.0 * ac.three_count::numeric / ac.answered_count::numeric, 0)
    END AS score
  FROM answer_counts ac
),

-- Factor details
factor AS (
  -- Anchor on params/base so we always have one row; enrich from snapshot_factors (if present) and factors
  SELECT p.factor_id,
         f.name AS factor_name
  FROM params p
  JOIN base b ON TRUE
  LEFT JOIN snapshot_factors sf
    ON sf.snapshot_id = b.snapshot_id
   AND sf.factor_id   = p.factor_id
  LEFT JOIN factors f
    ON f.id = p.factor_id
   AND (f.account_id = b.account_id OR f.account_id IS NULL)
),

-- Benchmark percentiles for default benchmark segment
bench AS (
  SELECT p.factor_id,
         bf.p_50::numeric AS benchmark_p50,
         bf.p_75::numeric AS benchmark_p75,
         bf.p_90::numeric AS benchmark_p90
  FROM params p
  JOIN benchmark_factors bf
    ON bf.factor_id = p.factor_id
   AND bf.benchmark_segment_id = p.default_benchmark_segment_id
)

SELECT
  sis.snapshot_squad_id,
  sis.snapshot_squad_name,
  COALESCE(l.parents, '') AS parents,
  s.score,
  COALESCE(s.one_count, 0)   AS one_count,
  COALESCE(s.two_count, 0)   AS two_count,
  COALESCE(s.three_count, 0) AS three_count,
  COALESCE(sr.team_size, 0)  AS team_size,
  f.factor_id,
  f.factor_name,
  b.benchmark_p50,
  b.benchmark_p75,
  b.benchmark_p90
FROM squads_in_scope sis
LEFT JOIN lineage l   ON l.snapshot_squad_id = sis.snapshot_squad_id
LEFT JOIN scores s    ON s.snapshot_squad_id = sis.snapshot_squad_id
LEFT JOIN squad_requests sr ON sr.snapshot_squad_id = sis.snapshot_squad_id
CROSS JOIN factor f
LEFT JOIN bench b ON b.factor_id = f.factor_id
ORDER BY sis.snapshot_squad_name;
