# sql_challenge

One of the regular types of problems we solve at DX is writing SQL to generate charts or tables to visualize the data.

![Team breakdown sentiment scores](image-1.png)

## Challenge

Your challenge will be to write the SQL that would provide the data to be able to build a table like we have above **in a single query**. Would recommend using CTEs to write this query

## The data that should be output:
- Snapshot Squad ID
- Snapshot squad Name
- listed snapshot squad names of the hierarchy above the team (parent snapshot squad, grandparent snapshot squad, great-grandparent snapshot squad)
- Sentiment score for a factor. The score is calculated by the percentage of people that gave a value of 3. People who marked na should not be included in the calculation.
- the number of people on a snapshot squad who marked 1 (bad)
- the number of people on a snapshot squad who marked 2 (so-so)
- the number of people on a snapshot squad who marked 3 (good)
- the total number of people on the snapshot squad, whether they answered with a value, N/A, or did not respond
- How the snapshot squad scored compared to the benchmark_factor of the default segment (benchmark_segments.id = 1) at the 50th percentile, 75th percentile, and 90th percentile
- the factor ID
- the factor name

Using the screenshot above with the `SecOps` team, a row would look like this:

```csv
snapshot_squad_id,snapshot_squad_name,parents,score,one_count,two_count,three_count,team_size,factor_id,factor_name
55427,SecOps,Payments > Security,100,0,0,3,5,223,Local environment setup
```

You should get the above data for the Ritual V3 account (ID 1726), the latest snapshot (ID 2849), and for the factor Local environment setup (ID 223). **There should only be one row for a snapshot squad ID.**

## A brief overview of the data model:

An `account` sends out a quarterly survey, which we call a `snapshot`. The `account` has set up `squads` that define the team hierarchy. We capture and effectively freeze that hierarchy by duplicating it at the time the survey begins into `snapshot_squads`. By having the `snapshot_squads`, an `account` can change their organizational hierarchy and team structure but still view their old surveys by the team structure they had in place at the time the survey was taken. We have a couple of tables, `squad_hierarchies` and `snapshot_squad_hierarchies`, that allow you to query all of the ancestors or descendants of a particular `squad` or `snapshot squad` as well as see what generation it is.

Anyone who is a member of a `squad` when the survey begins will be assigned a `snapshot_request`. The `snapshot_request` will reference the `snapshot_squad` that corresponds to the `squad` they are a member of at the time the survey began.

When a user begins the survey, we will create a `snapshot_response` for the user. This will serve as the parent object of all of their data for a particular `snapshot`. For each `factor` they answer Good (3), So-so (2) or Bad (1) to we will create a `snapshot_response_item`. Upon completion of the survey, we will put the `snapshot_response_id` on the `snapshot_request`. 

![Survey rate example](image.png)

Last, add a `WHERE` clause for the above query. We allow end users to select teams at any level of hierarchy and that includes the teams beneath them. Notice how the selection of the Growth and Payments team includes 47 teams. Make it so that we can pass in the snapshot squad IDs of `57103` and `57101` and get just the teams that are below them -- we shouldn't have any teams that are under the `Product & Data Platform` hierarchy nor the `Data Engineering` team.

![47 teams](image-3.png)

Please reach out if you have questions regarding the requirements of the challenge or the data model is unclear.

## Database to connect to

Would recommend downloading a Database client such as Postico and connecting to the database to view the data

## Solution query overview

The single-query solution lives in `solution.sql`. It uses a series of CTEs to:

- `params`: Set the account, snapshot, factor, default benchmark segment, and the selected root snapshot squad IDs.
- `base`: Constrain all joins to the specified `snapshot` and `account`.
- `selected_descendants`: Expand the selected roots (e.g., `57103`, `57101`) to all descendant `snapshot_squad` IDs, including the roots themselves.
- `squads_in_scope`: Limit `snapshot_squads` to the current snapshot and the selected descendants.
- `ancestors`/`lineage`: Build up to three ancestor names per squad and render them as a `parents` breadcrumb string (e.g., `Payments > Security`).
- `squad_requests`: Count everyone assigned to the squad for the snapshot (`team_size`).
- `answers`/`answer_counts`/`scores`: Count 1/2/3 responses for the given factor and compute `score` as the percentage of 3s among answered (excludes N/A).
- `factor` and `bench`: Pull factor details and placeholder benchmark percentiles for the default segment.

The final SELECT returns one row per `snapshot_squad_id` with:

- `snapshot_squad_id`, `snapshot_squad_name`, `parents`
- `score`, `one_count`, `two_count`, `three_count`, `team_size`
- `factor_id`, `factor_name`, and benchmark columns (`benchmark_p50`, `benchmark_p75`, `benchmark_p90`)

Notes:

- The filter to only include teams under the selected roots is implemented in the `selected_descendants` CTE.
- You can change inputs by editing the `params` CTE at the top of `solution.sql` (account, snapshot, factor, or selected teams).

## How to run the query

You can execute the query directly with `psql` using either the connection URL or the provided environment variables.

1) Using the connection URL:

```bash
psql "postgres://u_qqrxh...." -f solution.sql
```

2) Using environment variables:

```bash
export PGHOST="p.qrl..."
export PGDATABASE="postgres"
export PGUSER="u_qqrxh..."
export PGPASSWORD="..."
export PGPORT="####"
export PGSSLMODE="require"

psql -f solution.sql
```

Optional: open the database in a GUI like Postico and run the contents of `solution.sql` in a new SQL query tab.