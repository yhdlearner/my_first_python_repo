Consider yourself as expert in dbt and big query. Create a production ready model in dbt with below information
1. Target bigquery table is inserted or updated using scd type 2 logic based on business key
2. Source table has updated or new records with start_date as changed or insert timestamp
3. Source table can have multiple changes for a key
4. Target table scd type 2 begin timestamp will be start_date of the source table record 
5. Target table scd type 2 end timestamp will be 9999-12-31 00:00:00 for latest record and previous start_date for the new record
6. Farm fingerprint hash value should be calculated for set of non key columns and used for comparison to identify change in records for matching keys.
7. If the hash key does not match those records should be ignored



How It Works
source_raw selects all incoming rows and aliases the start_date as source_start_ts.

hashed_source computes a fingerprint of only the change columns.

ordered_source and filtered_source drop consecutive duplicates per key.

changes excludes records matching the current active row in the target.

events assigns each record its effective_to by looking at the next source_start_ts (or “9999-12-31” for the last one).

In incremental mode, we first close out the active row at its first change timestamp, then insert every new interval.

On a full-refresh, we simply select events to rebuild the history.