{{--
  Model: SCD Type 2 on BigQuery with intra-batch change filtering
  - Only loads events where the FARM_FINGERPRINT actually changes
  - Handles multiple source records per key by collapsing unchanged “no-ops”
  - Uses source.start_date as effective_from
  - Closes each version at the next change’s timestamp
  - Latest version has effective_to = '9999-12-31 00:00:00'
--}}

{{ config(
  materialized='incremental',
  unique_key=['order_id','effective_from'],
  partition_by={'field': 'effective_from', 'data_type': 'timestamp'},
  clustering=['order_id'],
  incremental_strategy='merge'
) }}

with source_raw as (

  -- 1. Pull in all incoming events
  select
    order_id,
    customer_id,
    order_status,
    order_total,
    start_date            as source_start_ts
  from {{ ref('raw_orders') }}

),

hashed_source as (

  -- 2. Compute fingerprint over non-key business columns
  select
    *,
    farm_fingerprint(
      to_json_string(
        struct(customer_id, order_status, order_total)
      )
    )                     as record_hash
  from source_raw

),

ordered_source as (

  -- 3. Order events per key and compare to the prior hash
  select
    *,
    lag(record_hash) over (
      partition by order_id
      order by source_start_ts
    )                     as prev_hash
  from hashed_source

),

filtered_source as (

  -- 4. Keep only first event per key or when the hash actually changes
  select
    order_id,
    customer_id,
    order_status,
    order_total,
    source_start_ts,
    record_hash
  from ordered_source
  where prev_hash is null
     or record_hash != prev_hash

),

changes as (

  -- 5. Compare filtered events to the current “active” record in target
  select
    f.order_id,
    f.customer_id,
    f.order_status,
    f.order_total,
    f.source_start_ts,
    f.record_hash
  from filtered_source f

  left join {{ this }} tgt
    on f.order_id = tgt.order_id
   and tgt.effective_to = timestamp('9999-12-31 00:00:00')

  where
    -- new key entirely, or new hash vs. current active
    tgt.order_id is null
    or f.record_hash != tgt.record_hash

),

events as (

  -- 6. Assign each change its closing timestamp (next change or 9999-12-31)
  select
    order_id,
    customer_id,
    order_status,
    order_total,
    record_hash,
    source_start_ts                          as effective_from,
    coalesce(
      lead(source_start_ts) over (
        partition by order_id
        order by source_start_ts
      ),
      timestamp('9999-12-31 00:00:00')
    )                                        as effective_to
  from changes

)

{% if is_incremental() %}

-- 7a. Close out existing active rows at their first change point
update `{{ this }}` as tgt
set
  effective_to = e.effective_from
from (
  select order_id, min(effective_from) as effective_from
  from events
  group by order_id
) as e
where
  tgt.order_id = e.order_id
  and tgt.effective_to = timestamp('9999-12-31 00:00:00');

-- 7b. Insert every new version interval
insert into `{{ this }}` (
  order_id,
  customer_id,
  order_status,
  order_total,
  record_hash,
  effective_from,
  effective_to
)
select
  order_id,
  customer_id,
  order_status,
  order_total,
  record_hash,
  effective_from,
  effective_to
from events e

-- guard against re-insertion if the same interval already exists
left join `{{ this }}` tgt
  on tgt.order_id      = e.order_id
 and tgt.effective_from = e.effective_from
where
  tgt.order_id is null;

{% else %}

-- 8. Full-refresh: rebuild end-to-end history
with all_changes as (

  select
    *,
    lag(record_hash) over (
      partition by order_id
      order by source_start_ts
    )                     as prev_hash
  from hashed_source

), true_changes as (

  select
    order_id,
    customer_id,
    order_status,
    order_total,
    source_start_ts,
    record_hash
  from all_changes
  where prev_hash is null
     or record_hash != prev_hash

), sequenced as (

  select
    *,
    lead(source_start_ts) over (
      partition by order_id
      order by source_start_ts
    )                   as next_start_ts
  from true_changes

)

select
  order_id,
  customer_id,
  order_status,
  order_total,
  record_hash,
  source_start_ts      as effective_from,
  coalesce(next_start_ts, timestamp('9999-12-31 00:00:00')) as effective_to
from sequenced;

{% endif %}
