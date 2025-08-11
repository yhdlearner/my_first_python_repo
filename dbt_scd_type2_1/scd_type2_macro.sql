{% macro scd2_loader_hashed_keys(
    source_relation,
    target_relation,
    key_columns,
    change_columns,
    start_date_column
) %}

-- 1. Raw source with original columns
with source_raw as (
  select
    {{ start_date_column }} as source_start_ts,
    {%- for col in key_columns %}
    {{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %},
    {%- for col in change_columns %}
    {{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
  from {{ source_relation }}
),

-- 2. Compute key_hash and record_hash via farm_fingerprint
hashed_source as (
  select
    *,
    farm_fingerprint(
      to_json_string(
        struct(
          {%- for col in key_columns %}
          {{ col }}{% if not loop.last %},{% endif %}
          {%- endfor %}
        )
      )
    ) as key_hash,
    farm_fingerprint(
      to_json_string(
        struct(
          {%- for col in change_columns %}
          {{ col }}{% if not loop.last %},{% endif %}
          {%- endfor %}
        )
      )
    ) as record_hash
  from source_raw
),

-- 3. Detect changes by comparing consecutive record_hash per key_hash
ordered_source as (
  select
    *,
    lag(record_hash) over (
      partition by key_hash
      order by source_start_ts
    ) as prev_hash
  from hashed_source
),

filtered_source as (
  select
    source_start_ts,
    key_hash,
    record_hash,
    {%- for col in key_columns %}
    {{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %},
    {%- for col in change_columns %}
    {{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
  from ordered_source
  where prev_hash is null
     or record_hash != prev_hash
),

-- 4. Exclude rows that match the current active record in target
changes as (
  select
    f.*
  from filtered_source f
  left join {{ target_relation }} tgt
    on f.key_hash = tgt.key_hash
   and tgt.effective_to = timestamp('9999-12-31 00:00:00')
  where tgt.key_hash is null
     or f.record_hash != tgt.record_hash
),

-- 5. Build version events with effective_from/to
events as (
  select
    key_hash,
    record_hash,
    {%- for col in key_columns %}
    {{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %},
    {%- for col in change_columns %}
    {{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %},
    source_start_ts as effective_from,
    coalesce(
      lead(source_start_ts) over (
        partition by key_hash
        order by source_start_ts
      ),
      timestamp('9999-12-31 00:00:00')
    ) as effective_to
  from changes
)

{% if is_incremental() %}

-- 6a. Close out the current active record at its first new event
update {{ target_relation }} as tgt
set effective_to = e.effective_from
from (
  select key_hash, min(effective_from) as effective_from
  from events
  group by key_hash
) e
where tgt.key_hash = e.key_hash
  and tgt.effective_to = timestamp('9999-12-31 00:00:00');

-- 6b. Insert each new version
insert into {{ target_relation }} (
  key_hash,
  record_hash,
  {%- for col in key_columns %} {{ col }}, {% endfor %}
  {%- for col in change_columns %} {{ col }}, {% endfor %}
  effective_from,
  effective_to
)
select
  key_hash,
  record_hash,
  {%- for col in key_columns %} {{ col }}, {% endfor %}
  {%- for col in change_columns %} {{ col }}, {% endfor %}
  effective_from,
  effective_to
from events e
left join {{ target_relation }} tgt2
  on e.key_hash = tgt2.key_hash
 and e.effective_from = tgt2.effective_from
where tgt2.key_hash is null;

{% else %}

-- Full refresh: rebuild the entire history
select
  key_hash,
  record_hash,
  {%- for col in key_columns %} {{ col }}{% if not loop.last %}, {% endif %}{% endfor %},
  {%- for col in change_columns %} {{ col }}{% if not loop.last %}, {% endif %}{% endfor %},
  effective_from,
  effective_to
from events;

{% endif %}

{% endmacro %}
