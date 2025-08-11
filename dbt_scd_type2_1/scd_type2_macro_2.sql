{% macro scd2_loader(
    source_query=None,         -- optional raw SQL string
    source_relation=None,      -- optional ref() or source()
    target_relation,           -- usually `this`
    key_columns,               -- ['order_id', …]
    change_columns,            -- ['status','amount', …]
    start_date_column          -- 'start_date' or your aliased timestamp
) %}

{% if not source_query and not source_relation %}
  {{ exceptions.raise_compiler_error("Either source_query or source_relation must be provided.") }}
{% endif %}

with

-- 1. base CTE: either your custom query or a straight table ref
source_raw as (
  {% if source_query %}
    {{ source_query }}
  {% else %}
    select
      {{ start_date_column }}         as source_start_ts,
      {%- for c in key_columns %} {{ c }}{% if not loop.last %}, {% endif %}{% endfor %},
      {%- for c in change_columns %} {{ c }}{% if not loop.last %}, {% endif %}{% endfor %}
    from {{ source_relation }}
  {% endif %}
),

-- 2. hash it
hashed_source as (
  select
    *,
    farm_fingerprint(
      to_json_string(
        struct(
          {%- for c in key_columns %} {{ c }}{% if not loop.last %}, {% endif %}{% endfor %}
        )
      )
    ) as key_hash,
    farm_fingerprint(
      to_json_string(
        struct(
          {%- for c in change_columns %} {{ c }}{% if not loop.last %}, {% endif %}{% endfor %}
        )
      )
    ) as record_hash
  from source_raw
),

-- 3. drop intra-batch no-ops
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
    {%- for c in key_columns %} {{ c }}{% if not loop.last %}, {% endif %}{% endfor %},
    {%- for c in change_columns %} {{ c }}{% if not loop.last %}, {% endif %}{% endfor %}
  from ordered_source
  where prev_hash is null
     or record_hash != prev_hash
),

-- 4. compare to current “active” row in target
changes as (
  select f.*
  from filtered_source f
  left join {{ target_relation }} tgt
    on f.key_hash = tgt.key_hash
   and tgt.effective_to = timestamp('9999-12-31 00:00:00')
  where tgt.key_hash is null
     or f.record_hash != tgt.record_hash
),

-- 5. compute your SCD2 intervals
events as (
  select
    key_hash,
    record_hash,
    {%- for c in key_columns %} {{ c }}{% if not loop.last %}, {% endif %}{% endfor %},
    {%- for c in change_columns %} {{ c }}{% if not loop.last %}, {% endif %}{% endfor %},
    source_start_ts                    as effective_from,
    coalesce(
      lead(source_start_ts) over (
        partition by key_hash
        order by source_start_ts
      ),
      timestamp('9999-12-31 00:00:00')
    )                                  as effective_to
  from changes
)

{% if is_incremental() %}

-- 6a. Close out active rows on first event per key
update {{ target_relation }} as tgt
set effective_to = e.effective_from
from (
  select key_hash, min(effective_from) as effective_from
  from events
  group by key_hash
) e
where tgt.key_hash = e.key_hash
  and tgt.effective_to = timestamp('9999-12-31 00:00:00');

-- 6b. Insert all the new intervals
insert into {{ target_relation }} (
  key_hash,
  record_hash,
  {%- for c in key_columns %} {{ c }}, {% endfor %} 
  {%- for c in change_columns %} {{ c }}, {% endfor %} 
  effective_from,
  effective_to
)
select
  key_hash,
  record_hash,
  {%- for c in key_columns %} {{ c }}, {% endfor %} 
  {%- for c in change_columns %} {{ c }}, {% endfor %} 
  effective_from,
  effective_to
from events e
left join {{ target_relation }} tgt2
  on e.key_hash      = tgt2.key_hash
 and e.effective_from = tgt2.effective_from
where tgt2.key_hash is null;

{% else %}

-- full-refresh: just output all intervals
select
  key_hash,
  record_hash,
  {%- for c in key_columns %} {{ c }}{% if not loop.last %}, {% endif %}{% endfor %},
  {%- for c in change_columns %} {{ c }}{% if not loop.last %}, {% endif %}{% endfor %},
  effective_from,
  effective_to
from events;

{% endif %}

{% endmacro %}
