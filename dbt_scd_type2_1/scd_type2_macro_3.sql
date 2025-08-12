{% macro scd2_loader_hashed_keys(
    source_query=None,
    source_relation=None,
    target_relation,
    key_columns,
    change_columns,
    start_date_column,
    current_ts_expr="CURRENT_TIMESTAMP()",
    end_of_time_ts_expr="TIMESTAMP('9999-12-31 00:00:00')"
) %}

{% if not source_query and not source_relation %}
  {{ exceptions.raise_compiler_error("Either source_query or source_relation must be provided.") }}
{% endif %}

with source_raw as (
  {% if source_query %}
    {{ source_query }}
  {% else %}
    select
      {{ start_date_column }} as source_start_ts,
      {%- for col in key_columns %} {{ col }}{% if not loop.last %}, {% endif %}{% endfor %},
      {%- for col in change_columns %} {{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
    from {{ source_relation }}
  {% endif %}
),

hashed_source as (
  select
    *,
    farm_fingerprint(concat(
      {%- for col in key_columns %}
        coalesce(cast({{ col }} as string), ''){% if not loop.last %} || '|' || {% endif %}
      {%- endfor %}
    )) as key_hash,
    farm_fingerprint(concat(
      {%- for col in change_columns %}
        coalesce(cast({{ col }} as string), ''){% if not loop.last %} || '|' || {% endif %}
      {%- endfor %}
    )) as record_hash
  from source_raw
),

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
    {%- for col in key_columns %} {{ col }}{% if not loop.last %}, {% endif %}{% endfor %},
    {%- for col in change_columns %} {{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
  from ordered_source
  where prev_hash is null or record_hash != prev_hash
),

changes as (
  select f.*
  from filtered_source f
  left join {{ target_relation }} tgt
    on f.key_hash = tgt.key_hash
   and tgt.effective_to = {{ end_of_time_ts_expr }}
  where tgt.key_hash is null or f.record_hash != tgt.record_hash
),

events as (
  select
    key_hash,
    record_hash,
    {%- for col in key_columns %} {{ col }}{% if not loop.last %}, {% endif %}{% endfor %},
    {%- for col in change_columns %} {{ col }}{% if not loop.last %}, {% endif %}{% endfor %},
    source_start_ts as effective_from,
    coalesce(
      lead(source_start_ts) over (
        partition by key_hash
        order by source_start_ts
      ),
      {{ end_of_time_ts_expr }}
    ) as effective_to
  from changes
)

{% if is_incremental() %}

-- 1. Close out active rows
update {{ target_relation }} as tgt
set
  effective_to = e.effective_from,
  record_effective_end_timestamp = {{ current_ts_expr }},
  current_record_ind = 0
from (
  select key_hash, min(effective_from) as effective_from
  from events
  group by key_hash
) e
where tgt.key_hash = e.key_hash
  and tgt.effective_to = {{ end_of_time_ts_expr }};

-- 2. Insert new versions
insert into {{ target_relation }} (
  key_hash,
  record_hash,
  {%- for col in key_columns %} {{ col }}, {% endfor %}
  {%- for col in change_columns %} {{ col }}, {% endfor %}
  effective_from,
  effective_to,
  record_effective_begin_timestamp,
  record_effective_end_timestamp,
  current_record_ind
)
select
  key_hash,
  record_hash,
  {%- for col in key_columns %} {{ col }}, {% endfor %}
  {%- for col in change_columns %} {{ col }}, {% endfor %}
  effective_from,
  effective_to,
  {{ current_ts_expr }},
  case
    when effective_to = {{ end_of_time_ts_expr }} then {{ end_of_time_ts_expr }}
    else {{ current_ts_expr }}
  end as record_effective_end_timestamp,
  case
    when effective_to = {{ end_of_time_ts_expr }} then 1
    else 0
  end as current_record_ind
from events e
left join {{ target_relation }} tgt2
  on e.key_hash = tgt2.key_hash
 and e.effective_from = tgt2.effective_from
where tgt2.key_hash is null;

{% else %}

-- Full refresh
select
  key_hash,
  record_hash,
  {%- for col in key_columns %} {{ col }}, {% endfor %}
  {%- for col in change_columns %} {{ col }}, {% endfor %}
  effective_from,
  effective_to,
  {{ current_ts_expr }} as record_effective_begin_timestamp,
  case
    when effective_to = {{ end_of_time_ts_expr }} then {{ end_of_time_ts_expr }}
    else {{ current_ts_expr }}
  end as record_effective_end_timestamp,
  case
    when effective_to = {{ end_of_time_ts_expr }} then 1
    else 0
  end as current_record_ind
from events;

{% endif %}

{% endmacro %}
