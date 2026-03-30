{% snapshot gold_user_snapshot %}

{{
  config(
    unique_key='id',
    strategy='timestamp',
    updated_at='date',
    dbt_valid_to_current="to_date('9999-12-31')",
    target_schema='gold',
    database=target.database
  )
}}

select * 
from {{ source('raw', 'users') }}

{% endsnapshot %}