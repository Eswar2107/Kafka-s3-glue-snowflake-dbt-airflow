{{
    config(
        materialized = 'incremental',
        incremental_strategy='append'
        )
}}


select * 
from  {{source('raw','carts')}}
{% if is_incremental() %}
where date > (select coalesce(max(date),'1999-01-01') from {{this}})
{%endif%}

