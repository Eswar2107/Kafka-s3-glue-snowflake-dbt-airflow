{{
    config(
        materialized = 'incremental',
        unique_key = 'ID'
        )
}}


select * 
from  {{source('raw','users')}}
{% if is_incremental() %}
where date>=(select coalesce(max(date),'1900-01-01') from {{this}} )
{%endif%}
