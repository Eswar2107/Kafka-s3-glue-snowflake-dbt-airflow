{{
    config(
        materialized = 'incremental',
        unique_key = 'ID'
        )
}}


select * 
from  {{source('raw','products')}}

