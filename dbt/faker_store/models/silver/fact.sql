{{
    config(
        materialized='incremental',
        unique_key=['id','productid']
    )
}}

with carts as (

    select * 
    from {{ ref('bronze_carts') }}

    {% if is_incremental() %}
    where date >= (
        select coalesce(max(date), cast('1900-01-01' as date))
        from {{ this }}
    )
    {% endif %}

),

products as (
    select id, category, price
    from {{ ref('bronze_products') }}
),

final as (
    select 
        c.id,
        c.userid,
        c.productid,
        c.quantity,
        (c.quantity * coalesce(p.price, 0)) as total_cost,
        c.date
    from carts c
    left join products p 
        on c.productid = p.id
)

select * from final