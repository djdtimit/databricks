{{ config(schema='raw') }}

with source_sales_orders as (
    select * from {{ source('dbx_datasets_retail_org', 'sales_orders') }}
),
final as (
    SELECT * FROM source_sales_orders
)
SELECT *
FROM final

