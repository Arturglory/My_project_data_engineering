INSERT `{{ params.project }}.silver.sales` (
    client_id,
    purchase_date,
    product,
    price,
    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    CAST(CustomerId AS INTEGER) as client_id,
    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', PurchaseDate),
        SAFE.PARSE_DATE('%Y/%m/%d', PurchaseDate),
        SAFE.PARSE_DATE('%Y-%h-%d', PurchaseDate)
        ) as purchase_date,
    Product as product_name,
    CAST(REGEXP_REPLACE(Price, r'USD|\$', '') AS INTEGER) AS price,
    _id,
    _logical_dt,
    _job_start_dt
FROM `{{ params.project }}.bronze.sales_dirty`
WHERE DATE(_logical_dt) = "{{ ds }}"
;