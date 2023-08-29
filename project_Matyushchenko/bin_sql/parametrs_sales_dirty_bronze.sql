DELETE FROM `{{ params.project }}.bronze.sales_dirty`
WHERE DATE(_logical_dt) = "{{ ds }}"
;

INSERT `{{ params.project }}.bronze.sales_dirty` (
    CustomerId,
    PurchaseDate,
    Product,
    Price,

    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    CustomerId,
    PurchaseDate,
    Product,
    Price,

    GENERATE_UUID() AS _id,
    CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS _logical_dt,
    CAST('{{ dag_run.start_date }}' AS TIMESTAMP) AS _job_start_dt
FROM sales_csv
;