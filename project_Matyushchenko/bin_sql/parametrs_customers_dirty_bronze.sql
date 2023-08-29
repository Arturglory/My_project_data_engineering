DELETE FROM `{{ params.project }}.bronze.customers_dirty`
WHERE DATE(_logical_dt) = "{{ ds }}"
;

INSERT `{{ params.project }}.bronze.customers_dirty` (
    Id,
    FirstName,
    LastName,
    Email,
    RegistrationDate,
    State,

    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    Id,
    FirstName,
    LastName,
    Email,
    RegistrationDate,
    State,

    GENERATE_UUID() AS _id,
    CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS _logical_dt,
    CAST('{{ dag_run.start_date }}' AS TIMESTAMP) AS _job_start_dt
FROM customers_csv
;