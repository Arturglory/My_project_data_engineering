MERGE INTO project-matyushchenko-arthur.silver.customers AS target
USING (
  SELECT
    CAST(Id AS INTEGER) as Id,
    FirstName,
    LastName,
    Email,
    PARSE_DATE('%Y-%m-%d', RegistrationDate) as RegistrationDate,
    State,
    _id,
    _logical_dt,
    _job_start_dt,
  FROM project-matyushchenko-arthur.bronze.customers_dirty where RegistrationDate = '{{ dag_run.logical_date.strftime("%Y-%m-%-d") }}'
) AS source
ON target.client_id = source.Id
WHEN NOT MATCHED THEN
  INSERT (
    client_id,
    first_name,
    last_name,
    email,
    registration_date,
    state,
    _id,
    _logical_dt,
    _job_start_dt
  ) VALUES (
    source.Id,
    source.FirstName,
    source.LastName,
    source.Email,
    source.RegistrationDate,
    source.State,
    source._id,
    source._logical_dt,
    source._job_start_dt
  );

