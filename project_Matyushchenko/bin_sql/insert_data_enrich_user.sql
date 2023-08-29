INSERT INTO gold.user_profiles_enriched
SELECT
    c.client_id,
    IFNULL(u.state, c.state) AS state,
    REGEXP_EXTRACT(u.full_name, r'(\\w+)', 1, 1) as first_name,
    REGEXP_EXTRACT(u.full_name, r'(\\w+)', 1, 2) as last_name,
    IFNULL(u.email, c.email) AS email,
    u.birth_date,
    u.phone_number,
    c.registration_date,
    c._id,
    c._logical_dt,
    c._job_start_dt
FROM
    `silver.customers` c
LEFT JOIN
    `silver.user_profiles` u ON c.email = u.email;