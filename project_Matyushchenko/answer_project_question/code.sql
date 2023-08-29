SELECT
    up.state AS states_of_america,
    COUNT(*) AS TV
FROM
    gold.user_profiles_enriched AS up
JOIN
    silver.sales AS c ON up.client_id = c.client_id
WHERE
    c.purchase_date BETWEEN DATE(2022,09,01) AND DATE(2022,09,10)
    AND DATE_DIFF(c.purchase_date, up.birth_date, YEAR) BETWEEN 20 AND 30
    AND c.product = 'TV'
GROUP BY
    states_of_america
ORDER BY
    TV DESC
LIMIT 6;
