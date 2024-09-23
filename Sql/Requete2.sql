SELECT
  T.client_id,
  SUM(CASE WHEN PN.product_type = 'MEUBLE' THEN T.prod_price * T.prod_qty ELSE 0 END) AS ventes_meuble,
  SUM(CASE WHEN PN.product_type = 'DECO' THEN T.prod_price * T.prod_qty ELSE 0 END) AS ventes_deco
FROM
  TRANSACTIONS T
JOIN
  PRODUCT_NOMENCLATURE PN ON T.prod_id = PN.product_id
WHERE
  T.date BETWEEN '2019-01-01' AND '2019-12-31'
GROUP BY
  T.client_id