SELECT 
  T.date
  SUM(T.prod_price * T.prod_qty) AS ventes
FROM 
  TRANSACTION AS T
WHERE 
  T.date BETWEEN '2019-01-01' AND '2019-12-31'
GROUP BY 
  T.date
ORDER BY 
  T.date;
