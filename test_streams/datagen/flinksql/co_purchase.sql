SELECT p1.product_id, p2.product_id, COUNT(*)
FROM purchases p1
JOIN purchases p2
  ON p1.user_id = p2.user_id
 AND p1.product_id <> p2.product_id
GROUP BY p1.product_id, p2.product_id;
