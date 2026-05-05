UNLOAD (
  'SELECT
     product_category,
     product_title,
     AVG(CAST(star_rating AS FLOAT)) AS avg_rating,
     SUM(helpful_votes) AS total_helpful_votes,
     COUNT(*) AS review_count
   FROM public.reviews
   GROUP BY product_category, product_title
   ORDER BY avg_rating DESC'
)
TO 's3://redshift-processed-data-us/output/topreviews_'
IAM_ROLE 'arn:aws:iam::865268032828:role/RedshiftSpectrumRole'
FORMAT AS PARQUET
ALLOWOVERWRITE
