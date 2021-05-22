WITH non_dupli_reviews AS (
    SELECT 
        * 
    FROM reviews
    GROUP BY reviewid
),
                
non_dupli_genres AS (
    SELECT
        reviewid,
        GROUP_CONCAT(genre) AS concat_genre
    FROM genres
    GROUP BY reviewid
),

non_dupli_labels AS (
    SELECT
        reviewid,
        GROUP_CONCAT(label) AS concat_label
    FROM labels
    GROUP BY reviewid
),

non_dupli_years AS (
    SELECT
        reviewid,
        GROUP_CONCAT(year) AS concat_year
    FROM years
    GROUP BY reviewid
)

SELECT 
    ndr.*,
    g.concat_genre,
    l.concat_label,
    y.concat_year
FROM non_dupli_reviews ndr
LEFT JOIN non_dupli_genres g ON ndr.reviewid = g.reviewid
LEFT JOIN non_dupli_labels l ON ndr.reviewid = l.reviewid
LEFT JOIN non_dupli_years y ON ndr.reviewid = y.reviewid