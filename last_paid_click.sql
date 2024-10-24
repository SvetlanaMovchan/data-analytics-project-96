WITH tab AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source,
        s.medium,
        s.campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        row_number()
            OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC)
        AS row_n
    FROM sessions AS s
    LEFT JOIN
        leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
    WHERE s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
)

SELECT
    visitor_id,
    visit_date,
    source,
    medium,
    campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
FROM tab
WHERE
    row_n = 1
ORDER BY
    amount DESC NULLS LAST, visit_date ASC, source ASC, medium ASC, campaign ASC
LIMIT 10
