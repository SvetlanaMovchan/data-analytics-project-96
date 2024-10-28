WITH tab AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source,
        s.medium,
        s.content,
        s.campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM sessions AS s
    LEFT JOIN
        leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
    WHERE s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

total AS (
    SELECT
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        sum(daily_spent) AS total_cost
    FROM vk_ads
    GROUP BY campaign_date, utm_source, utm_medium, utm_campaign, utm_content

    UNION ALL

    SELECT
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        sum(daily_spent) AS total_cost
    FROM ya_ads
    GROUP BY campaign_date, utm_source, utm_medium, utm_campaign, utm_content
)

SELECT
    source,
    medium,
    campaign,
    total_cost,
    to_char(visit_date, 'dd-mm-yyyy') AS visit_date,
    count(visitor_id) AS visitors_count,
    count(lead_id) AS leads_count,
    count(lead_id) FILTER (WHERE status_id = 142) AS purchases_count,
    sum(amount) AS revenue
FROM tab AS t
LEFT JOIN
    total AS tl
    ON
        t.visit_date = tl.campaign_date
        AND t.source = tl.utm_source
        AND t.medium = tl.utm_medium
        AND t.campaign = tl.utm_campaign
GROUP BY to_char(visit_date, 'dd-mm-yyyy'), source, medium, campaign, total_cost
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    source ASC,
    medium ASC,
    campaign ASC
LIMIT 15
