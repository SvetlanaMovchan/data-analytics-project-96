WITH tab AS (
    SELECT
        s.visitor_id,
        s.source,
        s.medium,
        s.content,
        s.campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        to_char(s.visit_date, 'yyyy-mm-dd') AS visit_date,
        row_number()
            OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC)
        AS row_n
    FROM sessions AS s
    LEFT JOIN
        leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
    WHERE s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

total AS (
    SELECT
        to_char(campaign_date, 'yyyy-mm-dd') AS campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) AS total_cost
    FROM vk_ads
    GROUP BY
        to_char(campaign_date, 'yyyy-mm-dd'),
        utm_source,
        utm_medium,
        utm_campaign

    UNION ALL

    SELECT
        to_char(campaign_date, 'yyyy-mm-dd') AS campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) AS total_cost
    FROM ya_ads
    GROUP BY
        to_char(campaign_date, 'yyyy-mm-dd'),
        utm_source,
        utm_medium,
        utm_campaign
),

finish AS (
    SELECT
        t.visit_date,
        t.source AS utm_source,
        t.medium AS utm_medium,
        t.campaign AS utm_campaign,
        tl.total_cost,
        count(t.visitor_id) AS visitors_count,
        count(t.lead_id) AS leads_count,
        count(t.lead_id) FILTER (WHERE t.status_id = 142) AS purchases_count,
        sum(t.amount) FILTER (WHERE t.status_id = 142) AS revenue
    FROM tab AS t
    LEFT JOIN
        total AS tl
        ON
            t.visit_date = tl.campaign_date
            AND t.source = tl.utm_source
            AND t.medium = tl.utm_medium
            AND t.campaign = tl.utm_campaign
    WHERE t.row_n = 1
    GROUP BY t.visit_date, t.source, t.medium, t.campaign, tl.total_cost
)

SELECT
    f.visit_date,
    f.visitors_count,
    f.utm_source,
    f.utm_medium,
    f.utm_campaign,
    f.total_cost,
    f.leads_count,
    f.purchases_count,
    f.revenue
FROM finish AS f
ORDER BY
    f.revenue DESC NULLS LAST,
    f.visit_date ASC,
    f.visitors_count DESC,
    f.utm_source ASC,
    f.utm_medium ASC,
    f.utm_campaign ASC
LIMIT 15
