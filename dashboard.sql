--Уникальные пользователи за весь период (visitors_count)
SELECT count(DISTINCT visitor_id) AS visitors_count
FROM sessions;

-- Сколько лидов приходят (count_leads)
SELECT count(DISTINCT lead_id) AS leads_count
FROM leads;

-- Сколько уникальных пользователей и лидов, которые заходят на сайт по платным каналам (conversion)
-- Конверсия из клика в лид и из лида в оплату расчитывается непосредственно в самом дашборде 
-- SUM(leads_count)::NUMERIC / SUM(visitors_count) 
-- SUM(leads_paid_count)::NUMERIC / SUM(leads_count) 
WITH tab AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source,
        s.medium,
        s.campaign,
        l.lead_id,
        l.created_at,
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
    to_char(visit_date, 'yyyy-mm-dd')::date AS visit_day,
    source,
    medium,
    campaign,
    count(DISTINCT visitor_id) AS visitors_count,
    count(DISTINCT lead_id) AS leads_count,
    count(DISTINCT lead_id) FILTER (WHERE status_id = 142) AS leads_paid_count
FROM tab
WHERE row_n = 1
GROUP BY to_char(visit_date, 'yyyy-mm-dd')::date, source, medium, campaign;

-- Какие каналы приводят на сайт пользователей? Хочется видеть по дням/неделям/месяцам
-- Количество посещений на сайте по дням (visits_count_for_day)
SELECT
    to_char(visit_date, 'yyyy-mm-dd')::date AS visit_day,
    source,
    medium,
    campaign,
    count(DISTINCT visitor_id) AS visitors_count
FROM sessions
GROUP BY to_char(visit_date, 'yyyy-mm-dd')::date, source, medium, campaign
ORDER BY visit_day ASC, visitors_count DESC;

-- Количество посещений на сайте с разбивкой по каналам - платным (visits_count_source_no_organic)
SELECT
    to_char(visit_date, 'yyyy-mm-dd')::date AS visit_day,
    source,
    medium,
    campaign,
    count(DISTINCT visitor_id) AS visitors_count
FROM sessions
WHERE medium != 'organic'
GROUP BY to_char(visit_date, 'yyyy-mm-dd')::date, source, medium, campaign
ORDER BY visit_day ASC, visitors_count DESC;

--Сколько мы тратим по разным каналам в динамике? (cost)
--Окупаются ли каналы?  расчитывается непосредственно в самом дашборде
--cpu затраты на одного клиента = total_cost/visitors_count 
--cpl затраты на лида = total_cost / leads_count
--cppu затраты на платящего лида = total_cost / purchases_count
--roi = (revenue - total_cost) / total_cost * 100%
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
        to_char(s.visit_date, 'yyyy-mm-dd')::date AS visit_date,
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
        to_char(campaign_date, 'yyyy-mm-dd')::date AS campaign_date,
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
        to_char(campaign_date, 'yyyy-mm-dd')::date AS campaign_date,
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
)

SELECT
    visit_date::date,
    source,
    medium,
    campaign,
    total_cost,
    count(visitor_id) AS visitors_count,
    count(lead_id) AS leads_count,
    count(lead_id) FILTER (WHERE status_id = 142) AS purchases_count,
    sum(amount) FILTER (WHERE status_id = 142) AS revenue
FROM tab AS t
LEFT JOIN
    total AS tl
    ON
        t.visit_date = tl.campaign_date
        AND t.source = tl.utm_source
        AND t.medium = tl.utm_medium
        AND t.campaign = tl.utm_campaign
WHERE row_n = 1
GROUP BY visit_date, source, medium, campaign, total_cost
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    source ASC,
    medium ASC,
    campaign ASC;

--расчет корреляции (correl)
    WITH organic AS (
    SELECT
        to_char(visit_date, 'yyyy-mm-dd')::date AS visit_day,
        count(DISTINCT visitor_id) AS organic_visitors_count
    FROM sessions
    WHERE medium = 'organic'
    GROUP BY to_char(visit_date, 'yyyy-mm-dd')::date
),

paid AS (
    SELECT
        to_char(visit_date, 'yyyy-mm-dd')::date AS visit_day,
        count(DISTINCT visitor_id) AS paid_visitors_count
    FROM sessions
    WHERE medium != 'organic'
    GROUP BY to_char(visit_date, 'yyyy-mm-dd')::date
)

SELECT
    o.visit_day,
    o.organic_visitors_count,
    p.paid_visitors_count
FROM organic AS o
LEFT JOIN paid AS p ON o.visit_day = p.visit_day
ORDER BY o.visit_day;

--Посадочные страницы, которые наиболее часто посещают пользователи, пришедшие из рекламы (landing)
WITH tab AS (
    SELECT
        s.visitor_id,
        s.source,
        s.medium,
        s.campaign,
        s.landing_page,
        l.lead_id,
        l.created_at,
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
    landing_page AS full_url,
    concat(left(landing_page, 25), '...') AS landing_page,
    count(DISTINCT visitor_id) AS visitors_count,
    count(DISTINCT lead_id) AS leads_count,
    count(DISTINCT lead_id) FILTER (WHERE status_id = 142) AS leads_paid_count,
    count(DISTINCT lead_id)::NUMERIC
    / count(DISTINCT visitor_id)
    * 100.00 AS conver
FROM tab
WHERE row_n = 1
GROUP BY landing_page
HAVING
    count(DISTINCT lead_id)::NUMERIC / count(DISTINCT visitor_id) * 100.00 > 0
ORDER BY visitors_count DESC
LIMIT 10;
