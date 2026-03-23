-- 5-minute windowed sentiment aggregation per ticker
-- Primary table queried by the AI Market Analyst Agent
with base as (
    select
        date_trunc('minute', event_time) -
            (extract(minute from event_time)::int % 5 * interval '1 minute') as five_min_bucket,
        ticker,
        sentiment_score,
        sentiment_label
    from {{ ref('stg_sentiment_events') }}
)

select
    five_min_bucket                                                    as bucket,
    ticker,
    round(avg(sentiment_score)::numeric, 4)                           as avg_sentiment_score,
    count(*)                                                           as mention_count,
    sum(case when sentiment_label = 'positive' then 1 else 0 end)     as positive_count,
    sum(case when sentiment_label = 'negative' then 1 else 0 end)     as negative_count,
    sum(case when sentiment_label = 'neutral'  then 1 else 0 end)     as neutral_count,
    round(avg(sentiment_score)::numeric, 4)
        - lag(round(avg(sentiment_score)::numeric, 4))
          over (partition by ticker order by five_min_bucket)          as sentiment_momentum
from base
group by five_min_bucket, ticker
order by five_min_bucket desc, mention_count desc
