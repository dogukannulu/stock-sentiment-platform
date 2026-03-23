-- Hourly price change vs sentiment — used by the AI analyst agent for briefs
with prices as (
    select
        event_hour                                                     as hour_bucket,
        ticker,
        first_value(price) over (
            partition by ticker, event_hour
            order by event_time
            rows between unbounded preceding and unbounded following
        )                                                              as open_price,
        last_value(price) over (
            partition by ticker, event_hour
            order by event_time
            rows between unbounded preceding and unbounded following
        )                                                              as close_price,
        max(price) over (
            partition by ticker, event_hour
        )                                                              as high_price,
        min(price) over (
            partition by ticker, event_hour
        )                                                              as low_price
    from {{ ref('stg_price_ticks') }}
),

prices_deduped as (
    select distinct
        hour_bucket, ticker, open_price, close_price, high_price, low_price
    from prices
),

sentiment as (
    select
        event_hour                          as hour_bucket,
        ticker,
        round(avg(sentiment_score)::numeric, 4) as avg_sentiment,
        count(*)                            as mention_count
    from {{ ref('stg_sentiment_events') }}
    group by event_hour, ticker
)

select
    p.hour_bucket,
    p.ticker,
    round(p.open_price::numeric, 4)        as open_price,
    round(p.close_price::numeric, 4)       as close_price,
    round(p.high_price::numeric, 4)        as high_price,
    round(p.low_price::numeric, 4)         as low_price,
    round(
        ((p.close_price - p.open_price) / nullif(p.open_price, 0) * 100)::numeric, 3
    )                                      as price_change_pct,
    s.avg_sentiment,
    coalesce(s.mention_count, 0)           as mention_count
from prices_deduped p
left join sentiment s
       on p.hour_bucket = s.hour_bucket
      and p.ticker      = s.ticker
where p.open_price is not null
order by p.hour_bucket desc, p.ticker
