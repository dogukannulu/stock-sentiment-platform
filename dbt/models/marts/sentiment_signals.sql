with sentiment_hourly as (
    select
        event_hour,
        ticker,
        avg(sentiment_score)                            as avg_sentiment,
        count(*)                                        as post_count,
        sum(case when sentiment_label = 'positive'
                 then 1 else 0 end)                     as positive_count,
        sum(case when sentiment_label = 'negative'
                 then 1 else 0 end)                     as negative_count,
        sum(case when sentiment_label = 'neutral'
                 then 1 else 0 end)                     as neutral_count
    from {{ ref('stg_sentiment_events') }}
    group by 1, 2
),

price_hourly as (
    select
        event_hour,
        ticker,
        avg(price)  as avg_price,
        min(price)  as min_price,
        max(price)  as max_price,
        sum(volume) as total_volume
    from {{ ref('stg_price_ticks') }}
    group by 1, 2
)

select
    coalesce(s.event_hour, p.event_hour) as event_hour,
    coalesce(s.ticker, p.ticker)         as ticker,
    s.avg_sentiment,
    s.post_count,
    s.positive_count,
    s.negative_count,
    s.neutral_count,
    p.avg_price,
    p.min_price,
    p.max_price,
    p.total_volume,
    case
        when s.avg_sentiment >  0.3 then 'bullish'
        when s.avg_sentiment < -0.3 then 'bearish'
        else 'neutral'
    end                                  as signal
from sentiment_hourly s
full outer join price_hourly p
    on  s.event_hour = p.event_hour
    and s.ticker     = p.ticker
