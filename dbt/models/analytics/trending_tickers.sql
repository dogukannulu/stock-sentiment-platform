-- Tickers with most mentions in the last hour vs the prior hour
with current_hour as (
    select
        ticker,
        count(*) as current_mentions
    from {{ ref('stg_sentiment_events') }}
    where event_time >= now() - interval '1 hour'
    group by ticker
),

prior_hour as (
    select
        ticker,
        count(*) as prior_mentions
    from {{ ref('stg_sentiment_events') }}
    where event_time between now() - interval '2 hours'
                         and now() - interval '1 hour'
    group by ticker
)

select
    c.ticker,
    c.current_mentions,
    coalesce(p.prior_mentions, 0)                                          as prior_mentions,
    c.current_mentions - coalesce(p.prior_mentions, 0)                     as mention_delta,
    case
        when coalesce(p.prior_mentions, 0) = 0 then null
        else round(
            (c.current_mentions::numeric / p.prior_mentions * 100 - 100), 1
        )
    end                                                                    as pct_change
from current_hour c
left join prior_hour p using (ticker)
order by mention_delta desc
