select
    event_time,
    ticker,
    source,
    sentiment_score::float         as sentiment_score,
    sentiment_label,
    raw_text,
    price_at_event::float          as price_at_event,
    metadata::jsonb                as metadata,
    date_trunc('hour', event_time) as event_hour
from sentiment_events
where event_time >= now() - interval '7 days'
