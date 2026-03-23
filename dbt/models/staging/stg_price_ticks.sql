select
    event_time,
    ticker,
    price::float                   as price,
    volume,
    date_trunc('hour', event_time) as event_hour
from price_ticks
where event_time >= now() - interval '7 days'
