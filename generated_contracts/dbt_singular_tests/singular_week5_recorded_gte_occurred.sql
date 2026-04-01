-- Singular test: temporal invariant (dbt). Point ref() at your staging model name.
select event_id
from {{ ref('week5_event_records') }}
where recorded_at < occurred_at
