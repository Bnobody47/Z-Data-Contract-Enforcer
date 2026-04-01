-- Singular test: per-fact confidence in [0,1]. Align ref() with your dbt model.
select 1 as bad_row
from {{ ref('week3_extractions') }}
where fact_confidence is not null
  and (fact_confidence < 0 or fact_confidence > 1)
