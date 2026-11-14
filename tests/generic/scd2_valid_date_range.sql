{#
    Fails for any version whose validity window is empty or reversed.
#}

{% test scd2_valid_date_range(model, valid_from='valid_from_dttm', valid_to='valid_to_dttm') %}

select *

from {{ model }}

where {{ valid_from }} >= {{ valid_to }}

{% endtest %}
