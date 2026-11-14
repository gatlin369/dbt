{#
    Fails for any business key that does not have exactly one
    current version (current_record_ind = true).
#}

{% test scd2_one_current_row_per_key(model, key_columns, current_flag='current_record_ind') %}

select
    {{ key_columns | join(', ') }},
    count_if({{ current_flag }}) as current_rows

from {{ model }}

group by {{ key_columns | join(', ') }}

having count_if({{ current_flag }}) <> 1

{% endtest %}
