{#
    Fails when a business key has more than one version starting
    at the same valid_from timestamp.
#}

{% test scd2_unique_versions(model, key_columns, valid_from='valid_from_dttm') %}

select
    {{ key_columns | join(', ') }},
    {{ valid_from }},
    count(*) as versions

from {{ model }}

group by {{ key_columns | join(', ') }}, {{ valid_from }}

having count(*) > 1

{% endtest %}
