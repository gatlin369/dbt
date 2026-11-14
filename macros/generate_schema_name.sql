{#
    Use a model's custom schema verbatim (e.g. schema: RITA_TEST -> RITA_TEST)
    instead of dbt's default <target_schema>_<custom_schema>.
    Models without a custom schema still build into the target schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
