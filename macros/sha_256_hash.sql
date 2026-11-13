{% macro sha_256_hash(cols) %}
    sha2_hex(
        {%- for col in cols %}
            COALESCE(CAST({{ col }} AS VARCHAR), '')
            {%- if not loop.last %} || '|' || {% endif %}
        {%- endfor %}, 256
    )
{% endmacro %}
