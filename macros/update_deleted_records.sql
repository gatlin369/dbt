{% macro update_deleted_records(
    source_table_name,
    target_table_name,
    primary_keys_list_target,
    primary_keys_list_source=None
) %}

    {% if primary_keys_list_source is none %}
        {% set primary_keys_list_source =
            primary_keys_list_target %}
    {% endif %}

    {% set primary_keys =
        primary_keys_list_source | join(', ') %}

    {% set join_condition = [] %}

    {% for i in range(primary_keys_list_target|length) %}

        {% set tgt =
            primary_keys_list_target[i] %}

        {% set src =
            primary_keys_list_source[i] %}

        {% do join_condition.append(
            "split_part(t."
            ~ tgt
            ~ ",':',2)=s."
            ~ src
            ~ "::string"
        ) %}

    {% endfor %}

    {% set update_query %}

        update {{ target_table_name }} t

        set
            current_record_ind = false,

            valid_to_dttm =
                s.src_upd_time,

            dbt_max_source_update_dttm =
                s.src_upd_time,

            dbt_update_dttm =
                current_timestamp()

        from
        (

            select
                {{ primary_keys }},
                updatetime,
                updatetime as src_upd_time

            from {{ source_table_name }}

            where gw_delete_flag = 'Y'

        ) s

        where
            t.current_record_ind = true

            and t.valid_to_dttm =
                '9999-12-31'

            and
            {{ join_condition | join(' and ') }}

    {% endset %}

    {% call statement(
        'soft_delete_upd',
        fetch_result=True
    ) %}

        {{ update_query }}

    {% endcall %}

    {% set res =
        load_result(
            'soft_delete_upd'
        ) %}

    {% if res is none %}
        {% set rows = 0 %}
    {% else %}
        {% set rows =
            res['response']['rows_affected']
            | default(0) %}
    {% endif %}

    {% set ctrl_db =
        var('ctrl_db') %}

    {% set ctrl_schema =
        var('ctrl_schema') %}

    {% set audit_table_name =
        ctrl_db ~ '.'
        ~ ctrl_schema
        ~ '.AUDIT_DBT_LOG_BATCH_DTLS' %}

    {% set run_id =
        env_var(
            'DBT_RUN_ID',
            'manual_run_' ~ run_started_at
        ) %}

    {% set batch_id =
        env_var(
            'BATCH_ID',
            '0'
        ) %}

    {% set short_table_name =
        target_table_name.identifier %}

    {% set log_query %}

        update {{ audit_table_name }}

        set
            soft_delete_effected_rows =
                {{ rows }},

            audit_table_update_time =
                current_timestamp()

        where invocation_id =
            '{{ invocation_id }}'

        and table_name =
            '{{ short_table_name }}'

        and batch_id =
            try_to_number('{{ batch_id }}')

        and run_id =
            '{{ run_id }}'

    {% endset %}

    {% do run_query(log_query) %}

{% endmacro %}
