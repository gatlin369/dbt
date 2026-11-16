{% macro capture_log_at_start(
    audit_table='AUDIT_DBT_LOG_BATCH_DTLS'
) %}

    {% set task_name =
        env_var('DBT_TASK_ID','local_task') %}

    {% set table_name =
        this.identifier %}

    {% set dag_name =
        env_var('DBT_DAG_ID','local_dag') %}

    {% set run_id =
        env_var(
            'DBT_RUN_ID',
            'manual_run_' ~ run_started_at
        ) %}

    {% set batch_id =
        env_var('BATCH_ID','0') %}

    {% set source_system =
        project_name %}

    {% set name_l =
        model.name | lower %}

    {% if 'cur_' in name_l %}
        {% set layer = 'CURATED' %}
    {% elif 'dim_' in name_l
        or 'fct_' in name_l
        or 'fft_' in name_l
        or 'cdm_' in name_l
        or '_inc' in name_l %}
        {% set layer = 'EDW' %}
    {% elif 'v_' in name_l %}
        {% set layer = 'GOLD' %}
    {% else %}
        {% set layer = 'UNKNOWN' %}
    {% endif %}

    {% set ctrl_db =
        var('ctrl_db') %}

    {% set ctrl_schema =
        var('ctrl_schema') %}

    {% set audit_table_name =
        ctrl_db ~ '.'
        ~ ctrl_schema ~ '.'
        ~ audit_table %}

    {% call statement(
        'audit_start',
        fetch_result=False
    ) %}

        insert into {{ audit_table_name }}
        (
            day_id,
            batch_id,
            source,
            layer,
            table_name,
            dag_name,
            task_name,
            run_id,
            start_time,
            status,
            audit_table_update_time,
            invocation_id
        )
        select
            current_date(),
            try_to_number('{{ batch_id }}'),
            '{{ source_system }}',
            '{{ layer }}',
            '{{ table_name }}',
            '{{ dag_name }}',
            '{{ task_name }}',
            '{{ run_id }}',
            current_timestamp(),
            'RUNNING',
            current_timestamp(),
            '{{ invocation_id }}'

    {% endcall %}

{% endmacro %}
