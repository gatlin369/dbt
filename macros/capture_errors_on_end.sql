{% macro capture_errors_on_end(
    results,
    audit_table='AUDIT_DBT_LOG_BATCH_DTLS'
) %}

    {% if execute %}

        {% set ctrl_db = var('ctrl_db') %}
        {% set ctrl_schema = var('ctrl_schema') %}

        {% set audit_table_name =
            ctrl_db
            ~ '.'
            ~ ctrl_schema
            ~ '.'
            ~ audit_table
        %}

        {% set run_id =
            env_var(
                'DBT_RUN_ID',
                'manual_run_' ~ run_started_at
            )
        %}

        {% set batch_id =
            env_var(
                'BATCH_ID',
                '0'
            )
        %}

        {% set dag_name =
            env_var(
                'DBT_DAG_ID',
                'local_dag'
            )
        %}

        {% set task_name =
            env_var(
                'DBT_TASK_ID',
                'local_task'
            )
        %}


        {% for res in results %}

            {% if
                res.node.resource_type == 'model'
                and res.status in ['error', 'skipped']
            %}

                {% if res.status == 'error' %}
                    {% set status = 'FAILED' %}
                {% else %}
                    {% set status = 'SKIPPED' %}
                {% endif %}


                {% set msg =
                    (res.message or '')
                    | replace('\\', '\\\\')
                    | replace("'", "''")
                %}

                {% set table_name =
                    res.node.alias
                %}

                {% set name_l =
                    res.node.name | lower
                %}


                {# Keep layer calculation consistent with start audit macro #}

                {% if 'cur_' in name_l %}

                    {% set layer = 'CURATED' %}

                {% elif
                    'dim_' in name_l
                    or 'fct_' in name_l
                    or 'fft_' in name_l
                    or 'cdm_' in name_l
                    or '_inc' in name_l
                %}

                    {% set layer = 'EDW' %}

                {% elif 'v_' in name_l %}

                    {% set layer = 'GOLD' %}

                {% else %}

                    {% set layer = 'UNKNOWN' %}

                {% endif %}


                {% set sql %}

                    merge into {{ audit_table_name }} t

                    using (
                        select
                            '{{ table_name }}'
                                as table_name
                    ) s

                    on
                        t.table_name = s.table_name

                        and t.run_id =
                            '{{ run_id }}'

                        and t.batch_id =
                            try_to_number(
                                '{{ batch_id }}'
                            )

                        and t.invocation_id =
                            '{{ invocation_id }}'


                    when matched then update set

                        end_time =
                            current_timestamp(),

                        status =
                            '{{ status }}',

                        duration_in_sec =
                            datediff(
                                second,
                                t.start_time,
                                current_timestamp()
                            ),

                        error_msg =
                            left(
                                '{{ msg }}',
                                4000
                            ),

                        audit_table_update_time =
                            current_timestamp()


                    when not matched then insert
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
                        end_time,
                        status,
                        error_msg,
                        audit_table_update_time,
                        invocation_id
                    )

                    values
                    (
                        current_date(),

                        try_to_number(
                            '{{ batch_id }}'
                        ),

                        '{{ project_name }}',

                        '{{ layer }}',

                        s.table_name,

                        '{{ dag_name }}',

                        '{{ task_name }}',

                        '{{ run_id }}',

                        current_timestamp(),

                        current_timestamp(),

                        '{{ status }}',

                        left(
                            '{{ msg }}',
                            4000
                        ),

                        current_timestamp(),

                        '{{ invocation_id }}'
                    )

                {% endset %}


                {% do run_query(sql) %}

            {% endif %}

        {% endfor %}

    {% endif %}

{% endmacro %}
