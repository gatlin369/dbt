{% macro capture_errors_on_end(results, audit_table='AUDIT_DBT_LOG_BATCH_DTLS') %}
  {% if execute %}
    {% set audit_table_name = var('ctrl_db') ~ '.' ~ var('ctrl_schema') ~ '.' ~ audit_table %}
    {% set run_id   = env_var('DBT_RUN_ID', 'manual_run_' ~ run_started_at) %}
    {% set batch_id = env_var('DBT_BATCH_ID', '0') %}

    {% for res in results %}
      {% if res.node.resource_type == 'model' and res.status in ['error', 'skipped'] %}
        {% set status = 'FAILED' if res.status == 'error' else 'SKIPPED' %}
        {% set msg = (res.message or '') | replace('\\', '\\\\') | replace("'", "''") %}

        {% set sql %}
          merge into {{ audit_table_name }} t
          using (select '{{ res.node.alias }}' as table_name) s
            on  t.table_name    = s.table_name
            and t.run_id        = '{{ run_id }}'
            and t.batch_id      = try_to_number('{{ batch_id }}')
            and t.invocation_id = '{{ invocation_id }}'
          when matched then update set              -- the model started: finish its row
            end_time = current_timestamp(),
            status = '{{ status }}',
            duration_in_sec = datediff(second, t.start_time, current_timestamp()),
            error_msg = left('{{ msg }}', 4000),
            audit_table_update_time = current_timestamp()
          when not matched then insert              -- never started (skipped): add a row
            (day_id, batch_id, source, layer, table_name, dag_name, task_name, run_id,
             start_time, end_time, status, error_msg, audit_table_update_time, invocation_id)
          values
            (current_date(), try_to_number('{{ batch_id }}'), '{{ project_name }}', null,
             s.table_name, '{{ env_var('DBT_DAG_ID', 'local_dag') }}',
             '{{ env_var('DBT_TASK_ID', 'local_task') }}', '{{ run_id }}',
             current_timestamp(), current_timestamp(), '{{ status }}',
             left('{{ msg }}', 4000), current_timestamp(), '{{ invocation_id }}')
        {% endset %}
        {% do run_query(sql) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
