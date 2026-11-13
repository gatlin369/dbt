-- macros/capture_log.sql
-- macro to capture the job details details to audit table
-- this.name       -> snapshot (dbt node) name
-- this.identifier -> final table name (after aliasing)

{% macro capture_log_at_start(audit_table='CTL_DBT_LOG_BATCH_DTLS') %}
  {# {% set task_name = (this.name if this is defined else env_var('DBT_TASK_ID','local_snapshot')) %} #}
  {% set task_name  = env_var('DBT_TASK_ID','local_task') %}
  {% set table_name = (this.identifier if this is defined else task_name) %}

  {% set dag_name     = env_var('DBT_DAG_ID','local_dag') %}
  {% set run_id       = env_var('DBT_RUN_ID','manual_run_' ~ run_started_at) %}
  {% set batch_id     = env_var('BATCH_ID','0') %}

  {# Determine source from project name #}
  {% set source = project_name %}

  {# Determine layer from filename #}
  {% set name_l = model.name | lower %}
  {% if 'cur_' in name_l %}
    {% set layer = 'curated' %}
  {% elif 'dim_' in name_l or 'fct_' in name_l or 'fft_' in name_l or 'cdm_' in name_l %}
    {% set layer = 'edw' %}
  {% elif 'v_' in name_l %}
    {% set layer = 'gold' %}
  {% else %}
    {% set layer = 'unknown' %}
  {% endif %}

  {# Safe defaults if ctrl vars arent passed #}
  {% set ctrl_db     = var('ctrl_db') %}
  {% set ctrl_schema = var('ctrl_schema') %}
  {% set audit_table_name = ctrl_db ~ '.' ~ ctrl_schema ~ '.' ~ audit_table %}

  {% call statement('audit_start', fetch_result=False) %}
    INSERT INTO {{ audit_table_name }} (
      day_id, batch_id, source, layer, table_name, dag_name, task_name, run_id,
      start_time, status, audit_table_update_time, invocation_id
    )
    SELECT
      TO_DATE(CURRENT_TIMESTAMP()),
      TRY_TO_NUMBER('{{ batch_id }}'),
      '{{ source }}',
      '{{ layer }}',
      '{{ table_name }}',
      '{{ dag_name }}',
      '{{ task_name }}',
      '{{ run_id }}',
      CURRENT_TIMESTAMP(),
      'RUNNING',
      CURRENT_TIMESTAMP(),
      '{{ invocation_id }}'
  {% endcall %}
{% endmacro %}


{% macro capture_log_at_end(status='SUCCESS', audit_table='CTL_DBT_LOG_BATCH_DTLS') %}
  {# {% set task_name = (this.name if this is defined else env_var('DBT_TASK_ID','local_snapshot')) %} #}
  {% set task_name  = env_var('DBT_TASK_ID','local_task') %}
  {% set table_name = (this.identifier if this is defined else task_name) %}

  {% set dag_name     = env_var('DBT_DAG_ID','local_dag') %}
  {% set run_id       = env_var('DBT_RUN_ID','manual_run_' ~ run_started_at) %}
  {% set batch_id     = env_var('BATCH_ID','0') %}

  {# Determine source from project name #}
  {% set source = project_name %}

  {# Determine layer from filename #}
  {% set name_l = model.name | lower %}
  {% if 'cur_' in name_l %}
    {% set layer = 'curated' %}
  {% elif 'dim_' in name_l or 'fct_' in name_l or 'fft_' in name_l or 'cdm_' in name_l %}
    {% set layer = 'edw' %}
  {% elif 'v_' in name_l %}
    {% set layer = 'gold' %}
  {% else %}
    {% set layer = 'unknown' %}
  {% endif %}

  {% set ctrl_db     = var('ctrl_db') %}
  {% set ctrl_schema = var('ctrl_schema') %}
  {% set audit_table_name = ctrl_db ~ '.' ~ ctrl_schema ~ '.' ~ audit_table %}

  {% call statement('audit_end', fetch_result=False) %}
    UPDATE {{ audit_table_name }}
       SET end_time = CURRENT_TIMESTAMP(),
           status   = '{{ status }}',
           duration_in_sec = DATEDIFF('second', start_time, CURRENT_TIMESTAMP()),
           audit_table_update_time = CURRENT_TIMESTAMP()
     WHERE batch_id = TRY_TO_NUMBER('{{ batch_id }}')
       AND run_id   = '{{ run_id }}'
       AND table_name = '{{ table_name }}'
       AND invocation_id = '{{ invocation_id }}'
  {% endcall %}
{% endmacro %}
