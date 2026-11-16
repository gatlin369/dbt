{% macro scd2_versioning(
        partition_key,
        surrogate_key=none,
        sort_key=[],
        cte_hashed='cte_hashed'
    )
%}

    {# ------------------------------------------------------------- #}
    {# Validate required arguments                                   #}
    {# ------------------------------------------------------------- #}

    {% if not partition_key %}
        {{ exceptions.raise_compiler_error(
            "scd2_versioning: partition_key cannot be empty."
        ) }}
    {% endif %}

    {% if partition_key is string %}
        {{ exceptions.raise_compiler_error(
            "scd2_versioning: partition_key must be a list, for example ['claim_id']."
        ) }}
    {% endif %}


    {# ------------------------------------------------------------- #}
    {# Determine whether the target relation exists and has rows     #}
    {# ------------------------------------------------------------- #}

    {% set target_exists = false %}
    {% set target_count = 0 %}

    {% if execute %}

        {% set target_relation = adapter.get_relation(
            database=this.database,
            schema=this.schema,
            identifier=this.identifier
        ) %}

        {% if target_relation is not none %}

            {% set target_exists = true %}

            {% set row_count_query %}
                select count(*) as cnt
                from {{ target_relation }}
            {% endset %}

            {% set result = run_query(row_count_query) %}

            {% if result is not none %}
                {% set target_count =
                    result.columns[0].values()[0] | int
                %}
            {% endif %}

        {% endif %}

    {% endif %}


    {# ------------------------------------------------------------- #}
    {# Incoming rows                                                  #}
    {#                                                               #}
    {# Add the SCD metadata columns before UNION ALL BY NAME.         #}
    {# This guarantees that the columns exist on the initial load.    #}
    {# ------------------------------------------------------------- #}

    , cte_incoming_normalized as (

        select
            i.*,

            i.hash_val as dbt_scd_id,

            i.dbt_max_source_update_dttm
                as dbt_update_dttm,

            cast(null as timestamp_ntz)
                as valid_from_dttm,

            cast(null as timestamp_ntz)
                as valid_to_dttm,

            cast(null as boolean)
                as current_record_ind

        from {{ cte_hashed }} i

    )


    {# ------------------------------------------------------------- #}
    {# Retrieve active target versions for affected business keys    #}
    {# ------------------------------------------------------------- #}

    , cte_existing_active as (

        {% if target_exists and target_count > 0 %}

            select
                t.*

                {% if surrogate_key %}
                    exclude ({{ surrogate_key }})
                {% endif %}

            from {{ this }} t

            where t.current_record_ind = true

              and exists (

                    select 1
                    from {{ cte_hashed }} i

                    where
                        {% for col in partition_key %}

                            t.{{ col }}
                                is not distinct from
                            i.{{ col }}

                            {% if not loop.last %}
                                and
                            {% endif %}

                        {% endfor %}
              )

        {% else %}

            {#
               Do not reference the target relation when it does not
               exist. Instead, create an empty relation using the
               normalized incoming schema.
               (Jinja comment on purpose: rendering `this` inside a SQL
               comment breaks inline/IDE compiles of ephemeral models.)
            #}


            select *
            from cte_incoming_normalized
            where 1 = 0

        {% endif %}

    )


    {# ------------------------------------------------------------- #}
    {# Select new keys and versions newer than the active target row #}
    {# ------------------------------------------------------------- #}

    , cte_hashed_modified as (

        {% if target_exists and target_count > 0 %}

            select i.*
            from cte_incoming_normalized i

            where not exists (

                select 1
                from {{ this }} t

                where t.current_record_ind = true

                  {% for col in partition_key %}

                      and t.{{ col }}
                          is not distinct from
                          i.{{ col }}

                  {% endfor %}

                  and t.dbt_max_source_update_dttm
                      >= i.dbt_max_source_update_dttm
            )

        {% else %}

            select *
            from cte_incoming_normalized

        {% endif %}

    )


    {# ------------------------------------------------------------- #}
    {# Combine incoming rows with affected active target rows        #}
    {# ------------------------------------------------------------- #}

    , cte_combined as (

        select *
        from cte_hashed_modified

        union all by name

        select *
        from cte_existing_active

    )


    {# ------------------------------------------------------------- #}
    {# Keep one row for each key and source update timestamp         #}
    {# ------------------------------------------------------------- #}

    , cte_deduped as (

        select *
        from cte_combined

        qualify row_number() over (

            partition by
                {{ partition_key | join(', ') }},
                dbt_max_source_update_dttm

            order by
                dbt_max_source_update_dttm desc

                {% for col, direction in sort_key %}

                    {% if col | lower
                          != 'dbt_max_source_update_dttm' %}

                        , {{ col }} {{ direction }}

                    {% endif %}

                {% endfor %}

        ) = 1

    )


    {# ------------------------------------------------------------- #}
    {# Compare each version with the preceding version               #}
    {# ------------------------------------------------------------- #}

    , cte_with_previous_hash as (

        select
            *,

            lag(hash_val) over (

                partition by
                    {{ partition_key | join(', ') }}

                order by
                    dbt_max_source_update_dttm

                    {% for col, direction in sort_key %}

                        {% if col | lower
                              != 'dbt_max_source_update_dttm' %}

                            , {{ col }} {{ direction }}

                        {% endif %}

                    {% endfor %}
            ) as dbt_previous_hash_val,

            row_number() over (

                partition by
                    {{ partition_key | join(', ') }}

                order by
                    dbt_max_source_update_dttm

                    {% for col, direction in sort_key %}

                        {% if col | lower
                              != 'dbt_max_source_update_dttm' %}

                            , {{ col }} {{ direction }}

                        {% endif %}

                    {% endfor %}
            ) as dbt_version_sequence

        from cte_deduped

    )


    {# ------------------------------------------------------------- #}
    {# Remove consecutive versions with an unchanged attribute hash #}
    {# ------------------------------------------------------------- #}

    , cte_hash_deduped as (

        select
            * exclude (
                dbt_previous_hash_val,
                dbt_version_sequence
            )

        from cte_with_previous_hash

        where dbt_version_sequence = 1
           or hash_val is distinct from dbt_previous_hash_val

    )


    {# ------------------------------------------------------------- #}
    {# Calculate SCD Type 2 validity boundaries                      #}
    {#                                                               #}
    {# Half-open interval:                                           #}
    {# valid_from_dttm <= event timestamp < valid_to_dttm            #}
    {# ------------------------------------------------------------- #}

    , cte_versioned as (

        select
            * exclude (
                valid_from_dttm,
                valid_to_dttm,
                current_record_ind
            ),

            dbt_max_source_update_dttm
                as valid_from_dttm,

            coalesce(

                lead(dbt_max_source_update_dttm) over (

                    partition by
                        {{ partition_key | join(', ') }}

                    order by
                        dbt_max_source_update_dttm

                        {% for col, direction in sort_key %}

                            {% if col | lower
                                  != 'dbt_max_source_update_dttm' %}

                                , {{ col }} {{ direction }}

                            {% endif %}

                        {% endfor %}
                ),

                '9999-12-31 00:00:00'::timestamp_ntz

            ) as valid_to_dttm,

            case

                when lead(dbt_max_source_update_dttm) over (

                    partition by
                        {{ partition_key | join(', ') }}

                    order by
                        dbt_max_source_update_dttm

                        {% for col, direction in sort_key %}

                            {% if col | lower
                                  != 'dbt_max_source_update_dttm' %}

                                , {{ col }} {{ direction }}

                            {% endif %}

                        {% endfor %}
                ) is null

                then true
                else false

            end as current_record_ind

        from cte_hash_deduped

    )


    select *
    from cte_versioned

{% endmacro %}
