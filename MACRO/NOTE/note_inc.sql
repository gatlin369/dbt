{{ config(
    materialized='ephemeral'
) }}

with cte_source as (

    select
        'cc-'||'cc_note'||':'||n.id::varchar(256) as noteid,

        'cc-'||'cc_activity'||':'||n.activityid::varchar(256) as activity,
        'cc-'||'cc_user'||':'||n.authorid::varchar(256) as authorid,

        n.authoringdate,
        n.body,

        'cc-'||'cc_claim'||':'||n.claimid::varchar(256) as claimid,
        'cc-'||'cc_claimcontact'||':'||n.claimcontactid::varchar(256) as claimcontactid,

        n.confidential,

        'cc-'||'cc_exposure'||':'||n.exposureid::varchar(256) as exposureid,

        n.language,

        'cc-'||'cc_matter'||':'||n.matterid::varchar(256) as matterid,

        n.portalauthorrealm_ext,
        n.portalauthorusername_ext,
        n.publicid,
        n.retired as retiredvalue,

        st.name as securitytype,

        'cc-'||'cc_servicerequest'||':'||n.servicerequestid::varchar(256) as servicerequest,

        n.subject,
        tp.name as topic,

        'cc-'||'cc_user'||':'||n.createuserid::varchar(256) as createuserid,
        'cc-'||'cc_user'||':'||n.updateuserid::varchar(256) as updateuser,

        'GWCC' as sourcesystem,

        cast(nvl(n.beanversion, 0) as varchar(256)) as beanversion,

        n.createtime,

        coalesce(n.updatetime, '1900-01-01')::timestamp_ntz as updatetime,

        coalesce(n.row_insert_tms, '1900-01-01')::timestamp_ntz as row_insert_tms

    from {{ source('mrg_cc', 'cc_note') }} n

    left join {{ source('mrg_cc', 'cctl_notesecuritytype') }} st
        on st.id = n.securitytype

    left join {{ source('mrg_cc', 'cctl_notetopictype') }} tp
        on tp.id = n.topic

),

cte_hashed as (

    select
        *,
        updatetime as dbt_max_source_update_dttm,

        sha2(
    concat_ws(
        '||',

        nvl(to_varchar(activity), ''),
        nvl(to_varchar(authorid), ''),
        nvl(to_varchar(authoringdate), ''),
        nvl(to_varchar(body), ''),
        nvl(to_varchar(claimid), ''),
        nvl(to_varchar(claimcontactid), ''),
        nvl(to_varchar(confidential), ''),
        nvl(to_varchar(exposureid), ''),
        nvl(to_varchar(language), ''),
        nvl(to_varchar(matterid), ''),
        nvl(to_varchar(portalauthorrealm_ext), ''),
        nvl(to_varchar(portalauthorusername_ext), ''),
        nvl(to_varchar(publicid), ''),
        nvl(to_varchar(retiredvalue), ''),
        nvl(to_varchar(securitytype), ''),
        nvl(to_varchar(servicerequest), ''),
        nvl(to_varchar(subject), ''),
        nvl(to_varchar(topic), '')
    ),
    256
) as hash_val

    from cte_source

)

{{ scd2_versioning(
    partition_key=['noteid'],
    surrogate_key=none,
    sort_key=[],
    cte_hashed='cte_hashed'
) }}
