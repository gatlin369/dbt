{{ config(
    materialized='ephemeral'
) }}

with cte_source as (

    select
        'cc-'||'cc_vehicle'||':'||v.id::varchar(256) as vehicleid,

        v.color,
        orvs.name as offroadstyle,
        v.vin,
        v.policysystemid,

        -- psl.tablename as tablename,
        replace(
            upper(substring(v.policysystemid, charindex(':', v.policysystemid) + 1, 10)),
            '_FL',
            ''
        ) as tablekey,

        v.loanmonthlypayment,
        v.loanpayoffamount,
        v.model,
        v.make,
        vm.name as manufacturer,
        st.name as state,
        vs.name as style,
        v.loanmonthsremaining,
        v.licenseplate,
        v.boattype,
        v.loan,
        v.year,
        v.serialnumber,

        'cc-'||'cc_user'||':'||v.createuserid::varchar(256) as createuserid,
        'cc-'||'cc_user'||':'||v.updateuserid::varchar(256) as updateuserid,

        v.publicid,

        'GWCC' as sourcesystem,

        v.createtime,

        coalesce(v.updatetime, '1900-01-01')::timestamp_ntz as updatetime,

        cast(nvl(v.beanversion, 0) as varchar(256)) as beanversion,

        v.lienholder_ext,
        v.lienholdername,

        coalesce(v.row_insert_tms, '1900-01-01')::timestamp_ntz as row_insert_tms

    from {{ source('mrg_cc', 'cc_vehicle') }} v

    left join {{ source('mrg_cc', 'cctl_jurisdiction') }} st
        on st.id = v.state

    left join {{ source('mrg_cc', 'cctl_vehiclestyle') }} vs
        on vs.id = v.style

    left join {{ source('mrg_cc', 'cctl_offroadvehiclestyle') }} orvs
        on orvs.id = v.offroadstyle

    left join {{ source('mrg_cc', 'cctl_vehiclemanufacturer') }} vm
        on vm.id = v.manufacturer


    -- left join mrg_cc.policysystemlookup psl
    --     on substring(v.policysystemid, charindex('.', v.policysystemid) + 1,
    --        charindex(':', v.policysystemid) - charindex('.', v.policysystemid) - 1) = psl.policysystem
    -- unable to get the correct table, showing as unauthorised table

),

cte_hashed as (

    select
        *,
        updatetime as dbt_max_source_update_dttm,

        sha2(
    concat_ws(
        '||',

        nvl(to_varchar(color), ''),
        nvl(to_varchar(offroadstyle), ''),
        nvl(to_varchar(vin), ''),
        nvl(to_varchar(policysystemid), ''),
        nvl(to_varchar(tablekey), ''),
        nvl(to_varchar(loanmonthlypayment), ''),
        nvl(to_varchar(loanpayoffamount), ''),
        nvl(to_varchar(model), ''),
        nvl(to_varchar(make), ''),
        nvl(to_varchar(manufacturer), ''),
        nvl(to_varchar(state), ''),
        nvl(to_varchar(style), ''),
        nvl(to_varchar(loanmonthsremaining), ''),
        nvl(to_varchar(licenseplate), ''),
        nvl(to_varchar(boattype), ''),
        nvl(to_varchar(loan), ''),
        nvl(to_varchar(year), ''),
        nvl(to_varchar(serialnumber), ''),
        nvl(to_varchar(lienholder_ext), ''),
        nvl(to_varchar(lienholdername), ''),
        nvl(to_varchar(publicid), '')
    ),
    256
) as hash_val

    from cte_source

)

{{ scd2_versioning(
    partition_key=['vehicleid'],
    surrogate_key=none,
    sort_key=[],
    cte_hashed='cte_hashed'
) }}
