{% set pre_hook_macro_log_ins %}
{{ capture_log_at_start() }}
{% endset %}

{% set post_hook_macro_gw_flag %}
{{ update_deleted_records(
    source('mrg_cc', 'cc_claim'),
    this,
    ['claimid'],
    ['id']
) }}
{% endset %}

{% set post_hook_macro_log_upd %}
{{ capture_log_at_end('SUCCESS') }}
{% endset %}


{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['claimid', 'valid_from_dttm'],
    full_refresh=false,

    pre_hook=[
        pre_hook_macro_log_ins
    ],

    post_hook=[
        post_hook_macro_gw_flag,
        post_hook_macro_log_upd
    ]
) }}


with cte_source as (

    select
        'cc-'||'cc_claim'||':'||cc.id::varchar(256) as claimid,
        'cc-'||'cc_policy'||':'||cc.policyid::varchar(256) as policyid,
        'cc-'||'cc_catastrophe'||':'||cc.catastropheid::varchar(256) as catastropheid,

        'cc-'||'cc_user'||':'||cc.previoususerid::varchar(256) as previoususerid,
        'cc-'||'cc_user'||':'||cc.assignedbyuserid::varchar(256) as assignedbyuserid,
        'cc-'||'cc_user'||':'||cc.assigneduserid::varchar(256) as assigneduserid,

        'cc-'||'cc_group'||':'||cc.previousgroupid::varchar(256) as previousgroupid,
        'cc-'||'cc_group'||':'||cc.assignedgroupid::varchar(256) as assignedgroupid,

        'cc-'||'cc_workcomp'||':'||cc.claimworkcompid::varchar(256) as claimworkcompid,

        cc.claimnumber as claimnum,
        cc.lossdate as lossdt,
        lc.name as losscause,
        cc.losslocationcode as losslocationcd,
        lt.name as losstype,
        cc.description as lossdesc,
        cs.name as claimsource,
        ct.name as claimtier,

        cc.dateformgiventoemp as formgiventoempdt,
        cc.dateformretbyemp as formretbyempdt,
        cc.datecompdcsndue as compdcsnduedt,

        cc.examinationdate as examinationdt,
        cc.exposurebegan as exposurebegandt,
        cc.exposureended as exposureendeddt,

        at.name as accidenttype,

        cc.reporteddate as reporteddt,
        hrt.name as howreported,
        rpt.name as reportedbytype,

        cc.mold,
        flt.name as flaggedind,
        cc.flaggeddate as flaggeddt,
        cc.reopendate as reopendt,

        crr.name as reopenedreasondesc,

        cc.claimantrprtddate as claimantrprtddt,

        cc.daterptdtoinsured as rptdtoinsureddt,
        cc.daterptdtoagent as rptdtoagentdt,
        cc.daterptdtoemployer as rptdtoemployerdt,

        cc.employervalidityreason,

        cc.deathdate as deathdt,
        cc.statutedate as statutedt,
        cc.hospitaldate as hospitaldt,

        cc.datecompdcsnmade as compdcsnmadedt,

        cc.preexdisblty as preexdisbltyind,
        cc.safetyequipprov as safetyequipprovind,
        cc.computersecurity as computersecurityind,
        cc.safetyequipused as safetyequipusedind,
        cc.employmentinjury as employmentinjuryind,

        iff(
            yn.name = 'Yes',
            true,
            false
        ) as medicalfirstinfoind,

        cc.hospitaldays,

        cc.coverageinquestion as coverageinquestionind,
        cc.incidentreport as incidentreportind,
        cc.firstnoticesuit as firstnoticesuitind,
        cc.concurrentemp as concurrentempind,
        cc.drugsinvolved as drugsinvolvedind,
        cc.weatherrelated as weatherrelatedind,

        wt.name as weather,

        cc.hazardouswaste as hazardouswasteind,
        cc.currentconditions as currentconditionsind,
        cc.permissionrequired as permissionreqind,
        cc.benefitsstatusdcsn as benefitsstatusdcsnind,
        cc.reinsurancereportable as reinsurancereportableind,

        rfs.name as reinsuranceflaggedstatus,
        cps.name as progress,
        s.name as state,
        lobcd.typecode as lobcd,
        ccs.name as segment,
        ccs2.name as strategy,

        cc.locationoftheft,
        cc.policedeptinfo as policedeptinfodesc,
        cc.firedeptinfo as firedeptinfodesc,

        rlt.name as maincontacttype,
        jur.name as jurisdictionstate,

        cc.stateacknumber as stateacknum,
        cc.statefilenumber as statefilenum,

        cc.flaggedreason as flaggedreasondesc,
        cc.fault,
        fr.name as faultrating,
        lln.name as largelossnotificationstatus,
        cas.name as assignmentstatus,

        cc.assignmentdate as assignmenttmstp,

        cis.name as isostatus,
        cc.isoknown as isoknownind,
        cc.isoenabled as isoenabledind,

        cc.isosenddate as isosenddt,
        cc.isoreceivedate as isoreceivedt,

        css.name as salvagestatus,
        cst.name as storagetype,
        cc.storagelocationstate,
        csc.name as storagecategory,
        cc.storagevolumes,
        cc.storagebarcodenum,

        cc.closedate as closetmstp,
        cct.name as closedoutcome,
        ls.name as litigationstatus,
        sius.name as siustatus,

        'pc-'||'pc_policyperiod'||':'||
            cplc.policysystemperiodid::varchar(256)
            as policysystemperiodid,

        'cc-'||'cc_address'||':'||
            cc.losslocationid::varchar(256)
            as losslocationid,

        'cc-'||'cc_contact'||':'||
            cc.insureddenormid::varchar(256)
            as insureddenormid,

        'cc-'||'cc_user'||':'||
            cc.createuserid::varchar(256)
            as createuserid,

        'cc-'||'cc_user'||':'||
            cc.updateuserid::varchar(256)
            as updateuserid,

        cc.publicid,

        'GWCC' as sourcesystem,

        cast(
            nvl(cc.beanversion, 0) ||
            nvl(cplc.beanversion, 0)
            as varchar(256)
        ) as beanversion,

        cc.createtime,

        coalesce(
            array_max([
                cc.updatetime,
                cplc.updatetime
            ]),
            '1900-01-01'
        )::timestamp_ntz as updatetime,

        coalesce(
            array_max([
                cc.row_insert_tms,
                cplc.row_insert_tms
            ]),
            '1900-01-01'
        )::timestamp_ntz as row_insert_tms

    from {{ source('mrg_cc', 'cc_claim') }} cc

    left join {{ source('mrg_cc', 'cc_policy') }} cplc
        on cc.policyid = cplc.id

    left join {{ source('mrg_cc', 'cctl_losscause') }} lc
        on cc.losscause = lc.id

    left join {{ source('mrg_cc', 'cctl_losstype') }} lt
        on cc.losstype = lt.id

    left join {{ source('mrg_cc', 'cctl_claimsource') }} cs
        on cc.claimsource = cs.id

    left join {{ source('mrg_cc', 'cctl_claimtier') }} ct
        on cc.claimtier = ct.id

    left join {{ source('mrg_cc', 'cctl_accidenttype') }} at
        on cc.accidenttype = at.id

    left join {{ source('mrg_cc', 'cctl_claimreopenedreason') }} crr
        on cc.reopenedreason = crr.id

    left join {{ source('mrg_cc', 'cctl_weathertype') }} wt
        on cc.weather = wt.id

    left join {{ source('mrg_cc', 'cctl_reinsuranceflaggedstatus') }} rfs
        on cc.reinsuranceflaggedstatus = rfs.id

    left join {{ source('mrg_cc', 'cctl_claimprogresstype') }} cps
        on cc.progress = cps.id

    left join {{ source('mrg_cc', 'cctl_claimstate') }} s
        on cc.state = s.id

    left join {{ source('mrg_cc', 'cctl_claimsegment') }} ccs
        on cc.segment = ccs.id

    left join {{ source('mrg_cc', 'cctl_claimstrategy') }} ccs2
        on cc.strategy = ccs2.id

    left join {{ source('mrg_cc', 'cctl_faultrating') }} fr
        on cc.faultrating = fr.id

    left join {{ source('mrg_cc', 'cctl_largelossnotificationstat') }} lln
        on cc.largelossnotificationstatus = lln.id

    left join {{ source('mrg_cc', 'cctl_assignmentstatus') }} cas
        on cc.assignmentstatus = cas.id

    left join {{ source('mrg_cc', 'cctl_isostatus') }} cis
        on cc.isostatus = cis.id

    left join {{ source('mrg_cc', 'cctl_salvagestatus') }} css
        on cc.salvagestatus = css.id

    left join {{ source('mrg_cc', 'cctl_storagetype') }} cst
        on cc.storagetype = cst.id

    left join {{ source('mrg_cc', 'cctl_storagecategory') }} csc
        on cc.storagecategory = csc.id

    left join {{ source('mrg_cc', 'cctl_claimclosedoutcometype') }} cct
        on cc.closedoutcome = cct.id

    left join {{ source('mrg_cc', 'cctl_litigationstatus') }} ls
        on cc.litigationstatus = ls.id

    left join {{ source('mrg_cc', 'cctl_siustatus') }} sius
        on cc.siustatus = sius.id

    left join {{ source('mrg_cc', 'cctl_lobcode') }} lobcd
        on cc.lobcode = lobcd.id

    left join {{ source('mrg_cc', 'cctl_personrelationtype') }} rlt
        on cc.maincontacttype = rlt.id

    left join {{ source('mrg_cc', 'cctl_personrelationtype') }} rpt
        on cc.reportedbytype = rpt.id

    left join {{ source('mrg_cc', 'cctl_howreportedtype') }} hrt
        on cc.howreported = hrt.id

    left join {{ source('mrg_cc', 'cctl_jurisdiction') }} jur
        on cc.jurisdictionstate = jur.id

    left join {{ source('mrg_cc', 'cctl_yesno') }} yn
        on cc.showmedicalfirstinfo = yn.id

    left join {{ source('mrg_cc', 'cctl_flaggedtype') }} flt
        on cc.flagged = flt.id


    /* ===============================================================
       Deleted Guidewire claims are not processed as normal SCD2 rows.

       update_deleted_records() handles them in the post-hook instead.
       =============================================================== */

    where coalesce(cc.gw_delete_flag, 'N') <> 'Y'


    /* ===============================================================
       Apply the watermark only when this is an incremental execution.

       The 3-day lookback deliberately rereads recent records to handle
       late arriving / delayed source changes.
       =============================================================== */

    {% if is_incremental() %}

        and coalesce(
                array_max([
                    cc.updatetime,
                    cplc.updatetime
                ]),
                '1900-01-01'
            )::timestamp_ntz
            >
            (
                select
                    dateadd(
                        day,
                        -3,
                        coalesce(
                            max(dbt_max_source_update_dttm),
                            '1900-01-01'::timestamp_ntz
                        )
                    )
                from {{ this }}
            )

    {% endif %}

),


cte_hashed as (

    select
        *,

        updatetime as dbt_max_source_update_dttm,

        sha2(
            concat_ws(
                '||',

                nvl(to_varchar(policyid), ''),
                nvl(to_varchar(claimnum), ''),
                nvl(to_varchar(losscause), ''),
                nvl(to_varchar(losstype), ''),
                nvl(to_varchar(losslocationcd), ''),
                nvl(to_varchar(lossdesc), ''),
                nvl(to_varchar(claimsource), ''),
                nvl(to_varchar(claimtier), ''),
                nvl(to_varchar(accidenttype), ''),
                nvl(to_varchar(progress), ''),
                nvl(to_varchar(reinsuranceflaggedstatus), ''),
                nvl(to_varchar(weather), ''),
                nvl(to_varchar(flaggedreasondesc), ''),
                nvl(to_varchar(fault), ''),
                nvl(to_varchar(publicid), '')
            ),
            256
        ) as hash_val

    from cte_source

)


{{ scd2_versioning(
    partition_key=['claimid'],
    surrogate_key=none,
    sort_key=[],
    cte_hashed='cte_hashed'
) }}
