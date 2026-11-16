# The 5 fixes — copy-paste code

Apply in dbt Cloud, on a branch, one at a time, and run `dbt build --select claim_inc`
after each. These are drafts written from reading the code; test in your sandbox first.
Do FIX 4 together with FIX 3 (the failure macro reads DBT_BATCH_ID).

Your dbt Cloud file may differ slightly from this copy, so change only the blocks shown,
don't paste a whole file over yours.

====================================================================
FIX 1 — Protect the history (full_refresh=false)
File: models/CLAIM/claim_inc.sql  →  the config() block near the top
Add ONE line: full_refresh=false
====================================================================

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['claimid','valid_from_dttm'],
    full_refresh=false,                     -- NEW (Fix 1)

    pre_hook=[
        pre_hook_macro_log_ins
    ],

    post_hook=[
        post_hook_macro_gw_flag,
        post_hook_macro_log_upd
    ]
) }}


====================================================================
FIX 2 + FIX 5 — Skip deleted claims, and read only changed rows
File: models/CLAIM/claim_inc.sql  →  the END of the cte_source block,
AFTER the last left join (cctl_flaggedtype ... flt), BEFORE the closing  ),
Add the where clause below.
====================================================================

    left join {{ source('mrg_cc', 'cctl_flaggedtype') }} flt
        on cc.flagged = flt.id

    where coalesce(cc.gw_delete_flag, 'N') <> 'Y'          -- NEW (Fix 2): skip deleted claims
    {% if is_incremental() %}                              -- NEW (Fix 5): only changed rows
      and coalesce(array_max([cc.updatetime, cplc.updatetime]), '1900-01-01')::timestamp_ntz
          > (select dateadd(day, -3, max(dbt_max_source_update_dttm)) from {{ this }})
    {% endif %}

),

-- Notes:
--  * Fix 2 keeps deleted claims out of the model; the post-hook
--    update_deleted_records.sql still closes them in CLAIM_INC, so history is kept.
--  * Fix 5: first run (table empty) reads everything; later runs read only claims
--    (or their policy) changed since the last load, minus 3 days safety.
--    Change the 3 if your lead wants a different lookback.


====================================================================
FIX 3 — Failure logging
Step A: new file  macros/capture_errors_on_end.sql  (see the separate
        capture_errors_on_end.sql file)
Step B: add these 2 lines to  dbt_project.yml  (top level, e.g. at the bottom)
====================================================================

on-run-end:
  - "{{ capture_errors_on_end(results) }}"

-- This runs once at the end of EVERY run, pass or fail, and updates the log
-- rows of any model that FAILED or was SKIPPED (status + end time + error message).


====================================================================
FIX 4 — Batch ID: rename BATCH_ID -> DBT_BATCH_ID  (in 3 macro files)
The COLUMN batch_id in the log table does NOT change — only the env_var name.
====================================================================

-- 1) macros/capture_log_at_start.sql
--    change:   env_var('BATCH_ID','0')
--    to:       env_var('DBT_BATCH_ID','0')

-- 2) macros/capture_log_at_end.sql
--    change:   env_var(
--                  'BATCH_ID',
--                  '0'
--              )
--    to:       env_var(
--                  'DBT_BATCH_ID',
--                  '0'
--              )

-- 3) macros/update_deleted_records.sql
--    same change as (2): 'BATCH_ID'  ->  'DBT_BATCH_ID'

-- Then in dbt Cloud: Deploy > Environments > (environment) > Environment variables,
-- add DBT_BATCH_ID. For scheduled runs, the Control-M team supplies the value.
-- If Control-M passes it as --vars instead of an env var, use var('batch_id', 0)
-- in the macros instead of env_var(...). Confirm with them first.
