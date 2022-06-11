{{ config(
    materialized = 'view',
    schema = 'dw_r_and_d',
) }}

with owner_financial_customers as (
select
distinct
    date_trunc('month', c.event_date)     as month
    , cf.id                               as company_id
    , cf.name                             as company_name
    , cf.salesforce_id
    , cf.company_segment                  as segment
    , cf.procore_start_date
from {{ ref('companies_filtered') }} as cf
inner join {{ ref('agg_company_action') }} as c
    on cf.id = c.company_id
where company_type_tier_1 = 'Owner / Real Estate Developer'
    and is_customer = true
    and is_active = true
    and product_bundle ilike '%Fin%'
    and datediff('month', event_date, current_date) <= 12
)
,
--workflow information
workflows as (
select
distinct
date_trunc('month', week)   as month
, company_id
from {{ ref('v_workflow_pilot') }} as w
inner join {{ ref('companies_filtered') }} as c
    on w.company_id = c.id
where datediff('month', procore_start_date, month) <= 1
)
,
--budget_changes
budget_changes as (
select
distinct
  date_trunc('month', bc.created_at::date) as month
  , company_id
  , project_id
from {{ ref('budget_changes_prep') }} as bc
inner join {{ ref('companies_filtered') }} as c
    on bc.company_id = c.id
where datediff('month', procore_start_date, month) <= 1
)
,

--owner dictionary
owner_dictionary as (
select
    company_id
    , date_trunc('month',min(ah.created_at::date)) as month
from {{ ref('active_histories_prep') }} as ah
inner join {{ ref('companies_filtered') }} as cf
    on ah.company_id = cf.id
where ref_type = 'Company'
    and column_name = 'locale'
    and new_value = 'en-owner-beta'
group by 1, procore_start_date
having datediff('month', procore_start_date, month) <= 1
)
,
--v2 custom fields
custom_fields as (
select
distinct
date_trunc('month', cfd.created_at::date) as month
, cfd.company_id
, 'Change Event' as custom_field_type
, cfd.id as custom_field
from {{ ref('companies_filtered') }} as c
inner join {{ ref('custom_field_definitions_prep') }} as cfd
    on c.id = cfd.company_id
inner join {{ ref('custom_field_metadata_prep') }} as cfm
    on cfm.deleted_at is null
    and cfm.custom_field_definition_id = cfd.id
inner join {{ ref('configurable_field_sets_prep') }} as cfs
    on cfs.deleted_at is null
    and cfs.id = cfm.source_id
    and cfs.type = 'ConfigurableFieldSet::ChangeEvent::Event'
inner join {{ ref('change_event_event_project_configurable_field_sets_prep') }} as pc
    on pc.configurable_field_set_change_event_event_id = cfs.id
inner join {{ ref('projects_prep') }} as p
    on p.id = pc.project_id
where cfd.deleted_at is null
    and datediff('month', procore_start_date, month) <= 1

union

--prime contract
select
distinct
date_trunc('month', cfd.created_at::date) as month
, cfd.company_id
, 'Prime Contract' as custom_field_type
, cfd.id as custom_field
from {{ ref('companies_filtered') }} as c
inner join {{ ref('custom_field_definitions_prep') }} as cfd
    on c.id = cfd.company_id
inner join {{ ref('custom_field_metadata_prep') }} as cfm
    on cfm.deleted_at is null
    and cfm.custom_field_definition_id = cfd.id
inner join {{ ref('configurable_field_sets_prep') }} as cfs
    on cfs.deleted_at is null
    and cfs.id = cfm.source_id
    and cfs.type = 'ConfigurableFieldSet::PrimeContract'
inner join {{ ref('prime_contract_project_configurable_field_sets_prep') }} as pc
    on pc.configurable_field_set_prime_contract_id = cfs.id
inner join {{ ref('projects_prep') }} as p
    on p.id = pc.project_id
where cfd.deleted_at is null
    and datediff('month', procore_start_date, month) <= 1

union

--purchase order contract
select
distinct
date_trunc('month', cfd.created_at::date) as month
, cfd.company_id
, 'Purchase Order Contract' as custom_field_type
, cfd.id as custom_field
from {{ ref('companies_filtered') }} as c
inner join {{ ref('custom_field_definitions_prep') }} as cfd
    on c.id = cfd.company_id
inner join {{ ref('custom_field_metadata_prep') }} as cfm
    on cfm.deleted_at is null
    and cfm.custom_field_definition_id = cfd.id
inner join {{ ref('configurable_field_sets_prep') }} as cfs
    on cfs.deleted_at is null
    and cfs.id = cfm.source_id
    and cfs.type = 'ConfigurableFieldSet::PurchaseOrderContract'
inner join {{ ref('purchase_order_contract_project_configurable_field_sets_prep') }} as pc
    on pc.configurable_field_set_purchase_order_contract_id = cfs.id
inner join {{ ref('projects_prep') }} as p
    on p.id = pc.project_id
where cfd.deleted_at is null
    and datediff('month', procore_start_date, month) <= 1

union

--work order contract
select
distinct
date_trunc('month', cfd.created_at::date) as month
, cfd.company_id
, 'Work Order Contract' as custom_field_type
, cfd.id as custom_field
from {{ ref('companies_filtered') }} as c
inner join {{ ref('custom_field_definitions_prep') }} as cfd
    on c.id = cfd.company_id
inner join {{ ref('custom_field_metadata_prep') }} as cfm
    on cfm.deleted_at is null
    and cfm.custom_field_definition_id = cfd.id
inner join {{ ref('configurable_field_sets_prep') }} as cfs
    on cfs.deleted_at is null
    and cfs.id = cfm.source_id
    and cfs.type = 'ConfigurableFieldSet::WorkOrderContract'
inner join {{ ref('work_order_contract_project_configurable_field_sets_prep') }} as pc
    on pc.configurable_field_set_work_order_contract_id = cfs.id
inner join {{ ref('projects_prep') }} as p
    on p.id = pc.project_id
where cfd.deleted_at is null
    and datediff('month', procore_start_date, month) <= 1
)

select
distinct
  o.month
  , o.company_id
  , o.company_name
  , o.salesforce_id
  , o.segment
  , o.procore_start_date
  , max(case when w.company_id is not null then true else false end)                as created_workflow_during_first_month
  , max(case when bc.company_id is not null then true else false end)               as created_budget_change_on_a_project_during_first_month
  , max(case when od.company_id is not null then true else false end)               as owner_dictionary_enabled_during_first_month
  , max(case when c.company_id is not null then true else false end)                as v2_custom_field_applied_during_first_month
  , max(case when c.company_id is not null 
     and c.custom_field_type = 'Change Event' then true else false end)             as v2_custom_field_change_event_applied_during_first_month
  , max(case when c.company_id is not null 
     and c.custom_field_type = 'Prime Contract' then true else false end)           as v2_custom_field_prime_contract_applied_during_first_month
  , max(case when c.company_id is not null 
     and c.custom_field_type = 'Purchase Order Contract' then true else false end)  as v2_custom_field_purchase_order_contract_applied_during_first_month
  , max(case when c.company_id is not null 
     and c.custom_field_type = 'Work Order Contract' then true else false end)      as v2_custom_field_work_order_contract_applied_during_first_month
from owner_financial_customers as o
left join workflows as w
    on o.company_id = w.company_id
    and o.month = w.month
left join budget_changes as bc
    on o.company_id = bc.company_id
    and o.month = bc.month
left join owner_dictionary as od
    on o.company_id = od.company_id
    and o.month = od.month
left join custom_fields as c
    on o.company_id = c.company_id
    and o.month = c.month  
group by 1,2,3,4,5,6 
