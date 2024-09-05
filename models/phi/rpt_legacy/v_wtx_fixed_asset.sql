{{ config( 
  tags=["finance","gl","gl_trans","po"]
) }} 
with gl_trans as (
    select
        caf_no,
        company_code,
        gl_date as date,
        sum(base_ledger_amt) as base_ledger_amt,
        sum(txn_ledger_amt) as txn_ledger_amt,
        sum(phi_ledger_amt) as phi_ledger_amt,
        sum(pcomp_ledger_amt) as pcomp_ledger_amt,
        0 as base_line_open_amt,
        0 as txn_line_open_amt,
        0 as phi_line_open_amt,
        0 as pcomp_line_open_amt
    from {{ ref('fct_wbx_fin_gl_trans')}}
    where
        caf_no is not null
       --and (company_code = 'WBX' or company_code = 'RFL')
       and company_code in {{env_var("DBT_COMPANY_FILTER")}} 
        --and source_object_id in (122900, 122901)
        and source_object_id in (110090, 110095) /*This was changed as part of D365 as the Account code are changing from 122900 to 110090 and 122901 to 110095 */
        and source_document_type != 'FAA'
        and source_document_type != 'FAU'
    group by caf_no, company_code, gl_date
),
prc_po as (
    select
        caf_no,
        po_order_company as company_code,
        po_gl_date as date,
        0 as base_ledger_amt,
        0 as txn_ledger_amt,
        0 as phi_ledger_amt,
        0 as pcomp_ledger_amt,
        sum(base_line_open_amt) as base_line_open_amt,
        sum(txn_line_open_amt) as txn_line_open_amt,
        sum(phi_line_open_amt) as phi_line_open_amt,
        sum(pcomp_line_open_amt) as pcomp_line_open_amt
    from {{ ref('fct_wbx_fin_prc_po')}}
    where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}' and line_status = 'PO OPEN'
    group by caf_no, po_order_company, po_gl_date
),
gl_po as (
    select * from gl_trans
    union
    select * from prc_po
),
project as (
    select
        project_guid,
        project_id,
        caf_no,
        description,
        project_status_descr as project_status,
        project_group,
        project_type_descr as project_type,
        site,
        department,
        cost_center,
        plant,
        customer,
        product,
        sortingid2_descr as budget_area,
        sortingid3_descr as category,
        creation_date,
        start_date_projected,
        start_date_actual,
        end_date_projected,
        end_date_actual,
        extension_date,
        rtrim(project_controller_id, '0.') as project_controller_id,
        project_controller,
        rtrim(project_manager_id, '0.') as project_manager_id,
        project_manager,
        rtrim(sales_manager_id, '0.') as sales_manager_id,
        sales_manager

    from {{ ref('dim_wbx_project')}}
),
bug as (
    select
        project_guid,
        project_id,
        sum(original_budget) as original_budget,
        sum(committed_revisions) as committed_revisions,
        sum(uncommitted_revisions) as uncommitted_revisions,
        sum(total_budget) as total_budget
    from {{ ref('dim_wbx_budg_line')}}
    group by project_guid, project_id
),
gl_project as (
                select
                    project.project_guid,
                    project.project_id,
                    project.caf_no,
                    project.description,
                    project.project_status,
                    project.project_group,
                    project.project_type,
                    project.site,
                    project.department,
                    project.cost_center,
                    project.plant,
                    project.customer,
                    project.product,
                    project.budget_area,
                    project.category,
                    project.creation_date,
                    project.start_date_projected,
                    project.start_date_actual,
                    project.end_date_projected,
                    project.end_date_actual,
                    project.extension_date,
                    project.project_controller_id,
                    project.project_controller,
                    project.project_manager_id,
                    project.project_manager,
                    project.sales_manager_id,
                    project.sales_manager,
                    gl_po.company_code,
                    cast(gl_po.date as date) as date,
                    gl_po.base_ledger_amt,
                    gl_po.txn_ledger_amt,
                    gl_po.phi_ledger_amt,
                    gl_po.pcomp_ledger_amt,
                    gl_po.base_line_open_amt,
                    gl_po.txn_line_open_amt,
                    gl_po.phi_line_open_amt,
                    gl_po.pcomp_line_open_amt,
                    row_number() over (
                        partition by project.project_guid
                        order by cast(gl_po.date as date)
                    ) as rn
                from project                   
                left join gl_po   
                on project.caf_no = gl_po.caf_no
),
final as (
    select
        gl_project.project_guid,
        gl_project.project_id,
        gl_project.caf_no,
        gl_project.description,
        gl_project.project_status,
        gl_project.project_group,
        gl_project.project_type,
        gl_project.site,
        gl_project.department,
        gl_project.cost_center,
        gl_project.plant,
        gl_project.customer,
        gl_project.product,
        gl_project.budget_area,
        gl_project.category,
        gl_project.creation_date,
        gl_project.start_date_projected,
        gl_project.start_date_actual,
        gl_project.end_date_projected,
        gl_project.end_date_actual,
        gl_project.extension_date,
        gl_project.project_controller_id,
        gl_project.project_controller,
        gl_project.project_manager_id,
        gl_project.project_manager,
        gl_project.sales_manager_id,
        gl_project.sales_manager,
        gl_project.company_code,
        gl_project.date,
        bug.original_budget,
        bug.committed_revisions,
        bug.uncommitted_revisions,
        bug.total_budget,
        sum(base_ledger_amt)        as base_ledger_amt,
        sum(txn_ledger_amt)         as txn_ledger_amt,
        sum(phi_ledger_amt)         as phi_ledger_amt,
        sum(pcomp_ledger_amt)       as pcomp_ledger_amt,
        sum(base_line_open_amt)     as base_line_open_amt,
        sum(txn_line_open_amt)      as txn_line_open_amt,
        sum(phi_line_open_amt)      as phi_line_open_amt,
        sum(pcomp_line_open_amt)    as pcomp_line_open_amt
    from gl_project
    left join bug on gl_project.project_guid = bug.project_guid
    and gl_project.rn = 1
    group by
        gl_project.project_guid,
        gl_project.project_id,
        gl_project.caf_no,
        gl_project.description,
        gl_project.project_status,
        gl_project.project_group,
        gl_project.project_type,
        gl_project.site,
        gl_project.department,
        gl_project.cost_center,
        gl_project.plant,
        gl_project.customer,
        gl_project.product,
        gl_project.budget_area,
        gl_project.category,
        gl_project.creation_date,
        gl_project.start_date_projected,
        gl_project.start_date_actual,
        gl_project.end_date_projected,
        gl_project.end_date_actual,
        gl_project.extension_date,
        gl_project.project_controller_id,
        gl_project.project_controller,
        gl_project.project_manager_id,
        gl_project.project_manager,
        gl_project.sales_manager_id,
        gl_project.sales_manager,
        gl_project.company_code,
        gl_project.date,
        bug.original_budget,
        bug.committed_revisions,
        bug.uncommitted_revisions,
        bug.total_budget
)
select
    project_guid,
    project_id,
    caf_no,
    description,
    project_status,
    project_group,
    project_type,
    site,
    department,
    cost_center,
    plant,
    customer,
    product,
    budget_area,
    category,
    creation_date,
    start_date_projected,
    start_date_actual,
    end_date_projected,
    end_date_actual,
    extension_date,
    project_controller_id,
    project_controller,
    project_manager_id,
    project_manager,
    sales_manager_id,
    sales_manager,
    company_code,
    date,
    original_budget,
    committed_revisions,
    uncommitted_revisions,
    total_budget,
    base_ledger_amt,
    txn_ledger_amt,
    phi_ledger_amt,
    pcomp_ledger_amt,
    base_line_open_amt,
    txn_line_open_amt,
    phi_line_open_amt,
    pcomp_line_open_amt
from final
