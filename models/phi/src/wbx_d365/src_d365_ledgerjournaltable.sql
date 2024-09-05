with
    d365_source as (
        select *
        from {{ source("D365", "ledger_journal_table") }} where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    ),
renamed as (


    select 
        'D365' as source,
        journal_num as journalnum,
        name as name,
        log as log,
        journal_type as journaltype,
        workflow_approval_status as workflowapprovalstatus,
        system_blocked as systemblocked,
        payments_generated_it as paymentsgenerated_it,
        null as userblockid,
        rejected_by as rejectedby,
        reported_as_ready_by as reportedasreadyby,
        journal_name as journalname,
        posted as posted,
        session_login_date_time as sessionlogindatetime,
        sessionlogindatetimetzid as sessionlogindatetimetzid,
        offset_account_type as offsetaccounttype,
        null as inuseby,
        null as groupblockid,
        original_journal_num as originaljournalnum,
        currency_code as currencycode,
        fixed_exch_rate as fixedexchrate,
        detail_summary_posting as detailsummaryposting,
        null as documentnum,
        exchrate_secondary as exchratesecondary,
        exch_rate as exchrate,
        eurotriangulation as eurotriangulation,
        fixed_offset_account as fixedoffsetaccount,
        journal_total_credit as journaltotalcredit,
        journal_total_debit as journaltotaldebit,
        journal_total_offset_balance as journaltotaloffsetbalance,
        removelineafterposting as removelineafterposting,
        current_operations_tax as currentoperationstax,
        ledger_journal_incl_tax as ledgerjournalincltax,
        original_company as originalcompany,
        session_id as sessionid,
        bank_remittance_type as bankremittancetype,
        null as bankaccountid,
        protest_settled_bill as protestsettledbill,
        journal_balance as journalbalance,
        end_balance as endbalance,
        cust_vend_neg_inst_protest_process as custvendneginstprotestprocess,
        voucher_allocated_at_posting as voucherallocatedatposting,
        num_of_lines as numoflines,
        lines_limit_before_distribution as lineslimitbeforedistribution,
        posted_date_time as posteddatetime,
        posteddatetimetzid as posteddatetimetzid,
        reverse_entry as reverseentry,
        reverse_date as reversedate,
        default_dimension as defaultdimension,
        offset_ledger_dimension as offsetledgerdimension,
        approver as approver,
        number_sequence_table as numbersequencetable,
        asset_transfer_type_lt as assettransfertype_lt,
        null as retailstatementid,
        tax_obligation_company as taxobligationcompany,
        modifiedby as modifiedby,
        createdby as createdby,
        upper(data_area_id) as dataareaid,
        recversion as recversion,
        partition as partition,
       recid  as recid
    from d365_source


)

select * from renamed 
