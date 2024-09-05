

with source as (

    select * from {{ source('WEETABIX', 'DBIX_tblRefAXLedgerTable') }}

),

renamed as (

    select
KLEDGERTABLE,
ACCOUNTNUM,
ACCOUNTNAME,
ACCOUNTPLTYPE,
OFFSETACCOUNT,
LEDGERCLOSING,
TAXGROUP,
BLOCKEDINJOURNAL,
DEBCREDPROPOSAL,
DIMENSION,
DIMENSION2_,
DIMENSION3_,
CONVERSIONPRINCIPLE,
OPENINGACCOUNT,
COMPANYGROUPACCOUNT,
DIMSPEC,
TAXCODE,
MANDATORYTAXCODE,
CURRENCYCODE,
MANDATORYCURRENCY,
AUTOALLOCATE,
POSTING,
MANDATORYPOSTING,
USER_,
MANDATORYUSER,
DEBCREDCHECK,
REVERSESIGN,
MANDATORYDIMENSION,
MANDATORYDIMENSION2_,
MANDATORYDIMENSION3_,
COLUMN_,
TAXDIRECTION,
LINESUB,
LINEEXCEED,
UNDERLINENUMERALS,
UNDERLINETXT,
ITALIC,
BOLDTYPEFACE,
EXCHADJUSTED,
ACCOUNTNAMEALIAS,
CLOSED,
DEBCREDBALANCEDEMAND,
TAXFREE,
TAXITEMGROUP,
MONETARY,
ACCOUNTCATEGORREF,
upper(DATAAREAID) as DATAAREAID,
RECVERSION,
RECID,
DEL_INCLUDEINFISCALJOURNAL_IT,
DEL_LEDGERSETTLEMENT,
DIMENSION4_,
MANDATORYDIMENSION4_,
DIMENSION5_,
MANDATORYDIMENSION5_,
DIMENSION6_,
MANDATORYDIMENSION6_,
COSTCENTRE,
KCOA_LINE,
DLA_CARP_PRD_LEVEL,
DLA_CARP_VALUEORWEIGHT,
IGNOREGL,
IGNOREGLTXT

from source

)

select * from renamed