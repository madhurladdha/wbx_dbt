# wbx_dbt
dbt repository for Weetabix
---
For more information on dbt:
- Read the [Introduction to dbt](https://docs.getdbt.com/docs/introduction).
- Read the [dbt viewpoint](https://docs.getdbt.com/docs/about/viewpoint).
- Join the [dbt chat](http://slack.getdbt.com/) on Slack for live questions and support.

For more information on standards for dbt:
- Read the [dbt Guide](https://postholdings.sharepoint.com/:w:/r/sites/PHI/shares/IT/decision_science/_layouts/15/Doc.aspx?sourcedoc=%7BC9780E0C-18F8-449F-8636-096FE0E14089%7D&file=dbt%20Architecture.docx&nav=eyJjIjo2NDE5OTkwMDR9&action=default&mobileredirect=true)
- Read the [dbt RDM Dimension Build Guidelines](https://postholdings.sharepoint.com/:w:/r/sites/PHI/shares/IT/decision_science/_layouts/15/Doc.aspx?sourcedoc=%7BE6946495-6EA7-4B18-AF9B-6155254A0936%7D&file=dbt%20RDM%20Dimension%20Build%20Guidelines.docx&action=default&mobileredirect=true&cid=4196f616-1796-48ed-9f0c-0ef5bcd65350)
- Read the [dbt Testing](https://postholdings.sharepoint.com/:w:/r/sites/PHI/shares/IT/decision_science/_layouts/15/Doc.aspx?sourcedoc=%7B077FBCC5-ED2B-4BE4-AEFE-3F186762E8FF%7D&file=dbt%20Testing.docx&action=default&mobileredirect=true&cid=20f5986c-bd31-4a39-8c1e-1f1e50d5ef44)

# dbt Style Guide

## Model Naming
dbt models (typically) fit into four main categories: staging, intermediate, dimension & fact. For more detail about why we use this structure, check out [dbt Guide](https://postholdings.sharepoint.com/:w:/r/sites/PHI/shares/IT/decision_science/_layouts/15/Doc.aspx?sourcedoc=%7BC9780E0C-18F8-449F-8636-096FE0E14089%7D&file=dbt%20Architecture.docx&nav=eyJjIjo2NDE5OTkwMDR9&action=default&mobileredirect=true).
All Table name should imply classification of data (dim/fact/stage/intermediate), OC, Domain, type of data for Stage, Source, Intermediate.

| Object Type               | Folder   | Model Naming Format                                        | Example                  |
| ------------------------- | -------- | ---------------------------------------------------------- | ------------------------ |
| Source                    | src      | `src_<Table Content>`                                      | `src_customers`          |
| Stage (Dimension)         | stg_dim  | `stg_d_<OC Entity/Source System>_<Module>_<Table Content>` | `stg_d_phi_customers`    |
| Intermediate (Dimension)  | int_dim  | `int_d_<OC Entity/Source System>_<Module>_<Table Content>` | `int_d_phi_customers`    |
| Dimension                 | dim      | `dim_<OC Entity/Source System>_<Module>_<Table Content>`   | `dim_phi_customers`      |
| Cross-Reference (xref)    | dim      | `xref_<OC Entity/Source System>_<Module>_<Table Content>`  | `xref_phi_customers`     |
| Stage (Fact)              | stg_fct  | `stg_f_<OC Entity/Source System>_<Module>_<Table Content>` | `stg_f_phi_inv_itemcost` |
| Intermediate (Fact)       | int_fct  | `int_f_<OC Entity/Source System>_<Module>_<Table Content>` | `int_f_phi_inv_itemcost` |
| Fact                      | fct      | `fct_<OC Entity/Source System>_<Module>_<Table Content>`   | `fct_phi_inv_itemcost`   |
| Tableau Reporting         | rpt      | `rpt_<OC Entity/Source System>_<Module>_<Table Content>`   | `rpt_phi_inv_itemcost`   |

The file and naming structures are as follows:
```
├── dbt_project.yml
└── models
|   ├── <OpCo>
|       └── src
|       |   ├── _sources_<schema>.yml
|       |   ├── _tests.yml
|       |   └── src_customers.sql
|       |   └── src_<Table Content>.sql
|       └── stg_dim
|       |   ├── _sources_<schema>.yml
|       |   ├── _tests.yml
|       |   └── stg_d_phi_customers.sql
|       |   └── stg_d_<OC Entity/Source System>_<Table Content>.sql
|       └── int_dim
|       |   ├── _sources_<schema>.yml
|       |   ├── _tests.yml
|       |   └── int_d_phi_customers.sql    
|       |   └── int_d_<OC Entity/Source System>_<Table Content>.sql    
|       └── dim
|       |   ├── _sources_<schema>.yml
|       |   ├── _tests.yml
|       |   └── dim_phi_customers.sql
|       |   └── dim_<OC Entity/Source System>_<Table Content>.sql
|       |   └── xref_<OC Entity/Source System>_<Table Content>.sql
|       └── stg_fct
|       |   ├── _sources_<schema>.yml
|       |   ├── _tests.yml
|       |   └── stg_f_phi_inv_itemcost.sql
|       |   └── stg_f_<OC Entity/Source System>_<Module>_<Table Content>.sql
|       └── int_fct
|       |   ├── _sources_<schema>.yml
|       |   ├── _tests.yml
|       |   └── int_f_phi_inv_itemcost.sql
|       |   └── int_f_<OC Entity/Source System>_<Module>_<Table Content>.sql
|       └── fct
|       |   ├── _sources_<schema>.yml
|       |   ├── _tests.yml
|       |   └── fct_phi_inv_itemcost.sql
|       |   └── fct_<OC Entity/Source System>_<Module>_<Table Content>.sql
|       └── rpt
|       |   ├── _sources_<schema>.yml
|       |   ├── _tests.yml
|       |   └── rpt_phi_inv_itemcost.sql
|       |   └── rpt_<OC Entity/Source System>_<Module>_<Table Content>.sql
└── tests
|   ├── <OpCo>  
|       └── dim
|       |   └── test_except_dim_<OC Entity/Source System>_<Table Content>.sql
|       |   └── test_except_dim_phi_customers.sql
|       |   └── test_compare_relations_<OC Entity/Source System>_<Table Content>.sql
|       |   └── test_compare_relations_dim_customers.sql
|       |   └── test_aggregate_<OC Entity/Source System>_<Table Content>.sql
|       |   └── test_aggregate_dim_customers.sql
|       └── fct
|       |   └── test_except_fct_<OC Entity/Source System>_<Module>_<Table Content>.sql
|       |   └── test_except_fct_phi_inv_itemcost.sql
|       |   └── test_compare_relations_fct_<OC Entity/Source System>_<Module>_<Table Content>.sql
|       |   └── test_compare_relations_fct_phi_inv_itemcost.sql
|       |   └── test_aggregate_fct_<OC Entity/Source System>_<Module>_<Table Content>.sql
|       |   └── test_aggregate_fct_phi_inv_itemcost.sql
|       └── rpt
|       |   └── test_except_rpt_<OC Entity/Source System>_<Module>_<Table Content>.sql
|       |   └── test_except_rpt_phi_inv_itemcost.sql
|       |   └── test_compare_relations_rpt_<OC Entity/Source System>_<Module>_<Table Content>.sql
|       |   └── test_compare_relations_rpt_phi_inv_itemcost.sql
|       |   └── test_aggregate_rpt_<OC Entity/Source System>_<Module>_<Table Content>.sql
|       |   └── test_aggregate_rpt_phi_inv_itemcost.sql
```

## Model configuration

- All Source models start with `src_` prefix must be created in `src` folder in models directory.
- All Staging models that feed into Dimensions that start with `stg_d_` prefix must be created in `stg_dim` folder in models directory.
- All Intermediate models that feed into Dimensions that start with `int_d_` prefix must be created in `int_dim` folder in models directory.
- All Dimension models that start with `dim_` prefix must be created in `dim` folder in models directory.
- All Cross-reference models that start with `xref_` prefix must be created in `dim` folder in models directory.
- All Staging models that feeds into Facts that start with `stg_f_` prefix must be created in `stg_fct` folder in models directory.
- All Intermediate models that feed into Facts that start with `int_f_` prefix must be created in `int_fct` folder in models directory.
- All Fact models that start with `fct_` prefix must be created in `fct` folder in models directory.
- All Reporting models that start with `rpt_` prefix must be created in `rpt` folder in models directory.- Model-specific attributes (like unique_key, transient, on_schema_change) should be specified in the model.
- If a particular configuration applies to all models in a directory, it should be specified in the `dbt_project.yml` file and dbt admins will handle that upon request.
- Final Dimensions & Facts should always be configured as tables and in-model configurations should be specified like this:
```python
{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns'
    )
}}
```

## YAML style guide

* Indents should be two spaces
* List items should be indented
* Use a new line to separate list items that are dictionaries where appropriate
* Lines of YAML should be no longer than 80 characters.

### Example YAML
```yaml
version: 2

models:
  - name: events
    columns:
      - name: event_id
        description: This is a unique identifier for the event
        tests:
          - unique
          - not_null

      - name: event_time
        description: "When the event occurred in UTC (eg. 2018-01-01 12:00:00)"
        tests:
          - not_null

      - name: user_id
        description: The ID of the user who recorded the event
        tests:
          - not_null
          - relationships:
              to: ref('users')
              field: id
```
## Acronyms
| Module Name   | Definition                    |
| ------------- | ----------------------------- |
| SLS           | Sales                         |
| FIN           | Finance                       |
| INV           | Inventory                     |
| PRC           | Procurement                   |
| MFG           | Manufacturing                 |
| MKT           | Marketing                     |
| KPI           | Key Performance Indicator     |
| SEC           | Security                      |
| PRJ           | ML Projection                 |
| EC            | E-Commerce                    |

| Entity/Source     | Definition                        |
| ----------------- | --------------------------------- |
| ENT               | Enterprise - Corporate            |
| PHI               | Post Holdings                     |
| PCB               | Post Consumer Brands              |
| AB                | Animated Brands                   |
| BEF               | Bob Evans Foods                   |
| MFI               | Michael Foods Inc                 |
| CFW               | Crystal Farms & Willamette        |
| WBX               | Weetabix                          |
| 8AVE              | 8th Avenue                        |
| BRBR              | Bell Ring                         |
| PNC               | Premier Nutrition                 |
| AN                | Active Nutrition                  |
| ANI               | Active Nutrition International    |
| DYM               | Dymatize                          |
| ATT               | Attune                            |
| GB                | Golden Boy                        |
| DAG               | Dakota Growers                    |

## CTEs

For more information about why we use so many CTEs, check out [this discourse post](https://discourse.getdbt.com/t/why-the-fishtown-sql-style-guide-uses-so-many-ctes/1091).

- All `{{ ref('...') }}` statements should be placed in CTEs at the top of the file
- Where performance permits, CTEs should perform a single, logical unit of work.
- CTE names should be as verbose as needed to convey what they do
- CTEs with confusing or noteable logic should be commented
- CTEs that are duplicated across models should be pulled out into their own models
- create a `final` or similar CTE that you select from as your last line of code. This makes it easier to debug code within a model (without having to comment out code!)
- CTEs should be formatted like this:

``` sql
with

events as (

    ...

),

-- CTE comments go here
filtered_events as (

    ...

)

select * from filtered_events
```



---
