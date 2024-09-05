
/* Do not yet have a proper D365 or D365S structure w/ data as there is no data to replicate, yet.  So for now defaulting to pull from AX w/ a filter to zero rows.
Once we have a proper structure from D365S, then can use the commented out code.
*/

select * from {{ ref("src_ax_workperiodtemplateline") }} 
where 0=1