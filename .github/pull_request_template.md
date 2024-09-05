## Description & motivation: 
<!--
Describe your changes, and why you're making them. Is this linked to an open Jira ticket, or another pull request? Link it here.
-->


## To-do before merge:
**(Optional -- remove this section if not needed)**
<!--
Include any notes about things that need to happen before this PR is merged.
-->


## Screenshots:
<!--
Include screenshot(s) of the relevant section of the updated DAG. You can access your version of the DAG by running `dbt docs generate && dbt docs serve`.
-->


## Validation of models:
<!--
Include any output that confirms that the models do what is expected. This might be a link to an in-development dashboard in Tableau or BI tool, or a query that compares an existing model with a new one, or output of `dbt build` command execution.
-->


## Changes to existing models:
<!--
Include this section if you are changing any existing models. Link any related pull requests on Tableau or BI tool, or instructions for merge (e.g. whether old models should be dropped after merge, or whether a full-refresh run is required)
-->


## Scheduling:
**(Optional -- remove this section if not needed but you must fill it out if scheduling is required for your job)**
<!--
When it is time to fill this out it will, in all likelihood, require input and assistance from those who provided the initial requirements as well as those maintaining production and scheduling in order to determine all that is required here. 
In general, this should provide details like:

>Job #1 logical Name  `  `
>
>What is the dbt command to run your pipeline?  `  `
>
>Is this a new job?  `Yes/No`
>
>Is this job part of the daily critical processing window?  `Yes/No`
>
>What should be the frequency of this job run?  `daily/weekly/other(provide details if necessary)`
>
>Provide specific job start time, if applicable.  `  `
>
>What are its prerequisites?  `  `
>
>List the jobs (logical or physical) that job #1 is dependent upon, if any.  `  `
>
>List any existing jobs (logical or physical) that Job #1 is to be a pre-requisite for, if any.  `  `
>
>Expected job run time (best approximation)  `  `
-->


## Failure Strategy:
<!--
Include action required to resolve the issue if your changes cause production failure. _e.g.: revert this merge request to return branch to its previous state_
-->


## Checklist:
<!--
This checklist is mostly useful as a reminder of small things that can easily be forgotten – it is meant as a helpful tool rather than hoops to jump through.
-->
Put an `x` in all the items that apply, make notes next to any that haven't been addressed, **and remove any items that are not relevant to this PR**.

- [ ] My pull request represents one logical piece of work.
- [ ] My commits are related to the pull request and look clean.
- [ ] My SQL follows the [dbt guide](https://postholdings.sharepoint.com/:b:/r/sites/PHI/shares/IT/decision_science/Shared%20Documents/Operating%20Companies%20-%20Shared/ENT%20-%20Shared/Self%20Service/Analytic%20Technologies/dbt%20Training%20%26%20Documents/dbt%20User%20Guide.pdf?csf=1&web=1&e=8OlkX6).
- [ ] I have materialized my models appropriately.
- [ ] I have followed all of the standard naming conventions.
- [ ] I have created models in their desired folder locations.
- [ ] I have provided failure strategy in case there are failures caused due to my changes.
- [ ] I have used re-used existing models when/where applicable.
- [ ] I have re-used existing macros or jinja functions when/where applicable.
- [ ] I have added appropriate tests and documentation to any new models.
- [ ] I have created new macros or jinja functions.
- [ ] I have modified existing macros or jinja functions.
- [ ] I have added new package(s) to packages.yml file.
- [ ] My pull request will modify dbt_project.yml file.