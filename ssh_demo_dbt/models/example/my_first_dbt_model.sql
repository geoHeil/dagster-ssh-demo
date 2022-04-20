
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    SELECT * FROM {{ source("ssh_demo", "combined_asset") }}
    /*select 1 as id
    union all
    select null as id
    */

)

select *, 1 as id
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
