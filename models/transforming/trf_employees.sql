{{config(materialized='table', schema=env_var('DBT_TRANSFORMSCHEMA', 'transforming_dev'))}}

with recursive managers 
      -- Column names for the "view"/CTE
    (indent, employee_id, employee_name, employee_title, manager_id, manager_name, manager_title,office) 
    as
      -- Common Table Expression
      (
 
        -- Anchor Clause
        select '*' as indent, empid as employee_id, first_name as employee_name, title as employee_title, empid as manager_id, first_name as manager_name, title as manager_title,office
          from {{ref('stg_employees')}}
          where title = 'President'
 
        union all
 
        -- Recursive Clause
        select indent || '*',
            e.empid as employee_id, e.first_name as employee_name, e.title as employee_title,
            m.employee_id, m.employee_name, m.employee_title,e.office
          from {{ref('stg_employees')}} as e join managers as m
            on e.reports_to = m.employee_id
      ),
offices (officeid, officecity, officestate, officecountry)
      as
      (
      select ho.office, so.officecity, so.officestateprovince, so.officecountry
      from {{ref('stg_hub_empoffice')}} as ho inner join {{ref('stg_sat_empoffice')}} as so
      on ho.officehashkey = so.officehashkey
 
      )
select m.indent, m.employee_id, m.employee_name, m.employee_title, m.manager_id, m.manager_name, m. manager_title, o.officecity,
  IFF(o.officestate is null, 'NA', o.officestate) as officestate,
  o.officecountry
    from managers as m left join offices as o on
    m.office = o.officeid