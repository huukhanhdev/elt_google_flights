-- macros/generate_schema_name.sql
--
-- Override dbt default behavior:
-- By default dbt creates schema as "{target_schema}_{custom_schema}" (e.g. staging_staging).
-- This macro makes dbt use the EXACT schema name specified in dbt_project.yml.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
