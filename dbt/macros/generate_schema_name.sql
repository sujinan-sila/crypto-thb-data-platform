{#- Use silver / gold as-is (dbt would otherwise make gold_silver).
    The ci target writes to ci_silver / ci_gold so CI never touches real tables. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set name = (custom_schema_name or target.schema) | trim -%}
    {%- if target.name == 'ci' -%}
        ci_{{ name }}
    {%- else -%}
        {{ name }}
    {%- endif -%}
{%- endmacro %}