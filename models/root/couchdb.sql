{{
    config(
        materialized = 'view',
    )
}}

SELECT
    doc->>'type' AS type,
    *
FROM {{ env_var('ROOT_POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }}