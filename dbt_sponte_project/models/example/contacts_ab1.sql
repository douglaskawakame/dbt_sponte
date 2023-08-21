{{ config (
    materialized="table"
)}}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('bi_sponte', '_airbyte_raw_contacts') }}
select
    {{ json_extract_scalar('_airbyte_data', ['archived'], ['archived']) }} as archived,
    {{ json_extract_scalar('_airbyte_data', ['createdAt'], ['createdAt']) }} as createdat,
    {{ json_extract_string_array('_airbyte_data', ['companies'], ['companies']) }} as companies,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract('table_alias', '_airbyte_data', ['properties'], ['properties']) }} as properties_teste,
    {{ json_extract_scalar('_airbyte_data', ['updatedAt'], ['updatedAt']) }} as updatedat,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('bi_sponte', '_airbyte_raw_contacts') }} as table_alias
-- contacts
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

