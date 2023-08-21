{{ config (
    materialized="table"
)}}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('bi_sponte', '_airbyte_raw_contacts') }}
select c.label , a.id , b.dealname, b.nome_fantasia,  b.data_ganhou, b.data_sql , b.data_mql, b.codigo_do_cliente 
From BI_Sponte.deals a
left join BI_Sponte.deals_properties b on a._airbyte_deals_hashid = b._airbyte_deals_hashid
left join BI_Sponte.deal_pipelines c on c.pipelineid = b.pipeline 
where b.data_mql >= '2023-01-01'
order by b.data_ganhou desc

