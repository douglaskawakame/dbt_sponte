{{ config (
    materialized="table"
)}}

select c.label , a.id , b.dealname, b.nome_fantasia,  b.data_ganhou, b.data_sql , b.data_mql, b.codigo_do_cliente 
From bi_sponte.deals a
left join bi_sponte.deals_properties b on a._airbyte_deals_hashid = b._airbyte_deals_hashid
left join bi_sponte.deal_pipelines c on c.pipelineid = b.pipeline 
where b.data_mql >= '2023-01-01'


