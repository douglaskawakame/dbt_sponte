{{ config (
    materialized="table"
)}}

with mql as(
	select 
	b.pipeline, 
	b.data_mql date, 
	'MQL' categoria,
	a.id, 
	b.dealname nome, 
	b.sponte__franquia_ou_sistema_de_ensino,
	b.sponte__numero_de_alunos,
	b.indicacao,	
	b.fonte_mais_recente,
	b.sponte__segmento_macro,
	b.sponte_produto,
	b.servicosadicionais__categoria,
	b.vendedor
	--b.codigo_do_cliente 
	from bi_sponte.deals a
	left join bi_sponte.deals_properties b on a."_airbyte_deals_hashid" = b."_airbyte_deals_hashid" 
	where b.data_mql is not null
	and b.data_mql >= '2022-01-01'
),

sql as(
	select 
	b.pipeline, 
	b.data_sql date, 
	'SQL' categoria,
	--b.data_ganhou, 
	--b.data_perdeu,
	a.id, 
	b.dealname nome, 
	b.sponte__franquia_ou_sistema_de_ensino,
	b.sponte__numero_de_alunos,
	b.indicacao,
	b.fonte_mais_recente,
	b.sponte__segmento_macro,
	b.sponte_produto,
	b.servicosadicionais__categoria,
	b.vendedor
	--b.codigo_do_cliente 
	from bi_sponte.deals a
	left join bi_sponte.deals_properties b on a."_airbyte_deals_hashid" = b."_airbyte_deals_hashid" 
	where b.data_sql is not null
	and b.data_sql >= '2022-01-01'
),

ganhou as(
	select 
	b.pipeline, 
	b.data_ganhou date, 
	'GANHOU' categoria,
	a.id, 
	b.dealname nome, 
	b.sponte__franquia_ou_sistema_de_ensino,
	b.sponte__numero_de_alunos,
	b.indicacao,
	b.fonte_mais_recente,
	b.sponte__segmento_macro,
	b.sponte_produto,
	b.servicosadicionais__categoria,
	b.vendedor
	--b.codigo_do_cliente 
	from bi_sponte.deals a
	left join bi_sponte.deals_properties b on a."_airbyte_deals_hashid" = b."_airbyte_deals_hashid" 
	where b.data_ganhou is not null
	and b.data_ganhou >= '2022-01-01'
),

perdeu as(
	select 
	b.pipeline, 
	b.data_perdeu date, 
	'PERDEU' categoria,
	a.id, 
	b.dealname nome, 
	b.sponte__franquia_ou_sistema_de_ensino,
	b.sponte__numero_de_alunos,
	b.indicacao,
	b.fonte_mais_recente,
	b.sponte__segmento_macro,
	b.sponte_produto,
	b.servicosadicionais__categoria,	
	b.vendedor
	--b.codigo_do_cliente 
	from bi_sponte.deals a
	left join bi_sponte.deals_properties b on a."_airbyte_deals_hashid" = b."_airbyte_deals_hashid" 
	where b.data_perdeu is not null
	and b.data_perdeu >= '2022-01-01'
)


select 
	p.label pipeline,
	date(date_trunc('month', date)) mes,
	a.date,
	a.categoria,
	a.id,
	a.nome,
	coalesce(a.sponte__franquia_ou_sistema_de_ensino, 'Nenhum(a)') franquia,
	case 
		when a.indicacao = 'Não é indicação' or a.indicacao is null 
		then 'Digital' 
		else 'Indicação' 
	end canal,
	a.sponte__numero_de_alunos num_alunos,
	a.fonte_mais_recente,
	a.sponte__segmento_macro,
	case 
		when p.label in ('MedPlus Sales', 'Sponte Sales')
		then a.sponte_produto
		else a.servicosadicionais__categoria
	end produto,	
	o.firstname vendedor
from(
	select *
	from mql
	
	union all
	
	select *
	from sql
	
	union all
	
	select *
	from ganhou
	
	union all
	
	select *
	from perdeu
) a
left join bi_sponte.deal_pipelines p on p.pipelineid = a.pipeline
left join bi_sponte.owners o on o.id = a.vendedor 




