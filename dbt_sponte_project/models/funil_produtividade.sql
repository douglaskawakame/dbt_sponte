{{ config (
    materialized="table"
)}}

with mql as(
	select *, 
	'MQL' categoria2,
	data_mql date
	from bi_sponte.deals_properties
	where data_mql is not null
	and data_mql >= '2022-01-01'
),

sql as(
	select *, 
	'SQL' categoria2,
	data_sql date
	from bi_sponte.deals_properties
	where data_sql is not null
	and data_sql >= '2022-01-01'
),

ganhou as(
	select *, 
	'GANHOU' categoria2,
	data_ganhou date
	from bi_sponte.deals_properties
	where data_ganhou is not null
	and data_ganhou >= '2022-01-01'
),

perdeu as(
	select *, 
	'PERDEU' categoria2,
	data_perdeu date
	from bi_sponte.deals_properties
	where data_perdeu is not null
	and data_perdeu >= '2022-01-01'
),

realizado as(
	select 'Realizado' visao,
	p.label pipeline, 
	date(date_trunc('month', date)) mes,
	b.date, 
	b.categoria2 categoria,
	a.id, 
	b.dealname nome, 
	coalesce(b.sponte__franquia_ou_sistema_de_ensino, 'Nenhum(a)') franquia,
	b.sponte__numero_de_alunos num_alunos,
	case 
		when b.indicacao = 'Não é indicação' or b.indicacao is null 
		then 'Digital'
		else 'Indicação' 
	end canal,
	b.fonte_mais_recente,
	case 
		when p.label = 'MedPlus Sales' then 'Medplus'
		else b.sponte__segmento_macro
	end segmento_macro,
	case 
		when p.label = 'MedPlus Sales' then 'Medplus'
		when p.label = 'Sponte Sales' then b.sponte_produto
		else b.servicosadicionais__categoria
	end produto,
	o.firstname vendedor,
	1 cont,
	case when b.categoria2 = 'MQL' then 1 else 0 end mql,
	case when b.categoria2 = 'SQL' then 1 else 0 end as sql,
	case when b.categoria2 = 'GANHOU' then 1 else 0 end ganhou,
	0 budget,
	0 bgt_mql,
	0 bgt_sql,
	0 bgt_ganhou
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
	) b
	left join bi_sponte.deals a on a."_airbyte_deals_hashid" = b."_airbyte_deals_hashid" 
	left join bi_sponte.deal_pipelines p on p.pipelineid = b.pipeline
	left join bi_sponte.owners o on o.id = b.vendedor 
),

budget as (
	select 
		'Budget' visao,
		case
			when produto in ('Ensino Regular', 'Cursos Livres', 'Idiomas') then 'Sponte Sales'
			when produto = 'Medplus' then 'MedPlus Sales'
			else null
		end pipeline,
		date(date_trunc('month', date(data))) mes,
		date(data) date,
		case
			when descr_macro = 'Vendas' then 'GANHOU'
			else descr_macro
		end categoria,
		null id,
		null nome,
		'Nenhum(a)' franquia,
		null :: int as num_alunos,
		split_part(descr_completa, ' ', 2) canal,
		null fonte_mais_recente,
		produto segmento_macro, 
		case
			when produto in ('Ensino Regular', 'Cursos Livres') then 'Educacional'
			else produto
		end produto,
		null vendedor,
		0 cont,
		0 mql,
		0 as sql,
		0 ganhou,
		valor budget,
		case when descr_macro = 'MQL' then 1 else 0 end bgt_mql,
		case when descr_macro = 'SQL' then 1 else 0 end as bgt_sql,
		case when descr_macro = 'Vendas' then 1 else 0 end bgt_ganhou
	from bi_sponte.budget
	where tipo = 'Funil'
	and descr_macro != 'Leads'
	and produto != 'Venda Total'
)


select a.*
from(
	select * from realizado
	
	union all
	
	select * from budget
) a
