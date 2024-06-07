get_Contatos_Rotina_Atual = """
select * from contatosdiretores CD with (nolock)

    inner join ContatosDiretores_DiasSemana CS with (nolock)

        on CD.id = CS.ContatosDiretoresId

    inner join ContatosDiretores_HorarioEnvio CH with (nolock)

        on CD.id = CH.ContatosDiretoresId

    inner join DiasSemana DS with (nolock)

        on  DS.id = CS.DiasSemanaId

    inner join HorarioEnvio HE with (nolock)

        on CH.HorarioEnvioId = HE.id

where CD.ativo = 1 and CD.deleted != 1

and 

DS.dia = 

    case when convert(varchar, datepart(dw, getdate())) = 6 then 'Sexta-feira' 

    when convert(varchar, datepart(dw, getdate())) = 7 then 'Sábado' 

    when convert(varchar, datepart(dw, getdate())) = 1 then 'Domingo' 

    when convert(varchar, datepart(dw, getdate())) = 2 then 'Segunda-feira' 

    when convert(varchar, datepart(dw, getdate())) = 3 then 'Terça-feira' 

    when convert(varchar, datepart(dw, getdate())) = 4 then 'Quarta-feira' 

    when convert(varchar, datepart(dw, getdate())) = 5 then 'Quinta-feira' 

    end

    and

    left(HE.horario, 2) = FORMAT(dateadd(hour, -3, GETDATE()),'HH')
"""

get_Top5_Pecs_Com_Escalonadas = """exec [dbo].[gps_Buscar_Top5_Pecs_Escalonadas]"""

get_Gerente_Com_Escalonadas = """exec [dbo].[gps_Buscar_Escalonadas_Gerentes]"""

get_Diretor_Com_Escalonadas = """exec [dbo].[gps_Buscar_Escalonadas_Diretores]"""


get_Pecs_Com_Escalonadas = """SELECT t.EstruturaNivel1, c.Supervisor, c.Gerente, c.GerenteRegional, c.DiretorRegional, c.DiretorExecutivo, COUNT(1) AS Quantidade
    FROM Vista_replication_PRD.dbo.Tarefa t WITH (NOLOCK)	
	INNER JOIN DW_Vista.dbo.DM_ESTRUTURA B ON t.EstruturaId = B.Id_Estrutura
	INNER JOIN DW_Vista.dbo.DM_CR C ON B.Id_CR = C.Id_CR
    WHERE t.Status != 85 AND Expirada = 0 
	and Escalonado != 0  and t.estruturaid in ({ids})
    GROUP BY t.EstruturaNivel1, c.Supervisor, c.Gerente, c.GerenteRegional, c.DiretorRegional, c.DiretorExecutivo
"""

get_Pecs_Por_Filtro = """SET NOCOUNT ON; 
exec dbo.gps_Buscar_Filtros_Por_Contato  @ContatosDiretoresId = '{id}', @OrdemGrupo = {ordem};
"""
get_Pecs_Com_Escalonadas_top10 = """SELECT top 10 
                                            SUM(CASE WHEN Escalonado >= 4 THEN 1 END) AS DIRETOR_REGIONAL_quantidade,
                                            c.DiretorRegional, A.EstruturaNivel1 
                                        FROM
                                            Vista_Replication_PRD.dbo.Tarefa A WITH (NOLOCK),
                                            DW_Vista.dbo.DM_ESTRUTURA B WITH (NOLOCK),
                                            DW_Vista.dbo.DM_CR C WITH (NOLOCK)
                                        WHERE
                                            1 = 1
                                            AND A.EstruturaId = B.Id_Estrutura
                                            AND B.Id_CR = C.Id_CR
                                            and c.DiretorRegional != 'null'
                                            and c.DiretorRegional != ''
                                        GROUP BY
                                            c.DiretorRegional,A.EstruturaNivel1
                                        HAVING
                                            SUM(CASE WHEN Escalonado >= 4 THEN 1 END) IS NOT NULL
                                        order by
                                            SUM(CASE WHEN Escalonado >= 4 THEN 1 END) desc"""

get_Quantidade_Escalonadas_Por_Diretor = """exec [dbo].[gps_Buscar_Soma_Escalonada_Diretores]"""

get_Pecs_Por_Contato = """SELECT
    C.ContatosDiretoresId,
    C.PecsId,
    P.Descricao as NomePec
FROM
    ContatosDiretores_pecs C
INNER JOIN
  Vista_Replication_PRD.[dbo].Estrutura P
ON P.Id = C.PecsId
where C.ContatosDiretoresId in ({contatos})"""

get_nivel_escalonamento = """select * from ContatosDiretores_NivelEscalonamento where 
ContatosDiretoresId in ({contatos})"""

get_filtro_valor = """select * from contatos_filtro where ContatosDiretoresId in ({contatos})"""

get_pecs_filtradas = """"""

get_comparison_filtro = """select  cf.filtro as filtro, cf.valor as valor, a.codigo, v.conteudo from  Vista_Replication_PRD.[dbo].Estrutura e with (nolock ) inner
join  Vista_Replication_PRD.[dbo].valorAtributoNivel v on e.id = v.estruturaid
inner join  Vista_Replication_PRD.[dbo].AtributoNivel a on a.id = v.AtributoId
inner join Contatos_Filtro cf on a.codigo = cf.filtro
where a.codigo in ({filtro}) and v.conteudo in ({valor})"""
#data
DIRETOR = 4
GERENTE = 3

