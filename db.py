import pyodbc
import json
import pandas as pd

import queries


# ---- PARAMS AND LOG ----
#Database credentials
DB_ADDRESS = '...'
DB_DATABASE = '...'
DB_USERNAME = '...'
DB_PASSWORD = '...'

#Conx string create based on given info
CONX_STRING = ('Driver={SQL Server};'
        'Server=' + DB_ADDRESS +
        ';Database=' + DB_DATABASE +
        ';UID=' + DB_USERNAME +
        ';PWD=' + DB_PASSWORD + ';')


# ---- OBJETO DE CONEXAO ----
#define a conexão para o banco
conn = pyodbc.connect(CONX_STRING)
cursor = conn.cursor()


# ---- MÉTODOS LOG ----
#define a conexão para o banco
def setQueryInsertLog(dataInicio,  dataFim, status, erro=None): 
    if erro is not None:
        erro_str = str(erro).replace("'", "''")
        dbInsertLog(f"""insert into LogsWhatsappDiretores (id, data_inicio, data_fim, status, erro)  values(NEWID(), '{dataInicio}', '{dataFim}', '{status}', '{erro_str}' )""")
        
    else: 
        dbInsertLog(f"""insert into LogsWhatsappDiretores (id, data_inicio, data_fim, status) values(NEWID(), '{dataInicio}', '{dataFim}', '{status}')""")

#DB insert new log

#DB insert new log
def dbInsertLog(queryLogs):
    try:
        cursor.execute(queryLogs)
        
        conn.commit()
        cursor.close()
           
    except Exception as e:
        return(e)

# ---- RETORNA OS CONTATOS FILTRADOS ----
#Recebe todos os dados filtrados relacionados ao envio atual
def get_Contatos_Rotina_Atual():
    try:
        return pd.read_sql(queries.get_Contatos_Rotina_Atual, conn)      
    except Exception as e:
        raise e

def get_Pecs_escalonadas_top10():
    try:
        return pd.read_sql(queries.get_Pecs_Com_Escalonadas_top10, conn)      
    except Exception as e:
        raise e

# ---- RETORNA AS 5 PECS COM MAIS TAREFAS ESCALONADAS ----
def get_Top5_Pecs_Com_Escalonadas():
    try:
        return pd.read_sql(queries.get_Top5_Pecs_Com_Escalonadas, conn)      
    except Exception as e:
        raise e
    
# ---- RETORNA os gerentes 3----
#busca todos os gerentes que possuem tarefas escalonadas (escalonamento 3)
def get_Gerente_Com_Escalonadas():
    try:
        return pd.read_sql(queries.get_Gerente_Com_Escalonadas, conn)      
    except Exception as e:
        raise e

# ---- RETORNA os diretores 4 e 5 com escalonadas----
#busca todos os diretores regionais e executivos  que possuem tarefas escalonadas (escalonamento 4 e 5)
def get_Diretor_Com_Escalonadas():
    try:
        return pd.read_sql(queries.get_Diretor_Com_Escalonadas, conn)      
    except Exception as e:
        raise e

# ---- RETORNA os pecs com escalonadas 3 e 4----
#busca todos os pecs, responsavel e quantidade de tarefas com escalonada (escalonamento 3 e 4)
def get_Pecs_Com_Escalonadas(df_Pecs_Por_Filtro):
    try:        
        valuesPecs =  ",".join("'{}'".format(valor) for valor in df_Pecs_Por_Filtro['id'].unique())
        return pd.read_sql(queries.get_Pecs_Com_Escalonadas.format(ids=valuesPecs), conn)    
    except Exception as e:
        raise e  

def get_Quantidade_Escalonadas_Por_Diretor():
    try:
        return pd.read_sql(queries.get_Quantidade_Escalonadas_Por_Diretor, conn)      
    except Exception as e:
        raise e

     
# ---- RETORNA a quantidade de pecs nivel 3 e 4 por diretor responsavel pelo escalonamento----
#busca a quantidade de tarefas escalonadas para os diretores regionais mais 
#as tarefas que escalonaram para os diretores executivos (nome diretor regional que deixou escalonar)
def get_Quantidade_Escalonadas_Por_Diretor():
    try:
        return pd.read_sql(queries.get_Quantidade_Escalonadas_Por_Diretor, conn)      
    except Exception as e:
        raise e
    
def get_filtro_valor(dfIdsContatos):
    try:
        values =  ",".join("'{}'".format(valor) for valor in dfIdsContatos)
        return pd.read_sql(queries.get_filtro_valor.format(contatos=values), conn)
    except Exception as e:
        raise e
    
def get_Pecs_Filtradas(idContato, ordem):
    try:
        #query precisa verificar se traz resultados, query mto grande para p pd.read_sql
        #cursor = conn.cursor()
        #cursor.execute(queries.get_Pecs_Por_Filtro.format(id=idContato, ordem=ordem))
        query = queries.get_Pecs_Por_Filtro.format(id=idContato, ordem=ordem)
        return pd.read_sql_query(query, conn)
    except Exception as e:
        raise e
    
# ---- RETORNA as pecs escolhidas pelo contato----
def get_Pecs_Por_Contato(dfIdsContatos):
    try:
        values =  ",".join("'{}'".format(valor) for valor in dfIdsContatos)
        return pd.read_sql(queries.get_Pecs_Por_Contato.format(contatos=values), conn)        
    except Exception as e:
        raise e

# ---- RETORNA o nivel de escalonamento que a pessoa escolheu contendo seu id, nivel do escalonamento e contatodiretoresid  
def get_Nivel_Por_Contato(dfIdsContatos):
    try:
        values =  ",".join("'{}'".format(valor) for valor in dfIdsContatos)
        return pd.read_sql(queries.get_nivel_escalonamento.format(contatos=values), conn)
    except Exception as e:
        raise e
    


def get_Niveis_Nomes():
    try: 
        return {
                'Supervisor': 'Supervisor',
                'Gerente': 'Gerente',
                'GerenteRegional': 'Gerente regional',
                'DiretorRegional': 'Diretor Regional',
                'DiretorExecutivo': 'Diretor executivo'
            }
    except Exception as e:
        raise e