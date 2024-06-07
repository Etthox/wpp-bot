#import pywhatkit as w
import enum
import re
import time
import pandas as pd
import pyodbc
import keyboard as k
import json
from datetime import date, datetime, timedelta
#from prettytable import PrettyTable
from bs4 import BeautifulSoup
import numpy as np
from termcolor import colored
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from enum import Enum
import time
from selenium.webdriver.common.by import By
import pathlib
import schedule
import sys
import db
import mock
import tools
import ast 
import json
import smtplib
import ssl
from email.message import EmailMessage

# Define email sender and receiver
email_sender = '...'
email_password = '...'
email_receiver = '...'

# Set the subject and body of the email
subject = 'Alerta de Erro no envio do BOT WHATSAPP'

em = EmailMessage()
em['From'] = email_sender
em['To'] = email_receiver
em['Subject'] = subject


# Add SSL (layer of security)
context = ssl.create_default_context()

# Log in and send the email
def email_erro(error):
    with smtplib.SMTP('smtp.office365.com', 587) as smtp:
        body = "O envio do BOT Whatsapp apresentou o seguinte erro: '{}', favor verificar\n\n".format(error)
        em.set_content(body)
        smtp.connect("smtp.office365.com",587)
        smtp.starttls()
        smtp.ehlo()
        smtp.login(email_sender, email_password)
        smtp.sendmail(email_sender, email_receiver, em.as_string(error))
    


dataInicio = ''
dataFim = ''



#pega a data que iniciou a execução para o log
today = date.today()
now = datetime.now()
dataInicio = f"""{today.strftime("%Y-%m-%d")}T{now.strftime("%H:%M:%S")}"""


#   DICIONARIO DE DADOS
#queryContatos = query principal para ContatosDiretores
#contatos = dataframe resultado contatos
#contato = contato atual do loop
#tudo = 

totalTarefas = 0


def rotina(test):

    if(test):
        dadosContatos = pd.DataFrame.from_records(json.loads(mock.dadosContatos))
        top5 = pd.DataFrame.from_records(json.loads(mock.top5))
        dftodos_3 = pd.DataFrame.from_records(json.loads(mock.dftodos_3))
        dftodos_4 = pd.DataFrame.from_records(json.loads(mock.dftodos_4))
        df_por_hierarquia = pd.DataFrame.from_records(json.loads(mock.df_por_hierarquia))
        df_por_todos_diret = pd.DataFrame.from_records(json.loads(mock.df_por_todos_diret))
        df_Pecs_Por_Contato = pd.DataFrame.from_records(json.loads(mock.df_Pecs_Por_Contato))
        df_Nivel_Por_Contato = pd.DataFrame.from_records(json.loads(mock.df_Nivel_Por_Contato))

    else:
        dadosContatos = db.get_Contatos_Rotina_Atual() 
        
        #usa os ids do envio atual como filtro para obter as pecs e nivelEscalonamento
        dfIdsContatos = dadosContatos['Id']
        if dfIdsContatos.size > 0:
            None
            #df_Nivel_Por_Contato = db.get_Nivel_Por_Contato(dfIdsContatos)


            #df_Pecs_Por_Contato = db.get_Pecs_Por_Contato(dfIdsContatos)
            #df_por_hierarquia = db.get_Pecs_Com_Escalonadas(df_Pecs_Por_Contato, df_Nivel_Por_Contato)
            #df_por_todos_diret = db.get_Quantidade_Escalonadas_Por_Diretor()
            #df_filtro_valor = db.get_filtro_valor(dfIdsContatos)
            #nova query renan


    dataInicio = ''
    dataFim = ''    
 
    #roda um for para cada contato na variavel Contatos
    for index, contato in dadosContatos.iterrows():
        try:
            x = pd.DataFrame(contato)
            if contato['OrdemFiltro'] != None:
                global totalTarefas
                df_Pecs_Por_Filtro = None
                ordemFiltro = ast.literal_eval(contato['OrdemFiltro'])
                for index,filtro in enumerate(ordemFiltro):
                    if len(filtro) > 0:
                        df_Pecs_Por_Filtro = pd.concat([df_Pecs_Por_Filtro, db.get_Pecs_Filtradas(contato['Id'], index+1)])

                df_filtro_valor = db.get_filtro_valor(dfIdsContatos)
                df_Nivel_Por_Contato = db.get_Nivel_Por_Contato(dfIdsContatos)
                df_Pecs_Por_Contato = db.get_Pecs_Por_Contato(dfIdsContatos)

                #jogar as pecs filtradas na query do renan
                df_por_hierarquia = db.get_Pecs_Com_Escalonadas(df_Pecs_Por_Filtro)
                df_por_todos_diret = db.get_Quantidade_Escalonadas_Por_Diretor()

                now = datetime.now()
                hora_formatada = datetime.now()                           
                dia_formatado = hora_formatada.strftime("%d/%m/%y")

                hora_hoje = (hora_formatada - timedelta(hours=3)).strftime("%H:%M")
                dataInicio = f"""{today.strftime("%Y-%m-%d")}T{now.strftime("%H:%M:%S")}"""



                
                #filtrar df_por_hierarquia de acordo com o nivel escalonado selecionado do contato
                # Ordenar as tarefas em ordem decrescente

                #soma diretor e diretor executivo
                
                total_Tarefas_Diretores = df_por_todos_diret['DIRETOR_REGIONAL_quantidade'].sum()





                #pega todos os pecs que contem o nome do solicitante
                pecs = df_por_hierarquia

                df_Nivel_Por_Contato = df_Nivel_Por_Contato.loc[
                    df_Nivel_Por_Contato['ContatosDiretoresId'] == contato['Id']
                ]

                

                resultado = pecs.groupby('EstruturaNivel1').agg({'Quantidade': 'sum'}).reset_index()


                pecs = pecs.sort_values('Quantidade', ascending=False)

                totalTarefas = pecs["Quantidade"].sum()
                
                niveis_Nomes = db.get_Niveis_Nomes()

                PecsResultado = resultado.sort_values('Quantidade',ascending=False)


                filtros = df_filtro_valor[['filtro','valor']]

                print(filtros)
                

            
                mensagemEnvio = """*|| ÍRIS GPS VISTA ||*%0a%0aOlá {}%0aSegue o volume de tarefas escalonadas dia {} às {}%0a*TOTAL: {}*%0a""".format(contato["nome"], dia_formatado, hora_hoje, totalTarefas.real)
                colunasniveis = pecs[['DiretorExecutivo','DiretorRegional','GerenteRegional','Gerente','Supervisor']].copy()
                for name, item in colunasniveis.items():
                    mensagemEnvio += f"%0a%0aEscalonamento por *{niveis_Nomes[name]}*%0a"
                    juncaocolaborador = pecs.groupby([name])['Quantidade'].sum()
                    juncaocolaborador = juncaocolaborador.sort_values(ascending=False)
                    for colaborador, quantidade in juncaocolaborador.items():
                        mensagemEnvio += f"%0a *{quantidade}* : {colaborador}%0a"

                mensagemEnvioPec = """%0a%0aEscalonamento por *PECs*%0a"""
                for _, row in PecsResultado.iterrows():
                    mensagemEnvioPec +=  f"%0a *{row['Quantidade']}* : {row['EstruturaNivel1']}"
                    print(mensagemEnvioPec)

                print(mensagemEnvioPec)

                print(mensagemEnvio)
            
                

        except Exception as error: 
            print('Handling data error')
            
            now = datetime.now()
            dataFim = f"""{today.strftime("%Y-%m-%d")}T{now.strftime("%H:%M:%S")}"""
            db.setQueryInsertLog(dataInicio, dataFim, 'Erro', str(error))
            email_erro(str(error))



        contact = contato['telefone']

        if totalTarefas >0 :
            try:
                todas_mensagens= mensagemEnvio + mensagemEnvioPec
                sendLink = f'https://web.whatsapp.com/send?phone={contact}&text={todas_mensagens}'
                #service = Service('C:\Program Files\Chrome Driver\chromedri    ver.exe')
                options = webdriver.ChromeOptions()

                #script_directory = pathlib.Path().absolute()
                options.add_argument("...")


                driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
                driver.get(sendLink)
                time.sleep(90)
                driver.find_element(By.CSS_SELECTOR, '''span[data-icon='send']''').click()
                time.sleep(60)
                print("JOB FINALIZADO")
                driver.quit()
                now = datetime.now()
                dataFim = f"""{today.strftime("%Y-%m-%d")}T{now.strftime("%H:%M:%S")}"""
            except Exception as error:
                print('Whatsapp send error')
                now = datetime.now()
                dataFim = f"""{today.strftime("%Y-%m-%d")}T{now.strftime("%H:%M:%S")}"""
                db.setQueryInsertLog(dataInicio, dataFim, 'Erro', str(error))
                email_erro(str(error))

    exit()
    
rotina(test=False)