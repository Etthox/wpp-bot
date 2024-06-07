from datetime import date, datetime
import pyodbc
import os

def writeDevLog(logMessage, path = ''):
    today = date.today()
    now = datetime.now()
    logText = f"""{today.strftime("%Y-%m-%d")} {now.strftime("%H:%M:%S")} - {logMessage}"""
    
    
    with open(path if path else os.getcwd() + '\\log.txt', 'a') as f:
        f.write('\n' + logText)
        


#DB insert new log
def writeDbLog(CONX_STRING, query):
    try:
        conx = pyodbc.connect(CONX_STRING)
        cursor = conx.cursor()
        cursor.execute(query)
        
        conx.commit()
        cursor.close()
           
    except Exception as e:
        return(e)