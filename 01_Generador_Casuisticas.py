# -*- coding: utf-8 -*-
"""
UNIVERSIDAD NACIONAL DE EDUCACIÓN A DISTANCIA

Master Inteligencia Artificial Avanzada

Trabajo de Fin de Master
Generador de diferente Casuísticas en Históricos

@author: Sergio Montes Vázquez
"""

#0.- Librerías para conexión a base de datos
import pymysql.cursors
import random
from datetime import datetime, date, time, timedelta


#1.- Creación de datos virtuales normales
def CreaRegistrosIniciles(pHost, pUser, pPassword, pDB):
    #1.0.- Inicialización de parámetro globales y conexión a base de datos
    Conexion = pymysql.connect(host = pHost, user = pUser, password = pPassword, db = pDB)
    try:
        with Conexion.cursor() as Cursor:
            #1.1.- Se limpian todas las tablas empezando por las que son dependientes
            stSQL = "DELETE FROM " + pDB + ".HIST_A_B WHERE ID_PRINCIPAL <> -1"
            Cursor.execute(stSQL)
            stSQL = "DELETE FROM " + pDB + ".HIST_A2 WHERE ID_PRINCIPAL <> -1"
            Cursor.execute(stSQL)
            stSQL = "DELETE FROM " + pDB + ".HIST_A1 WHERE ID_PRINCIPAL <> -1"
            Cursor.execute(stSQL)
            stSQL = "DELETE FROM " + pDB + ".HIST_PRINCIPAL_B WHERE ID_PRINCIPAL_B <> -1"
            Cursor.execute(stSQL)
            stSQL = "DELETE FROM " + pDB + ".HIST_PRINCIPAL_A WHERE ID_PRINCIPAL <> -1"
            Cursor.execute(stSQL)
            stSQL = "DELETE FROM " + pDB + ".TB_PRINCIPAL WHERE ID_PRINCIPAL <> -1"
            Cursor.execute(stSQL)
            stSQL = "DELETE FROM " + pDB + ".CAT_A1 WHERE ID_A1 <> -1"
            Cursor.execute(stSQL)
            stSQL = "DELETE FROM " + pDB + ".CAT_A2 WHERE ID_A2 <> -1"
            Cursor.execute(stSQL)

            #1.2.- Se crean 1000 registros para la tabla principal
            for i in range(0,1000):
                stSQL = "INSERT INTO " + pDB + ".TB_PRINCIPAL (ID_PRINCIPAL, DES_PRINCIPAL, COMENTARIO) " 
                stSQL = stSQL + "VALUES (%s, %s, %s)" 
                Cursor.execute(stSQL,(str(i),"Registro Núm. " + str(i),"Situación Normal"))

            #1.3.- Se crean 10 registros para el Catálogo A1
            for i in range(0,10):
                stSQL = "INSERT INTO " + pDB + ".CAT_A1 (ID_A1, DES_A1) " 
                stSQL = stSQL + "VALUES (%s, %s)" 
                Cursor.execute(stSQL,(str(i),"Catálogo A1 Reg. Núm. " + str(i)))

            #1.4.- Se crean 10 registros para el Catálogo A2
            for i in range(0,10):
                stSQL = "INSERT INTO " + pDB + ".CAT_A2 (ID_A2, DES_A2) " 
                stSQL = stSQL + "VALUES (%s, %s)" 
                Cursor.execute(stSQL,(str(i),"Catálogo A2 Reg. Núm. " + str(i)))
                
            #1.5.- Se crean de 0 a 3 tramos en el Histórico Principal A con tramos con fechas aleatorias
            for i in range(0,1000):
                numHist = random.randint(0,3)
                Fec_Inicio = datetime(2010, 1, 1, 0, 0, 0)
                Fec_Fin = datetime(2010, 1, 1, 0, 0, 0)
                for j in range(0,numHist):
                    if Fec_Fin < datetime(2100, 1, 1, 0, 0, 0):
                        if j == 0:
                            Fec_Inicio = Fec_Inicio + timedelta(days = 100 + random.randint(0,1000))
                        else:
                            Fec_Inicio = Fec_Fin + timedelta(days = 1)
                        numDias = 100 + random.randint(0,10000) 
                        Fec_Fin = Fec_Inicio + timedelta(days = numDias)
                        if Fec_Fin >= datetime(2025, 1, 1, 0, 0, 0):
                            Fec_Fin = datetime(2100, 1, 1, 0, 0, 0)
                        stSQL = "INSERT INTO " + pDB + ".HIST_PRINCIPAL_A (ID_PRINCIPAL, ORD_PRINCIPAL, FECHA_INICIO, FECHA_FIN, COMENTARIO) "
                        stSQL = stSQL + "VALUES (%s, %s, %s, %s, %s)"
                        Cursor.execute(stSQL,(str(i), str(j), Fec_Inicio.strftime('%Y-%m-%d'), Fec_Fin.strftime('%Y-%m-%d'),'Histórico Normal'))
                
            #1.6.- Se crean 20 registros para el Histórico Principal B con tramos con fechas aleatorias 
            #      Nota: para asegúranos que todos los registros del Histórico Principal A están
            #            cubiertos, el primer registro de este histórico va desde 01/01/2010 al 01/01/2100
            for i in range(0,20):
                Fec_Inicio = datetime(2010, 1, 1, 0, 0, 0)
                Fec_Fin = datetime(2100, 1, 1, 0, 0, 0)
                if i > 0:
                    Fec_Inicio = Fec_Inicio + timedelta(days = random.randint(0,1000))
                    numDias = 500 + random.randint(0,10000) 
                    Fec_Fin = Fec_Inicio + timedelta(days = numDias)
                    if Fec_Fin >= datetime(2025, 1, 1, 0, 0, 0):
                        Fec_Fin = datetime(2100, 1, 1, 0, 0, 0)
                stSQL = "INSERT INTO " + pDB + ".HIST_PRINCIPAL_B(ID_PRINCIPAL_B,DES_PRINCIPAL_B,FECHA_INICIO,FECHA_FIN,COMENTARIO) "
                stSQL = stSQL + "VALUES (%s,%s,%s,%s,%s)"
                Cursor.execute(stSQL,(str(i), 'Catálogo/Histórico Principal B Reg. ' + str(i), Fec_Inicio.strftime('%Y-%m-%d'), Fec_Fin.strftime('%Y-%m-%d'),'Histórico Normal'))
            
            #1.7.- Creación de los Históricos Dependientes
            #1.7.1.- Se recorren los registros del Histórico Principal A, donde van a colgar el
            #      resto de los registros 
            stSQL = "SELECT * FROM " + pDB + ".HIST_PRINCIPAL_A"
            Cursor.execute(stSQL)
            HIST_PRINCIPAL_A = Cursor.fetchall()
            for i in range(0,len(HIST_PRINCIPAL_A)):
                ID_PRINCIPAL  = HIST_PRINCIPAL_A[i][0]
                ORD_PRINCIPAL = HIST_PRINCIPAL_A[i][1]
                FECHA_INICIO  = HIST_PRINCIPAL_A[i][2]
                FECHA_FIN     = HIST_PRINCIPAL_A[i][3]
				
                #1.7.2.- Creación de Registros Dependiente de Catálogo A1
                #      Este es un histórico con tramos completos
                numHist     = random.randint(0,5)
                banAgregar  = numHist
                vFec_Inicio = FECHA_INICIO
                for j in range(0,numHist):
                    id_A1 = random.randint(0,9)
                    Num_Dias = 1 + random.randint(0,1000)
                    vFec_Fin = vFec_Inicio + timedelta(days = Num_Dias)
                    if (vFec_Fin > FECHA_FIN or j == numHist-1) and banAgregar > 0:
                        vFec_Fin = FECHA_FIN
                        banAgregar = 1
                    if banAgregar > 0 :
                        stSQL = "INSERT INTO " + pDB + ".HIST_A1 (ID_PRINCIPAL, ORD_PRINCIPAL, ID_A1, FECHA_INICIO, FECHA_FIN, COMENTARIO) "
                        stSQL = stSQL + "VALUES (%s,%s,%s,%s,%s,%s)"
                        Cursor.execute(stSQL,(str(ID_PRINCIPAL), str(ORD_PRINCIPAL), str(id_A1), vFec_Inicio.strftime('%Y-%m-%d'), vFec_Fin.strftime('%Y-%m-%d'),'Histórico Normal'))
                        vFec_Inicio = vFec_Fin + timedelta(days = 1)
                        banAgregar = banAgregar - 1
						
                #1.7.3.- Creación de Registros Dependiente de Catálogo A2
                #      Este es un histórico con tramos discontinuos
                numHist    = random.randint(0,5)
                banAgregar = numHist
                vFec_Inicio = FECHA_INICIO                
                for j in range(0,numHist):
                    id_A2 = random.randint(0,9)
                    Num_Dias = 1 + random.randint(0,1000)
                    vFec_Fin = vFec_Inicio + timedelta(days = Num_Dias)
                    if (vFec_Fin > FECHA_FIN or j == numHist-1) and banAgregar > 0:
                        vFec_Fin = FECHA_FIN
                        banAgregar = 1
                    if banAgregar > 0 :
                        banInsertar = random.randint(0,1)
                        if banInsertar == 1:
                            stSQL = "INSERT INTO " + pDB + ".HIST_A2 (ID_PRINCIPAL, ORD_PRINCIPAL, ID_A2, FECHA_INICIO, FECHA_FIN, COMENTARIO) "
                            stSQL = stSQL + "VALUES (%s,%s,%s,%s,%s,%s)"
                             Cursor.execute(stSQL,(str(ID_PRINCIPAL), str(ORD_PRINCIPAL), str(id_A2), vFec_Inicio.strftime('%Y-%m-%d'), vFec_Fin.strftime('%Y-%m-%d'),'Histórico Normal'))
                            vFec_Inicio = vFec_Fin + timedelta(days = 1)
                        banAgregar = banAgregar - 1
                
                #1.7.4.- Creación de Registros Dependiente del histórico principal B
                #      Este es un histórico con tramos continuos y obligatorio
                numHist    = random.randint(1,3)
                banAgregar = numHist
                vFec_Inicio = FECHA_INICIO
                for j in range(0,numHist):
                    Num_Dias = 1 + random.randint(0,1000)
                    vFec_Fin = vFec_Inicio + timedelta(days = Num_Dias)
                    if (vFec_Fin > FECHA_FIN or j == numHist-1) and banAgregar > 0:
                        vFec_Fin = FECHA_FIN
                        banAgregar = 1
                    if banAgregar > 0 :
                        stSQL = "SELECT ID_PRINCIPAL_B,FECHA_INICIO,FECHA_FIN FROM " + pDB + ".HIST_PRINCIPAL_B WHERE FECHA_INICIO <= %s AND %s <= FECHA_FIN "
                        Cursor.execute(stSQL,(vFec_Fin.strftime('%Y-%m-%d'),vFec_Inicio.strftime('%Y-%m-%d')))
                        HIST_A_B       = Cursor.fetchall()
                        numReg         = random.randint(0,len(HIST_A_B)-1)
                        ID_PRINCIPAL_B = HIST_A_B[numReg][0]
                        stSQL = "INSERT INTO " + pDB + ".HIST_A_B (ID_PRINCIPAL, ORD_PRINCIPAL, ID_PRINCIPAL_B, FECHA_INICIO, FECHA_FIN, COMENTARIO) "
                        stSQL = stSQL + "VALUES (%s,%s,%s,%s,%s,%s)"
                        Cursor.execute(stSQL,(str(ID_PRINCIPAL), str(ORD_PRINCIPAL), str(ID_PRINCIPAL_B), vFec_Inicio.strftime('%Y-%m-%d'), vFec_Fin.strftime('%Y-%m-%d'),'Histórico Normal'))

                        vFec_Inicio = vFec_Fin + timedelta(days = 1)
                        banAgregar = banAgregar - 1
        Conexion.commit()
        Conexion.close()
    finally:
        pass
    
#2.- Creación de datos virtuales error tipo I
def CreaRegistrosErrorTipoI(pHost, pUser, pPassword, pDB, pNumEjemplos):
    #2.0.- Inicialización de parámetro globales y conexión a base de datos
    Conexion = pymysql.connect(host = pHost, user = pUser, password = pPassword, db = pDB)
    try:
        with Conexion.cursor() as Cursor:
            #2.1.- Vector de Nombre de Tablas Históricas a modificar 
            VecTabla = ["HIST_PRINCIPAL_A","HIST_A1","HIST_A2","HIST_A_B"]
            
            #2.2.- Genera los Ejemplos de Error Tipo I
            if pNumEjemplos > 0:
                #2.2.0.- Se elimina los registros del Log del Error tipo 1
                stSQL = "DELETE FROM " + pDB + ".LOG_HIST WHERE ID_ERROR = 1"
                Cursor.execute(stSQL)
                for i in range(0,pNumEjemplos):
                    #2.2.1.- Se selecciona la tabla de forma aleatoria
                    idTabla = random.randint(0,len(VecTabla)-1)
                    #2.2.2.- Se selecciona el registro a cambiar
                    stSQL = "SELECT * FROM " + pDB + "." + VecTabla[idTabla] + " WHERE FECHA_INICIO <= FECHA_FIN  "
                    Cursor.execute(stSQL)
                    auxTabla = Cursor.fetchall()
                    numReg = random.randint(0,len(auxTabla)-1)
                    
                    #2.2.3.- Se recuperan los datos de las columnas a partir del diccionario de datos y se
                    #        prepara el vector de parámetros para la instrucción de actualización SQL
                    stSQL = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY "
                    stSQL = stSQL + "FROM INFORMATION_SCHEMA.COLUMNS "
                    stSQL = stSQL + "WHERE TABLE_SCHEMA = '" + pDB + "' "
                    stSQL = stSQL + "AND TABLE_NAME = '" + VecTabla[idTabla] + "' "
                    stSQL = stSQL + "ORDER BY ORDINAL_POSITION"
                    Cursor.execute(stSQL)
                    auxDiccionario = Cursor.fetchall()
                    idFec_Inicio = 0
                    idFec_Fin = 0
                    stWhere = "WHERE "
                    vecWhere=[]
                    for j in range(0,len(auxDiccionario)-1):
                        if auxDiccionario[j][0] == "FECHA_INICIO": idFec_Inicio = j
                        if auxDiccionario[j][0] == "FECHA_FIN": idFec_Fin = j
                        if auxDiccionario[j][2] == "PRI":
                            #Se construye la instrucción Where y el Vector de datos para filtrar la instrucción SQL 
                            stWhere = stWhere + "" + auxDiccionario[j][0] +  "=%s AND "
                            if auxDiccionario[j][1] == "int":
                                vecWhere.append(str(auxTabla[numReg][j]))
                            if auxDiccionario[j][1] == "date":
                                vecWhere.append(auxTabla[numReg][j].strftime('%Y-%m-%d'))
                    stWhere = stWhere + " 1 = 1 "
                    
                    #2.2.4.- Se selecciona la forma de alterar los históricos
                    idTipo = random.randint(0,3)
                    vFec_Inicio = datetime(2010, 1, 1, 0, 0, 0)
                    vFec_Fin    = datetime(2100, 1, 1, 0, 0, 0)
                    if idTipo == 0:
                        #Se invierten las fechas de Inicio y de Fin
                        vFec_Inicio = auxTabla[numReg][idFec_Fin]
                        vFec_Fin    = auxTabla[numReg][idFec_Inicio]
                    if idTipo == 1:
                        #Se mantiene fijo la fecha inicio y la fecha fin se modifica
                        vFec_Inicio = auxTabla[numReg][idFec_Inicio]
                        Num_Dias    = -100 - random.randint(0,1000)
                        vFec_Fin    = vFec_Inicio + timedelta(days = Num_Dias)
                    if idTipo == 2:
                        #Se mantiene fijo la fecha fin y la fecha inicio se modifica
                        vFec_Fin = auxTabla[numReg][idFec_Fin]
                        Num_Dias    = 100 + random.randint(0,1000)
                        vFec_Inicio = vFec_Fin + timedelta(days = Num_Dias)
                    if idTipo == 3:
                        #Las fecha son completamente aleatorias y la fecha Inicio va después de la fecha fin
                        Num_Dias    = 100 + random.randint(0,1000)
                        vFec_Fin    =  vFec_Inicio + timedelta(days = Num_Dias)
                        Num_Dias    = 100 + random.randint(0,1000)
                        vFec_Inicio = vFec_Fin + timedelta(days = Num_Dias)

                    #2.2.5.- Se guarda el valor de las fechas y la referencia para comparar después
                    stSQL = "SELECT COUNT(*) FROM " + pDB + ".LOG_HIST "
                    Cursor.execute(stSQL)
                    auxLog   = Cursor.fetchall()
                    numLog   = auxLog[0][0] + 1
                    stRefLog = "Error 1, Reg " + str(numLog) + " : "
                    stSQL = "INSERT INTO " + pDB + ".LOG_HIST (ID_ERROR,TABLA,REFERENCIA,FECHA_INICIO_OLD,FECHA_FIN_OLD,FECHA_INICIO_NEW,FECHA_FIN_NEW) "
                    stSQL = stSQL + "VALUES (%s,%s,%s,%s,%s,%s,%s)"
                    Cursor.execute(stSQL,("1", VecTabla[idTabla], stRefLog, auxTabla[numReg][idFec_Inicio].strftime('%Y-%m-%d'), auxTabla[numReg][idFec_Fin].strftime('%Y-%m-%d'),vFec_Inicio.strftime('%Y-%m-%d'),vFec_Fin.strftime('%Y-%m-%d')))
                    
                    #2.2.6.- Se genera en error en las tablas
                    vecWhere.insert(0,vFec_Inicio.strftime('%Y-%m-%d'))
                    vecWhere.insert(1,vFec_Fin.strftime('%Y-%m-%d'))
                    vecWhere.insert(2,stRefLog)
                    stSQL = "UPDATE " + pDB + "." + VecTabla[idTabla] + " SET FECHA_INICIO=%s, FECHA_FIN=%s, COMENTARIO = %s "
                    stSQL = stSQL + stWhere
                    Cursor.execute(stSQL,vecWhere)
        Conexion.commit()
        Conexion.close()
    finally:
        pass

#3.- Creación de datos virtuales error tipo II
def CreaRegistrosErrorTipoII(pHost, pUser, pPassword, pDB, pNumEjemplos):
    #3.0.- Inicialización de parámetro globales y conexión a base de datos
    Conexion = pymysql.connect(host = pHost, user = pUser, password = pPassword, db = pDB)
    try:
        with Conexion.cursor() as Cursor:
            #3.1.- Vector de Nombre de Tablas Históricas a modificar 
            VecTabla = ["HIST_PRINCIPAL_A","HIST_A1","HIST_A2","HIST_A_B"]
            
            #3.2.- Genera los Ejemplos de Error Tipo I
            for i in range(0,pNumEjemplos):
                #3.2.1.- Se selecciona la tabla de forma aleatoria
                idTabla = random.randint(0,len(VecTabla)-1)
    
                #3.2.2.- Se selecciona el registro a cambiar
                stSQL = "SELECT * FROM " + pDB + "." + VecTabla[idTabla] + " WHERE COMENTARIO='Histórico Normal'"
                Cursor.execute(stSQL)
                auxTabla = Cursor.fetchall()
                numReg = random.randint(0,len(auxTabla)-1)
                
                #3.2.3.- Se recuperan los datos de las columnas a partir del diccionario de datos y se
                #        prepara el vector de parámetros para la instrucción de actualización SQL
                stSQL = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY "
                stSQL = stSQL + "FROM INFORMATION_SCHEMA.COLUMNS "
                stSQL = stSQL + "WHERE TABLE_SCHEMA = '" + pDB + "' "
                stSQL = stSQL + "AND TABLE_NAME = '" + VecTabla[idTabla] + "' "
                stSQL = stSQL + "ORDER BY ORDINAL_POSITION"
                Cursor.execute(stSQL)
                auxDiccionario = Cursor.fetchall()
                idFec_Inicio = 0
                idFec_Fin = 0
                stWhere = "WHERE "
                vecWhere=[]
                for j in range(0,len(auxDiccionario)-1):
                    if auxDiccionario[j][0] == "FECHA_INICIO": idFec_Inicio = j
                    if auxDiccionario[j][0] == "FECHA_FIN": idFec_Fin = j
                    if auxDiccionario[j][2] == "PRI":
                        #Se construye la instrucción Where y el Vector de datos para filtrar la instrucción SQL 
                        stWhere = stWhere + "" + auxDiccionario[j][0] +  "=%s AND "
                        if auxDiccionario[j][1] == "int":
                            vecWhere.append(str(auxTabla[numReg][j]))
                        if auxDiccionario[j][1] == "date":
                            vecWhere.append(auxTabla[numReg][j].strftime('%Y-%m-%d'))
                stWhere = stWhere + " 1 = 1 "
                
                #3.2.4.- Se selecciona la forma de alterar los históricos
                idTipo = random.randint(0,2)
                vFec_Inicio = auxTabla[numReg][idFec_Inicio]
                vFec_Fin    = auxTabla[numReg][idFec_Fin]
                if idTipo == 0 or idTipo == 2:
                    #Se cambia la fecha de inicio
                    Num_Dias    = random.randint(0,1000) - 500
                    vFec_Inicio = auxTabla[numReg][idFec_Inicio] + timedelta(days = Num_Dias)
                if idTipo == 1 or idTipo == 2:
                    #Se cambia la fecha de fin
                    Num_Dias = random.randint(100,6000)
                    vFec_Fin = vFec_Inicio + timedelta(days = Num_Dias)
                
                #3.2.5.- Se guarda el valor de las fechas y la referencia para comparar después
                stSQL = "SELECT COUNT(*) FROM " + pDB + ".LOG_HIST "
                Cursor.execute(stSQL)
                auxLog   = Cursor.fetchall()
                numLog   = auxLog[0][0] + 1
                stRefLog = "Error 2, Reg " + str(numLog) + " : "
                stSQL = "INSERT INTO " + pDB + ".LOG_HIST (ID_ERROR,TABLA,REFERENCIA,FECHA_INICIO_OLD,FECHA_FIN_OLD,FECHA_INICIO_NEW,FECHA_FIN_NEW) "
                stSQL = stSQL + "VALUES (%s,%s,%s,%s,%s,%s,%s)"
                Cursor.execute(stSQL,("2", VecTabla[idTabla], stRefLog, auxTabla[numReg][idFec_Inicio].strftime('%Y-%m-%d'), auxTabla[numReg][idFec_Fin].strftime('%Y-%m-%d'),vFec_Inicio.strftime('%Y-%m-%d'),vFec_Fin.strftime('%Y-%m-%d')))
                
                #3.2.6.- Se genera en error en las tablas
                vecWhere.insert(0,vFec_Inicio.strftime('%Y-%m-%d'))
                vecWhere.insert(1,vFec_Fin.strftime('%Y-%m-%d'))
                vecWhere.insert(2,stRefLog)
                stSQL = "UPDATE " + pDB + "." + VecTabla[idTabla] + " SET FECHA_INICIO=%s, FECHA_FIN=%s, COMENTARIO = %s "
                stSQL = stSQL + stWhere
                Cursor.execute(stSQL,vecWhere)
            
        Conexion.commit()
        Conexion.close()
    finally:
        pass

#99.- Rutina Principal
stHost =  'localhost'
stUser = 'root'
stPassword = 'xxxxxxxxxx'
stDB = 'tfm_smv'
CreaRegistrosIniciles(stHost,stUser,stPassword,stDB)
NumEjemplos = 50
CreaRegistrosErrorTipoI(stHost,stUser,stPassword,stDB,NumEjemplos)
CreaRegistrosErrorTipoII(stHost,stUser,stPassword,stDB,NumEjemplos)


