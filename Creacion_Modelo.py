# -*- coding: utf-8 -*-
"""
UNIVERSIDAD NACIONAL DE EDUCACION A DISTANCIA
Master: Inteligencia Artificial Avanzada 
Materia: Trabajo de Fin de Master 
Módulo: Creación de Modelos
@author: Sergio Montes Vázquez
Noviembre 2019
"""

#0.- Librerias e iniciación de variables 
import datetime as dt
import pymysql.cursors
from mysql.connector import Error
import math
import time

#1.- Subrutinas Auxiliares
#1.1.- Probabilidad de la variable X
def probX(pX):
    matResultado = []
    for i in range(0,len(pX)):
        banAgregar = True
        for j in range(0,len(matResultado)):
            if pX[i] == matResultado[j][0]:
                banAgregar = False
                matResultado[j][1] = matResultado[j][1] + 1/len(pX)
        if banAgregar:
            ren = [pX[i],1/len(pX)]
            matResultado.append(ren)
    return(matResultado)

#1.2.- Probabilidad de la variable Y dado X
def probY_X(pX,pY):
    matResultado = []
    vPX = probX(pX)
    vPY_X = []
    for i in range(0,len(pX)):
        banAgregar = True
        for j in range(0,len(vPY_X)):
            if pX[i] == vPY_X[j][0] and pY[i] == vPY_X[j][1]:
                banAgregar = False
                vPY_X[j][2] = vPY_X[j][2] + 1/len(pX)
        if banAgregar:
            ren = [pX[i],pY[i],1/len(pX)]
            vPY_X.append(ren)
    for i in range(0,len(vPY_X)):
        for j in range(0,len(vPX)):
            if vPY_X[i][0] == vPX[j][0]:
                ren = [vPY_X[i][0],vPY_X[i][1],vPY_X[i][2]/vPX[j][1]]
                matResultado.append(ren)
    return(matResultado)

#1.3.- Entropia de la variable X    
def EntropiaX(pX):
    varH = 0
    vPX = probX(pX)
    for i in range(0,len(vPX)):
        varH = varH - vPX[i][1]*math.log(vPX[i][1],2)
    return(varH)
    
#1.4.- Entropia de la variable Y dado X 
def EntropiaY_X(pX,pY):
    varH = 0
    vPX = probX(pX)
    vPY_X = probY_X(pX,pY)
    
    for i in range(0,len(vPX)):
        vSumY_X = 0
        for j in range(0,len(vPY_X)):
            if vPX[i][0] == vPY_X[j][0]:
                vSumY_X = vSumY_X - vPY_X[j][2]*math.log(vPY_X[j][2],2)
        varH = varH + vPX[i][1] * vSumY_X

    return(varH)
    
#1.5.- Diferencia entre fechas
def Dif_Tiempo(Inicio,Fin):
    vAux1 = Inicio.year + 30 * (Inicio.month - 1) + Inicio.day + Inicio.hour / 24 + Inicio.minute / 60 / 24 + Inicio.second / 60 / 60 / 24
    vAux2 = Fin.year + 30 * (Fin.month - 1) + Fin.day + Fin.hour / 24 + Fin.minute / 60 / 24 + Fin.second / 60 / 60 / 24
    
    vAux = 24 * (vAux2 - vAux1)
    vHoras = int(vAux)
    vAux = (vAux - vHoras) * 60
    vMinutos = int(vAux)
    vAux = (vAux - vMinutos) * 60
    vSegundos = int(vAux)
    stResultado  = str(vHoras) + ":" + str(vMinutos).zfill(2) + ":" + str(vSegundos).zfill(2)     
    return(stResultado)
    
#2.- Subrutinas para gestionar el cálculo de las probsbilidades por Base de Datos
#2.1.- Guarda las probabvilidades
def Guarda_Probabilidad(myConexion, stDB, idParticion, stVariables, stProbabilidad, stExperimento, vInicio, vFin, vTiempo):

    stAux = stProbabilidad
    idParte = 1
    try:
        with myConexion.cursor() as myCursor:
            while True:
                stProb = stAux[0:16000]
                stAux = stAux[16000:]
                if len(stProb)>0:
                    stSQL = "INSERT INTO " + stDB + ".TB_PROBABILIDAD "
                    stSQL = stSQL + " (idParticion, stVariables, idParte, stProbabilidad,"
                    stSQL = stSQL + " ORIGEN_EXPERIMENTO, INICIO, FIN, TIEMPO) "
                    stSQL = stSQL + " VALUES("  + str(idParticion) + "," 
                    stSQL = stSQL + "'" + stVariables + "'," + str(idParte) + ",'" + stProb + "',"
                    stSQL = stSQL + "'" + stExperimento + "','" + str(vInicio) + "','" + str(vFin) + "',"
                    stSQL = stSQL + "'" + vTiempo + "')"
                    idParte = idParte + 1
                    
                    myCursor.execute(stSQL)
                    myConexion.commit()
                else:
                    break
            
    except Error as e:
        print("Error reading data from MySQL table", e)  
        myConexion.close()      
                
    finally:
        pass
    
#2.2.- Busca probabailidad
def Busca_Probabilidad(myConexion, stDB, idParticion, stVariables, stExperimento):

    stProbabilidad = "N/E"
    
    try:
        with myConexion.cursor() as myCursor:
            #Registra el uso de la probabilidad
            stSQL = "SELECT Count(*) FROM " + stDB + ".TB_PROBABILIDAD_USO "
            stSQL = stSQL + " WHERE idParticion = "  + str(idParticion)
            stSQL = stSQL + " AND stVariables ='" + stVariables + "'"
            stSQL = stSQL + " AND DES_EXPERIMENTO ='" + stExperimento + "'"
            myCursor.execute(stSQL)
            regCtl = myCursor.fetchall()
            if regCtl[0][0] == 0:
                stSQL = "INSERT INTO " + stDB + ".TB_PROBABILIDAD_USO "
                stSQL = stSQL + " (idParticion, stVariables, DES_EXPERIMENTO) "
                stSQL = stSQL + " VALUES("  + str(idParticion) + "," 
                stSQL = stSQL + "'" + stVariables + "','" + stExperimento + "')"
                myCursor.execute(stSQL)
            
            #Regresa la Probabilidad
            stSQL = "SELECT Count(*) FROM " + stDB + ".TB_PROBABILIDAD "
            stSQL = stSQL + " WHERE idParticion = "  + str(idParticion)
            stSQL = stSQL + " AND stVariables ='" + stVariables + "'"
            myCursor.execute(stSQL)
            regCtl = myCursor.fetchall()
            
            if regCtl[0][0] != 0:
                stSQL = "SELECT stProbabilidad FROM " + stDB + ".TB_PROBABILIDAD "
                stSQL = stSQL + " WHERE idParticion = "  + str(idParticion)
                stSQL = stSQL + " AND stVariables ='" + stVariables + "'"
                stSQL = stSQL + " ORDER BY idParte"
                myCursor.execute(stSQL)
                regProb = myCursor.fetchall()
                stProbabilidad = ""
                for i in range(0,len(regProb)):
                    stProbabilidad = stProbabilidad + regProb[i][0]
            
    except Error as e:
        print("Error reading data from MySQL table", e)  
        myConexion.close()      
                
    finally:
        pass

    return(stProbabilidad)
    
                
#3.- Subrutina de Creación de Modelos
def Crea_Modelo(stHost, stUser, stPassword, stDB, stRuta, varParametro, stExperimento):

    print('0.- Entro ' + time.strftime("%H:%M:%S"))
    
    stVer = varParametro[0]
    NumMaxCampos = varParametro[1]
    banIncluye_Estado_1 = varParametro[2]
    banDecision = varParametro[3]
    vCorte_GainR = varParametro[4]
    vCorrecionLaplace = varParametro[5]
    stCorrecionLaplace = str(vCorrecionLaplace)
    idTipoDiagrama = varParametro[6]
    idParticion = varParametro[7]
    
    stArchivo = "Modelo_v" + stVer
    if banIncluye_Estado_1 == True:
        stArchivo = stArchivo + "_ie1"
    else:
        stArchivo = stArchivo + "_ne1"
    if banDecision == True:
        stArchivo = stArchivo + "_id"
    else:
        stArchivo = stArchivo + "_nd"
    stCorte_GainR = str(vCorte_GainR)
    stArchivo = stArchivo + "_g" + stCorte_GainR.replace(".","") + "_c" + str(NumMaxCampos )
    stArchivo = stArchivo + "_cl" + stCorrecionLaplace.replace(".","")
    
    if idTipoDiagrama == 0:
        stTipoDiagrama = "MID"
        stArchivo = stArchivo + "_MID"
    if idTipoDiagrama == 1:
        stTipoDiagrama = "InfluenceDiagram"
        stArchivo = stArchivo + "_ID"
        
    if idParticion != -1:
        stArchivo = stArchivo + "_Par_" + str(idParticion)
        
    stArchivo = stArchivo + ".pgmx"
    
    #1.- Conexión e Inicialización Variables
    varResultado = 1
    myConexion = pymysql.connect(host = stHost, user = stUser, password = stPassword, db = stDB)
    
    vecVariables_Fijas = ['SEXO','NIVEL_EDUCATIVO']
    vecVariables = ['SEXO','EDAD','NUM_HIJOS','FAMILIA','PUESTO', 
                    'GRUPO_PUESTO','ANNO_ANTIGUEDAD','ANTIGUEDAD','SBA_IMPORTE','SBA',
                    'PORC_AUMENTO','NIVEL_EDUCATIVO','NUM_CURSOS','FORMACION','DESEMPENNO_NUM',
                    'DESEMPENNO','EVP_APRENDIZAJE','EVP_CARRERA','EVP_EQUILIBRIO','EVP_RETRIBUCION',
                    'EVP_NUM','EVP','ESTADO']
    vecClase = ['EDAD','NUM_HIJOS','FAMILIA','PUESTO', 'GRUPO_PUESTO',
                'ANNO_ANTIGUEDAD','ANTIGUEDAD','SBA_IMPORTE','SBA','PORC_AUMENTO',
                'NUM_CURSOS','FORMACION','DESEMPENNO_NUM', 'DESEMPENNO','EVP_APRENDIZAJE',
                'EVP_CARRERA','EVP_EQUILIBRIO','EVP_RETRIBUCION',
                'EVP_NUM','EVP','ESTADO']
    
    vecVariables = ['SEXO','EDAD','FAMILIA', 
                    'GRUPO_PUESTO','ANTIGUEDAD','SBA','DES_PORC_AUM',
                    'NIVEL_EDUCATIVO','FORMACION',
                    'DESEMPENNO','EVP','ESTADO']
    vecDecision = ['SBA','FORMACION']
    vecClase = ['EDAD','FAMILIA','GRUPO_PUESTO',
                'ANTIGUEDAD','SBA','DES_PORC_AUM',
                'FORMACION', 'DESEMPENNO','EVP','ESTADO']
    vecClase_Fija = ['ES_ABANDONO','ES_SEPARACION','FIN_PUESTO']
    vecClase_Fija_Conteo = ['B.ID_EMPLEADO','B.ID_EMPLEADO','B.PUESTO']
    vecVariables_Utilidad = ['GRUPO_PUESTO','ES_ABANDONO','ES_SEPARACION','FIN_PUESTO']
   
    try:
        with myConexion.cursor() as myCursor:
            print ('--> 1.- Entra en Cursor MySql')

            #1.- Consulta General
            stSQLFiltro = " FROM " + stDB + ".MID_DATOS A, " + stDB + ".MID_DATOS B"
            stSQLFiltro = stSQLFiltro + " WHERE A.ID_EMPLEADO = B.ID_EMPLEADO "
            stSQLFiltro = stSQLFiltro + " AND A.ANNO + 1 = B.ANNO "
            stSQLFiltro = stSQLFiltro + " AND A.ANNO_ANTIGUEDAD IS NOT NULL "
            stSQLFiltro = stSQLFiltro + " AND A.GRUPO_PUESTO <> 'S/D'"
            stSQLFiltro = stSQLFiltro + " AND A.SBA <> 'S/D'"
            stSQLFiltro = stSQLFiltro + " AND A.PUESTO <> 'S/D'"
            stSQLFiltro = stSQLFiltro + " AND A.ESTADO <> 'S/D'"
            stSQLFiltro = stSQLFiltro + " AND B.ESTADO <> 'S/D'"
            stSQLFiltro = stSQLFiltro + " AND A.MUESTRA <> " + str(idParticion) + " "

            #2.- Encabezado Red
            print('1.-- Inicia XML ' + time.strftime("%H:%M:%S"))
            
            arhcPGMX = open (stRuta  + stArchivo,'w')
            arhcPGMX.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            arhcPGMX.write('<ProbModelXML formatVersion="0.2.0">\n')
            arhcPGMX.write('  <ProbNet type="' + stTipoDiagrama + '">\n')
            arhcPGMX.write('    <Comment showWhenOpeningNetwork="false"><![CDATA[<<Pulse dos veces para incluir/modificar comentario>>]]></Comment>\n')
            arhcPGMX.write('    <DecisionCriteria>\n')
            arhcPGMX.write('      <Criterion name="---" unit="---" />\n')
            arhcPGMX.write('    </DecisionCriteria>\n')
            if idTipoDiagrama == 0:
                arhcPGMX.write('    <TimeUnit unit="YEAR" Value="1.0" />\n')
            arhcPGMX.write('    <AdditionalProperties />\n')
            
            #3.- Variables
            arhcPGMX.write('    <Variables>\n')
            vecNodo = []
            #3.1.- Variables Estátitcas
            print('2.-- Variables Estátitcas '  + time.strftime("%H:%M:%S"))
            posX = 100
            posY = 100
            for i in range(0,len(vecVariables_Fijas)):
                posY = 200 * (i+1)
                arhcPGMX.write('      <Variable name="' + vecVariables_Fijas[i] + '" type="finiteStates" role="chance">\n')
                arhcPGMX.write('        <Coordinates x="' + str(posX) + '" y="' + str(posY) + '" />\n')
                arhcPGMX.write('        <States>\n')
                stSQL = "SELECT A." + vecVariables_Fijas[i]
                stSQL = stSQL + stSQLFiltro
                stSQL = stSQL + " GROUP BY A." + vecVariables_Fijas[i]
                stSQL = stSQL + " ORDER BY A." + vecVariables_Fijas[i]
                myCursor.execute(stSQL)
                regAux = myCursor.fetchall()
                vecEstados = [] 
                for row in regAux:
                    vecEstados.append(row[0])
                
                #Revisa si hay orden en los valores
                NumOrden = 0
                stSQL = "SELECT COUNT(*) FROM " + stDB + ".CAMPO_VALOR_ORDEN WHERE CAMPO ='" +  vecVariables_Fijas[i] + "'"
                myCursor.execute(stSQL)
                regAux = myCursor.fetchall()
                for resSQL in regAux:
                    NumOrden = resSQL[0]
                if NumOrden > 0:
                    auxEstados = vecEstados
                    vecEstados = []
                    stSQL = "SELECT VALOR FROM " + stDB + ".CAMPO_VALOR_ORDEN WHERE CAMPO ='" +  vecVariables_Fijas[i] + "' ORDER BY ORDEN"
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                    for resSQL in regAux:
                        for row in auxEstados:
                            if resSQL[0] == row:
                                vecEstados.append(row)
                               
                for row in vecEstados:
                    arhcPGMX.write('          <State name="' + row + '" />\n')
                    
                arhcPGMX.write('       </States>\n')
                arhcPGMX.write('      </Variable>\n')
                vecNodo.append([vecVariables_Fijas[i],vecEstados])
                
            #3.2.- Variables Estado [0]
            print('3.-- Variables Estado 0 '  + time.strftime("%H:%M:%S"))
            posX = 500
            if banDecision == True:
                posX = 300
            posY = 100
            k = 1
            for i in range(0,len(vecVariables)):
                banAgregar = True
                for j in range(0,len(vecVariables_Fijas)):
                    if vecVariables[i] == vecVariables_Fijas[j]:
                        banAgregar = False
                        
                stRole="chance"   
                if banDecision == True:
                    for row in vecDecision:
                        if vecVariables[i] == row:
                            banAgregar = False
                            stRole="decision"
                if banAgregar == True:
                    posY = 50 * k
                    k = k + 1
                    
                    stNombreNodo = vecVariables[i] + '" timeSlice="0"'
                    if idTipoDiagrama == 1:
                        stNombreNodo = vecVariables[i] + '_0"'
                    
                    arhcPGMX.write('      <Variable name="' + stNombreNodo + ' type="finiteStates" role="' + stRole + '">\n')
                    arhcPGMX.write('        <Coordinates x="' + str(posX) + '" y="' + str(posY) + '" />\n')
                    arhcPGMX.write('        <States>\n')
                    stSQL = "SELECT A." + vecVariables[i]
                    stSQL = stSQL + stSQLFiltro
                    stSQL = stSQL + " GROUP BY A." + vecVariables[i]
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                    vecEstados = []
                    for row in regAux:
                        vecEstados.append(row[0])
                        
                    stSQL = "SELECT B." + vecVariables[i]
                    stSQL = stSQL + stSQLFiltro
                    stSQL = stSQL + " GROUP BY B." + vecVariables[i]
                    stSQL = stSQL + " ORDER BY B." + vecVariables[i]
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                    for row in regAux:
                        banAgregar = True
                        for j in range(0,len(vecEstados)):
                            if vecEstados[j]==row[0]:
                                banAgregar = False
                        if banAgregar == True:
                            vecEstados.append(row[0])
                       
                    #Revisa si hay orden en los valores
                    NumOrden = 0
                    stSQL = "SELECT COUNT(*) FROM " + stDB + ".CAMPO_VALOR_ORDEN WHERE CAMPO ='" +  vecVariables[i] + "'"
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                    for resSQL in regAux:
                        NumOrden = resSQL[0]
                    if NumOrden > 0:
                        auxEstados = vecEstados
                        vecEstados = []
                        stSQL = "SELECT VALOR FROM " + stDB + ".CAMPO_VALOR_ORDEN WHERE CAMPO ='" +  vecVariables[i] + "' ORDER BY ORDEN"
                        myCursor.execute(stSQL)
                        regAux = myCursor.fetchall()
                        for resSQL in regAux:
                            for row in auxEstados:
                                if resSQL[0] == row:
                                    vecEstados.append(row)
                     
                    for row in vecEstados:
                        arhcPGMX.write('          <State name="' + str(row) + '" />\n')
                            
            
                    arhcPGMX.write('       </States>\n')
                    arhcPGMX.write('      </Variable>\n')
                    vecNodo.append([vecVariables[i],vecEstados])
                    
                    
            #3.3.- Variables Estado [0]
            print('4.-- Variables Decisión estado 0 ' + time.strftime("%H:%M:%S"))
            posX = 500
            if banDecision == True:
                for i in range(0,len(vecDecision)):
                    banAgregar = True
                    for j in range(0,len(vecVariables_Fijas)):
                        if vecDecision[i] == vecVariables_Fijas[j]:
                            banAgregar = False
            
                    if banAgregar == True:
                        posX = posX + 75 * i
                        posY = posY + 75 * i
                        k = k + 1
                        
                        stNombreNodo = vecDecision[i] + '" timeSlice="0"'
                        if idTipoDiagrama == 1:
                             stNombreNodo = vecDecision[i] + '_0"'
                                                     
                        arhcPGMX.write('      <Variable name="' + stNombreNodo + ' type="finiteStates" role="decision">\n')
                        arhcPGMX.write('        <Coordinates x="' + str(posX) + '" y="' + str(posY) + '" />\n')
                        arhcPGMX.write('        <States>\n')
                        stSQL = "SELECT A." + vecDecision[i]
                        stSQL = stSQL + stSQLFiltro
                        stSQL = stSQL + "GROUP BY A." + vecDecision[i]
                        myCursor.execute(stSQL)
                        regAux = myCursor.fetchall()
                        vecEstados = []
                        for row in regAux:
                            vecEstados.append(row[0])
                            
                        stSQL = "SELECT B." + vecDecision[i]
                        stSQL = stSQL + stSQLFiltro
                        stSQL = stSQL + "GROUP BY B." + vecDecision[i]
                        stSQL = stSQL + " ORDER BY B." + vecDecision[i]
                        myCursor.execute(stSQL)
                        regAux = myCursor.fetchall()
                        for row in regAux:
                            banAgregar = True
                            for j in range(0,len(vecEstados)):
                                if vecEstados[j]==row[0]:
                                    banAgregar = False
                            if banAgregar == True:
                                vecEstados.append(row[0])
                           
                        #Revisa si hay orden en los valores
                        NumOrden = 0
                        stSQL = "SELECT COUNT(*) FROM " + stDB + ".CAMPO_VALOR_ORDEN WHERE CAMPO ='" +  vecDecision[i] + "'"
                        myCursor.execute(stSQL)
                        regAux = myCursor.fetchall()
                        for resSQL in regAux:
                            NumOrden = resSQL[0]
                        if NumOrden > 0:
                            auxEstados = vecEstados
                            vecEstados = []
                            stSQL = "SELECT VALOR FROM " + stDB + ".CAMPO_VALOR_ORDEN WHERE CAMPO ='" +  vecDecision[i] + "' ORDER BY ORDEN"
                            myCursor.execute(stSQL)
                            regAux = myCursor.fetchall()
                            for resSQL in regAux:
                                for row in auxEstados:
                                    if resSQL[0] == row:
                                        vecEstados.append(row)
                         
                        for row in vecEstados:
                            arhcPGMX.write('          <State name="' + str(row) + '" />\n')

                        arhcPGMX.write('       </States>\n')
                        arhcPGMX.write('      </Variable>\n')
                        vecNodo.append([vecDecision[i],vecEstados])
                    
            #3.4.- Variables Estado [1], Clases
            print('5.-- Variables Estado 1 ' + time.strftime("%H:%M:%S"))
            posX = 900
            posY = 100
            k = 1
            for i in range(0,len(vecClase)):
                posY = 50 * (i+1)
                k = k + 1
                
                stNombreNodo = vecClase[i] + '" timeSlice="1"'
                if idTipoDiagrama == 1:
                     stNombreNodo = vecClase[i] + '_1"'
                
                arhcPGMX.write('      <Variable name="' + stNombreNodo + ' type="finiteStates" role="chance">\n')
                arhcPGMX.write('        <Coordinates x="' + str(posX) + '" y="' + str(posY) + '" />\n')
                arhcPGMX.write('        <States>\n')
                for row in vecNodo:
                    if row[0] == vecClase[i]:
                        for col in row[1]:
                            arhcPGMX.write('          <State name="' + str(col) + '" />\n')
            
                arhcPGMX.write('       </States>\n')
                arhcPGMX.write('      </Variable>\n')
            
            
            #3.5.- Variables Clases Fija
            print('6.-- Variables Estado Fijo ' + time.strftime("%H:%M:%S"))
            posX = 1200
            posY = 500
            k = 1
            for i in range(0,len(vecClase_Fija)):
                posY = 100 * (i+1)
                k = k + 1
                
                stNombreNodo = vecClase_Fija[i] + '" timeSlice="1"'
                if idTipoDiagrama == 1:
                     stNombreNodo = vecClase_Fija[i] + '_1"'
                
                arhcPGMX.write('      <Variable name="' + stNombreNodo + ' type="finiteStates" role="chance">\n')
                arhcPGMX.write('        <Coordinates x="' + str(posX) + '" y="' + str(posY) + '" />\n')
                arhcPGMX.write('        <States>\n')
                
                stSQL = "SELECT B." + vecClase_Fija[i]
                stSQL = stSQL + stSQLFiltro
                stSQL = stSQL + "GROUP BY B." + vecClase_Fija[i]
                stSQL = stSQL + " ORDER BY B." + vecClase_Fija[i]
                myCursor.execute(stSQL)
                regAux = myCursor.fetchall()
                vecEstados = []
                for row in regAux:
                    vecEstados.append(row[0])
                    
                #Revisa si hay orden en los valores
                NumOrden = 0
                stSQL = "SELECT COUNT(*) FROM " + stDB + ".CAMPO_VALOR_ORDEN WHERE CAMPO ='" +  vecClase_Fija[i] + "'"
                myCursor.execute(stSQL)
                regAux = myCursor.fetchall()
                for resSQL in regAux:
                    NumOrden = resSQL[0]
                if NumOrden > 0:
                    auxEstados = vecEstados
                    vecEstados = []
                    stSQL = "SELECT VALOR FROM " + stDB + ".CAMPO_VALOR_ORDEN WHERE CAMPO ='" +  vecClase_Fija[i] + "' ORDER BY ORDEN"
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                    for resSQL in regAux:
                        for row in auxEstados:
                            if resSQL[0] == row:
                                vecEstados.append(row)
                                
                for row in vecEstados:
                    arhcPGMX.write('          <State name="' + str(row) + '" />\n')

                arhcPGMX.write('       </States>\n')
                arhcPGMX.write('      </Variable>\n')
                vecNodo.append([vecClase_Fija[i],vecEstados])
            
            #3.6.- Utilidad / Costos de las Decisiones
            print('7.-- Utilidad / Costos de las Decisiones ' + time.strftime("%H:%M:%S"))
            posX = 1500
            posY = 300
            
            stNombreNodo = 'UTILIDAD" timeSlice="1"'
            if idTipoDiagrama == 1:
                 stNombreNodo = 'UTILIDAD_1"'
            
            arhcPGMX.write('      <Variable name="' + stNombreNodo + ' type="numeric" role="utility">\n')
            arhcPGMX.write('        <Coordinates x="' + str(posX) + '" y="' + str(posY) + '" />\n')
            if idTipoDiagrama == 1:
                arhcPGMX.write('        <AdditionalProperties>\n')
                arhcPGMX.write('          <Property name="Purpose" value="cost" />\n')
                arhcPGMX.write('        </AdditionalProperties>\n')
            
            arhcPGMX.write('        <Unit />\n')
            arhcPGMX.write('        <Precision>0.01</Precision>\n')
            arhcPGMX.write('        <Criterion name="---" />\n')
            arhcPGMX.write('      </Variable>\n')
            
            for i in range(0,len(vecDecision)):
                posX = 300 + i * 200
                posY = 650 + i * 50
                
                stNombreNodo = 'COSTO_' + vecDecision[i] + '" timeSlice="0"'
                if idTipoDiagrama == 1:
                     stNombreNodo = 'COSTO_' + vecDecision[i] + '_0"'
            
                arhcPGMX.write('      <Variable name="' + stNombreNodo + ' type="numeric" role="utility">\n')
                arhcPGMX.write('        <Coordinates x="' + str(posX) + '" y="' + str(posY) + '" />\n')
                arhcPGMX.write('        <Unit />\n')
                arhcPGMX.write('        <Precision>0.01</Precision>\n')
                arhcPGMX.write('        <Criterion name="---" />\n')
                arhcPGMX.write('      </Variable>\n')
            
            
            arhcPGMX.write('    </Variables>\n')
            
            arhcPGMX.write('    <Links>\n')
            
            print('8.-- Links Variables -> Nodos Decisión '  + time.strftime("%H:%M:%S"))
            vecLinksDecision = []
            for i in range(0,len(vecDecision)):
                vecNodoLink = []
                for j in range(0,len(vecVariables)):
                    banAgregar = True
                    for k in range(0,len(vecDecision)):
                        if vecVariables[j] == vecDecision[k]:
                            banAgregar = False
                    if banAgregar == True:
                        stTimeSlice = 'timeSlice="0"'
                        for k in range(0,len(vecVariables_Fijas)):
                            if vecVariables[j] == vecVariables_Fijas[k]:
                                stTimeSlice = ''
                                
                        stNombreNodo = vecVariables[j] + '" ' + stTimeSlice
                        if idTipoDiagrama == 1 and stTimeSlice == 'timeSlice="0"':
                             stNombreNodo = vecVariables[j] + '_0"'
                        
                        arhcPGMX.write('      <Link directed="true">\n')
                        arhcPGMX.write('        <Variable name="' +  stNombreNodo + ' />\n')
                        
                        stNombreNodo = vecDecision[i] + '" timeSlice="0"'
                        if idTipoDiagrama == 1 :
                             stNombreNodo = vecDecision[i] + '_0"'
                        
                        arhcPGMX.write('        <Variable name="' + stNombreNodo + ' />\n')
                        arhcPGMX.write('      </Link>\n')
                        vecNodoLink.append(vecVariables[j])
                vecLinksDecision.append([vecDecision[i],vecNodoLink])
            
            print('9.-- Links Clases ' + time.strftime("%H:%M:%S"))
            #4.- Reducción de varaiables y links Clases
            
            vecLinks = []
            for iClase in range(0,len(vecClase)):
                #4.1.- Recuperación de datos
                i = len(vecClase) - iClase - 1
                
                stSQL = "SELECT "
                for j in range(0,len(vecVariables)):
                    stSQL = stSQL + "A." + vecVariables[j] + ", "
                vecClase_1 = []
                if banIncluye_Estado_1 == True:
                    for j in range(0,len(vecClase)):
                        if vecClase[i] != vecClase[j]:
                            stSQL = stSQL + "B." + vecClase[j] + " " + vecClase[j] + "_1, "
                            vecClase_1.append(vecClase[j] + "_1")
             
                stSQL = stSQL + "B." + vecClase[i] + " CLASE"
                stSQL = stSQL + stSQLFiltro 
            
                myCursor.execute(stSQL)
                regAux = myCursor.fetchall()
            
                matDatos=[]
                for row in regAux:
                    vRec = []
                    for k in range(0,len(row)):
                        vRec.append(row[k])
                    matDatos.append(vRec)
                
                #4.2.- Cálculo de la Ganancia de la Información y del Ratio de Ganancia
                matGainR = []
                sumGainR = 0
                for j in range(0,len(matDatos[0])-1):
                    auxColX = [row[j] for row in matDatos]
                    auxColY = [row[len(matDatos[0])-1] for row in matDatos]
                    IG_Y_X = EntropiaX(auxColY)-EntropiaY_X(auxColX,auxColY)
                    GainR_Y_X = IG_Y_X/EntropiaX(auxColX)
                    if j < len(vecVariables):
                        vRen = [vecVariables[j],IG_Y_X,GainR_Y_X,0,0]
                    else:
                        vRen = [vecClase_1[j-len(vecVariables)],IG_Y_X,GainR_Y_X,0,0]
                    matGainR.append(vRen)
                    sumGainR = sumGainR + IG_Y_X
                    
            
                #4.3.- Ordenación de los valores por Ratio
                for j in range(0,len(matGainR)-1):
                    for k in range (j+1,len(matGainR)):
                        if matGainR[j][1] < matGainR[k][1]:
                            vRen = matGainR[j]
                            matGainR[j] = matGainR[k]
                            matGainR[k] = vRen
                 
                #4.4.- Cálculo de los acumulados y selección de las variables 
                vAcumulado = 0
                vecNodoLink = []
                vCorte = float(vCorte_GainR)
                maxCampo = NumMaxCampos
                for j  in range(0,len(matGainR)):
                    vAcumulado = vAcumulado + matGainR[j][1]
                    matGainR[j][3] = vAcumulado
                    matGainR[j][4] = vAcumulado / sumGainR
                    banAgregar = True
                    stAuxCampo = matGainR[j][0]
                    if stAuxCampo.find("_1") != -1:
                        stAuxCampo = stAuxCampo.replace("_1","")
                        for k in range(0,len(vecLinks)):
                            if vecLinks[k][0] == stAuxCampo:
                                banAgregar = False

                    if banAgregar == True:
                        if vCorte > 0 and maxCampo > 0:
                            vecNodoLink.append(matGainR[j][0])
                            maxCampo = maxCampo - 1
                        vCorte = vCorte - matGainR[j][1] / sumGainR
            
                #4.5.- Creación de los links
                vecNodos = []
                for j in range(0,len(vecNodoLink)):
                    
                    stTimeSlice = ' timeSlice="0"'
                    stNodoLink = vecNodoLink[j] 
                    for k in range(0,len(vecVariables_Fijas)):
                        if vecNodoLink[j] == vecVariables_Fijas[k]:
                            stTimeSlice = ''
                            
                    if stNodoLink.find("_1") != -1:
                        stTimeSlice = ' timeSlice="1"'
                        stNodoLink = stNodoLink[:-2]
            
                    arhcPGMX.write('      <Link directed="true">\n')
                    
                    stNombreNodo = stNodoLink + '" ' + stTimeSlice
                    if idTipoDiagrama == 1 and stTimeSlice == ' timeSlice="0"':
                         stNombreNodo = stNodoLink + '_0"'
                    if idTipoDiagrama == 1 and stTimeSlice == ' timeSlice="1"':
                         stNombreNodo = stNodoLink + '_1"'
                    
                    arhcPGMX.write('        <Variable name="' + stNombreNodo + ' />\n')
                    
                    stNombreNodo = vecClase[i] + '" timeSlice="1"'
                    if idTipoDiagrama == 1:
                         stNombreNodo = vecClase[i] + '_1"'
                    
                    arhcPGMX.write('        <Variable name="' + stNombreNodo + ' />\n')
                    arhcPGMX.write('      </Link>\n')
                    vecNodos.append(vecNodoLink[j])
                    
                vecLinks.append([vecClase[i],vecNodos])
                
            
            #5.- Reducción de varaiables y links Clases Fijas
            print('10.-- Links Clases Fijas ' + time.strftime("%H:%M:%S"))
            for i in range(0,len(vecClase_Fija)):
                #5.1.- Recuperación de datos
                
                stSQL = "SELECT "
                for j in range(0,len(vecVariables)):
                    stSQL = stSQL + "A." + vecVariables[j] + ", "
                vecClase_1 = []
                
                for j in range(0,len(vecClase)):
                    stSQL = stSQL + "B." + vecClase[j] + " " + vecClase[j] + "_1, "
                    vecClase_1.append(vecClase[j] + "_1")
             
                stSQL = stSQL + "B." + vecClase_Fija[i] + " CLASE"
                stSQL = stSQL + stSQLFiltro 
            
                myCursor.execute(stSQL)
                regAux = myCursor.fetchall()
                
                matDatos=[]
                for row in regAux:
                    vRec = []
                    for k in range(0,len(row)):
                        vRec.append(row[k])
                    matDatos.append(vRec)
                    
                #5.2.- Cálculo de la Ganancia de la Información y del Ratio de Ganancia
                matGainR = []
                sumGainR = 0
                for j in range(0,len(matDatos[0])-1):
                    auxColX = [row[j] for row in matDatos]
                    auxColY = [row[len(matDatos[0])-1] for row in matDatos]
                    IG_Y_X = EntropiaX(auxColY)-EntropiaY_X(auxColX,auxColY)
                    GainR_Y_X = IG_Y_X/EntropiaX(auxColX)
                    if j < len(vecVariables):
                        vRen = [vecVariables[j],IG_Y_X,GainR_Y_X,0,0]
                    else:
                        vRen = [vecClase_1[j-len(vecVariables)],IG_Y_X,GainR_Y_X,0,0]
                    matGainR.append(vRen)
                    sumGainR = sumGainR + IG_Y_X
                    
                #5.3.- Ordenación de los valores por Ratio
                for j in range(0,len(matGainR)-1):
                    for k in range (j+1,len(matGainR)):
                        if matGainR[j][1] < matGainR[k][1]:
                            vRen = matGainR[j]
                            matGainR[j] = matGainR[k]
                            matGainR[k] = vRen
                            
                #5.4.- Cálculo de los acumulados y selección de las variables 
                vAcumulado = 0
                vecNodoLink = []
                vCorte = float(vCorte_GainR)
                maxCampo = NumMaxCampos 
                for j  in range(0,len(matGainR)):
                    vAcumulado = vAcumulado + matGainR[j][1]
                    matGainR[j][3] = vAcumulado
                    matGainR[j][4] = vAcumulado / sumGainR
                    if vCorte > 0 and maxCampo > 0:
                        vecNodoLink.append(matGainR[j][0])
                        maxCampo = maxCampo - 1
                    vCorte = vCorte - matGainR[j][1] / sumGainR
                    
                #5.5.- Creación de los links
                vecNodos = []
                for j in range(0,len(vecNodoLink)):
                    
                    stTimeSlice = ' timeSlice="0"'
                    stNodoLink = vecNodoLink[j] 
                    for k in range(0,len(vecVariables_Fijas)):
                        if vecNodoLink[j] == vecVariables_Fijas[k]:
                            stTimeSlice = ''
                            
                    if stNodoLink.find("_1") != -1:
                        stTimeSlice = ' timeSlice="1"'
                        stNodoLink = stNodoLink[:-2]
            
                    arhcPGMX.write('      <Link directed="true">\n')
                    
                    stNombreNodo = stNodoLink + '" ' + stTimeSlice
                    if idTipoDiagrama == 1:
                        if stTimeSlice == ' timeSlice="0"':
                            stNombreNodo =  stNodoLink + '_0"'
                        if stTimeSlice == ' timeSlice="1"':
                            stNombreNodo =  stNodoLink + '_1"'
                    
                    arhcPGMX.write('        <Variable name="' + stNombreNodo + ' />\n')
                    
                    stNombreNodo = vecClase_Fija[i] + '" timeSlice="1"'
                    if idTipoDiagrama == 1:
                        stNombreNodo = vecClase_Fija[i] + '_1"'
                    
                    arhcPGMX.write('        <Variable name="' + stNombreNodo + ' />\n')
                    arhcPGMX.write('      </Link>\n')
                    vecNodos.append(vecNodoLink[j])
                    
                vecLinks.append([vecClase_Fija[i] , vecNodos])
            
            print('11.-- Links para las Utilidades / Costos ' + time.strftime("%H:%M:%S")) 
            for row in vecVariables_Utilidad:
                arhcPGMX.write('      <Link directed="true">\n')
                
                stNombreNodo = row + '" timeSlice="1"'
                if idTipoDiagrama == 1:
                    stNombreNodo = row + '_1"'
                
                arhcPGMX.write('        <Variable name="' + stNombreNodo + ' />\n')
                
                stNombreNodo = 'UTILIDAD" timeSlice="1"'
                if idTipoDiagrama == 1:
                    stNombreNodo = 'UTILIDAD_1"'
                
                arhcPGMX.write('        <Variable name="' + stNombreNodo + ' />\n')
                arhcPGMX.write('      </Link>\n')
            
            for i in range(0,len(vecDecision)):    
                arhcPGMX.write('      <Link directed="true">\n')
                    
                stNombreNodo = vecDecision[i] + '" timeSlice="0"'
                if idTipoDiagrama == 1:
                    stNombreNodo = vecDecision[i] + '_0"'
                
                arhcPGMX.write('        <Variable name="' + stNombreNodo + ' />\n')
                
                stNombreNodo = 'COSTO_' + vecDecision[i] + '" timeSlice="0"'
                if idTipoDiagrama == 1:
                    stNombreNodo = 'COSTO_' + vecDecision[i] + '_0"'
                
                arhcPGMX.write('        <Variable name="' + stNombreNodo + ' />\n')
                arhcPGMX.write('      </Link>\n')
                
            arhcPGMX.write('    </Links>\n')    
            
            #6.-Obtención de las probabilidades
            arhcPGMX.write('    <Potentials>\n')
            #6.1.- Probabilidades Variables Fijas y Estado 0
            print('12.-- Potencias Variables Fijas y Estado 0 ' + time.strftime("%H:%M:%S"))
            for row in vecNodo:
                banAvanzar = True
                
                for j in range(0,len(vecClase_Fija)):
                    if row[0] == vecClase_Fija[j]:
                        banAvanzar = False
                            
                if banDecision == True:
                    for j in range(0,len(vecDecision)):
                        if row[0] == vecDecision[j]:
                            banAvanzar = False
                
                if banAvanzar == True:
                    
                    arhcPGMX.write('      <Potential type="Table" role="joinProbability">\n')
                    arhcPGMX.write('        <Variables>\n')
                    stTimeSlice = ' timeSlice="0"'
                    stVariables = row[0] + "_0"
                    for k in range(0,len(vecVariables_Fijas)):
                        if row[0] == vecVariables_Fijas[k]:
                            stTimeSlice = ''
                            stVariables = row[0]
                            
                    stNombreNodo = row[0] + '" ' + stTimeSlice
                    if idTipoDiagrama == 1 and stTimeSlice == ' timeSlice="0"':
                        stNombreNodo = row[0] + '_0"'
                            
                    arhcPGMX.write('          <Variable name="' + stNombreNodo + '  />\n')
                    arhcPGMX.write('        </Variables>\n')
                    vecProb = []
                    varTotal = 0
                    stProb = Busca_Probabilidad(myConexion, stDB, idParticion, stVariables, stExperimento)
                    if stProb == "N/E":
                        Inicio = dt.datetime.today()
                        stProb = ""
                        for valor in row[1]:
                            stSQL = "SELECT COUNT(*) FROM " + stDB + ".MID_DATOS WHERE " + row[0] + "='" + str(valor) + "' AND ANNO=2017 AND MUESTRA <> " + str(idParticion)
                            myCursor.execute(stSQL)
                            regAux = myCursor.fetchall()
                            for resSQL in regAux:
                                vecProb.append(resSQL[0])
                                varTotal =  varTotal + resSQL[0]
                                
                        #Correción de Laplace
                        if varTotal + vCorrecionLaplace * len(vecProb) > 0 :
                            for i in range(0,len(vecProb)):
                                vecProb[i] = (vecProb[i] + vCorrecionLaplace ) / (varTotal + vCorrecionLaplace * len(vecProb))
                        
                        for i in range(0,len(vecProb)):
                                stProb = stProb + ' ' + str(vecProb[i])
                        Fin = dt.datetime.today()
                        Tiempo = Dif_Tiempo(Inicio,Fin)
                        Guarda_Probabilidad(myConexion, stDB, idParticion, stVariables, stProb, stExperimento, Inicio, Fin, Tiempo)
                    arhcPGMX.write('        <Values>' + stProb +'</Values>\n')
                    arhcPGMX.write('      </Potential>\n')
                    
            #6.2.- Probabilidades Variables Estado 1
            print('13.-- Potencias Estado 1 ' + time.strftime("%H:%M:%S"))
            for row in vecLinks:

                stConteo = '*'
                stTimeSlice = ' timeSlice="1"'
                
                arhcPGMX.write('      <Potential type="Table" role="conditionalProbability">\n')
                arhcPGMX.write('        <Variables>\n')
                
                stNombreNodo = row[0] + '" ' + stTimeSlice
                if idTipoDiagrama == 1 and stTimeSlice == ' timeSlice="1"':
                    stNombreNodo = row[0] + '_1"'
                
                stVariables = row[0] + '_1'
                arhcPGMX.write('          <Variable name="' + stNombreNodo + ' />\n')
                vClaseVal = []
                for auxNodo in vecNodo:
                    if row[0] == auxNodo[0]:
                        vClaseVal = auxNodo[1]
                vecMax = []
                vecCon = []
                vVariableVal = []
           
                for auxVar in row[1]:
                    auxStVar = auxVar + '_0'
                    stTimeSlice = ' timeSlice="0"'
                    for k in range(0,len(vecVariables_Fijas)):
                        if auxVar == vecVariables_Fijas[k]:
                            stTimeSlice = ''
                            auxStVar = auxVar
                            
                    if auxVar.find("_1") != -1:
                        stTimeSlice = ' timeSlice="1"'
                        auxVar = auxVar[:-2]
                        auxStVar = auxVar + '_1'
                    
                    for k in range(0,len(vecClase_Fija)):
                        if auxVar == vecClase_Fija[k]:
                            stTimeSlice = ' timeSlice="1"'
                            stConteo = 'DISTINCT ' + vecClase_Fija_Conteo[k]
                            auxStVar = auxVar + '_1'
                            
                    stVariables =  stVariables + "," + auxStVar
                    stNombreNodo = auxVar + '" ' + stTimeSlice
                    if idTipoDiagrama == 1:
                        if stTimeSlice == ' timeSlice="0"':
                            stNombreNodo = auxVar + '_0"'
                        if stTimeSlice == ' timeSlice="1"':
                            stNombreNodo = auxVar + '_1"'
                        
                    arhcPGMX.write('          <Variable name="' + stNombreNodo + ' />\n')
                    for auxNodo in vecNodo:
                        if auxVar == auxNodo[0]:
                            vVariableVal.append(auxNodo[1])
                            vecMax.append(len(auxNodo[1])-1)
                            vecCon.append(0)
                
                vecProb = []
                banAvanzar = True
                while True:
                    vecCampos = []
                    for i in range(0,len(vecCon)):
                        vecCampos.append(vVariableVal[i][vecCon[i]])
                    vecProb.append(vecCampos)
                    
                    auxCol = 0 #(len(vecCon)-1) 
                    vecCon[auxCol] = vecCon[auxCol] + 1
                    for i in range(0,len(vecCon)):
                        auxCol = i   #(len(vecCon)-1) - i
                        if vecCon[auxCol] > vecMax[auxCol]:
                            vecCon[auxCol] = 0
                            auxCol = auxCol + 1
                            if auxCol < len(vecCon):
                                vecCon[auxCol] = vecCon[auxCol] + 1
                            else:
                                banAvanzar = False
                    if banAvanzar == False:
                        break
                
                
                stProb = Busca_Probabilidad(myConexion, stDB, idParticion, stVariables, stExperimento)
                if stProb == "N/E":
                    Inicio = dt.datetime.today()
                    stProb = ""
                    for vProb in vecProb:
                        stSQL = "SELECT  Count(" + stConteo + ")"
                        stSQL = stSQL + stSQLFiltro 
                        stMismoCampoEstado_0 = ""
                        for i in range(0,len(vProb)):
                            stCampo = "A." + row[1][i]
                            if row[1][i].find("_1") != -1:
                                stCampo = "B." + row[1][i][:-2]
                            else:
                                if row[1][i] == row[0]:
                                    stMismoCampoEstado_0  = " AND " + stCampo + "='" + str(vProb[i]) + "'"
                            stSQL = stSQL + " AND " + stCampo + "='" + str(vProb[i]) + "'"
                         
                        vecProbRes = []
                        varTotal = 0
                        for i in range(0,len(vClaseVal)): 
                            myCursor.execute(stSQL + " AND B." + row[0] +"='" + str(vClaseVal[i]) + "'")
                            regAux = myCursor.fetchall()
                            for resSQL in regAux:
                                vecProbRes.append(resSQL[0])
                                varTotal =  varTotal + resSQL[0]
                        
                        
                        if varTotal == 0 :
                            
                            stSQL = "SELECT  Count(" + stConteo + ")"
                            stSQL = stSQL + stSQLFiltro 
                            myCursor.execute(stSQL)
                            regAux = myCursor.fetchall()
                            for resSQL in regAux:
                                varTotal = resSQL[0]
                            if varTotal > 0:
                                for i in range(0,len(vClaseVal)): 
                                    myCursor.execute(stSQL + " AND B." + row[0] +"='" + str(vClaseVal[i]) + "' " )
                                    
                                    for resSQL in regAux:
                                        vecProbRes[i]=resSQL[0]
                                        
                        #Correción de Laplace
                        if varTotal + vCorrecionLaplace * len(vClaseVal) > 0 :
                            for i in range(0,len(vecProbRes)):
                                vecProbRes[i] = (vecProbRes[i] + vCorrecionLaplace ) / (varTotal + vCorrecionLaplace * len(vClaseVal))
                                        

                        for i in range(0,len(vecProbRes)):
                            stProb = stProb + ' ' + str(vecProbRes[i]) 
                            

                    Fin = dt.datetime.today()
                    Tiempo = Dif_Tiempo(Inicio,Fin)
                    Guarda_Probabilidad(myConexion, stDB, idParticion, stVariables, stProb, stExperimento, Inicio, Fin, Tiempo)

                arhcPGMX.write('        </Variables>\n')
                arhcPGMX.write('        <Values>' + stProb + '</Values>\n')
                arhcPGMX.write('      </Potential>\n')

                
            #7.- Cálculo de Utilidad de Estado Final
            print('14.-- Cálculo de las Utilidades '  + time.strftime("%H:%M:%S"))
            if idTipoDiagrama == 0:
                #7.1.-  Markov Influence Diagram
                arhcPGMX.write('       <Potential type="Tree/ADD" role="utility">\n')
                
                stNombreNodo = 'UTILIDAD" timeSlice="1"'
                if idTipoDiagrama == 1:
                    stNombreNodo = 'UTILIDAD_1"'
                
                arhcPGMX.write('        <UtilityVariable name="' + stNombreNodo + ' />\n')
                arhcPGMX.write('        <Variables>\n')
                for row in vecVariables_Utilidad:
                    
                    stNombreNodo = row + '" timeSlice="1"'
                    if idTipoDiagrama == 1:
                        stNombreNodo = row + '_1"'
                    
                    arhcPGMX.write('          <Variable name="' + stNombreNodo + ' />\n')
                arhcPGMX.write('        </Variables>\n')
                
                stNombreNodo = 'FIN_PUESTO" timeSlice="1"'
                if idTipoDiagrama == 1:
                    stNombreNodo = 'FIN_PUESTO_1"'
                
                arhcPGMX.write('        <TopVariable name="' + stNombreNodo + ' />\n')
                arhcPGMX.write('        <Branches>\n')
                
                #7.1.1.- Rama cuando no hay fin de plaza
                arhcPGMX.write('          <Branch>\n')
                arhcPGMX.write('            <States>\n')
                arhcPGMX.write('              <State name="0" />\n')
                arhcPGMX.write('            </States>\n')
                arhcPGMX.write('            <Potential type="Tree/ADD">\n')
                
                stNombreNodo = 'UTILIDAD" timeSlice="1"'
                if idTipoDiagrama == 1:
                    stNombreNodo = 'UTILIDAD_1"'
                
                arhcPGMX.write('              <UtilityVariable name="' + stNombreNodo + ' />\n')
                arhcPGMX.write('              <Variables>\n')
                for row in vecVariables_Utilidad:
                    
                    stNombreNodo = row + '" timeSlice="1"'
                    if idTipoDiagrama == 1:
                        stNombreNodo = row + '_1"'
                    
                    arhcPGMX.write('                <Variable name="' + stNombreNodo + ' />\n')
                arhcPGMX.write('              </Variables>\n')
                
                stNombreNodo = 'GRUPO_PUESTO" timeSlice="1"'
                if idTipoDiagrama == 1:
                    stNombreNodo = 'GRUPO_PUESTO_1"'
                
                arhcPGMX.write('              <TopVariable name="' + stNombreNodo + ' />\n')
                arhcPGMX.write('              <Branches>\n')
                for row in vecNodo:
                    if row[0] == 'GRUPO_PUESTO':
                        for i in range(0,len(row[1])):
                            stSQL = "SELECT "
                            stSQL = stSQL + " AVG(SBA_IMPORTE * 0.15) PERMANENCIA, "
                            stSQL = stSQL + " (-1) * AVG(SBA_IMPORTE * 0.15 + SEPARACION ) SEPARACION, "
                            stSQL = stSQL + " (-1) * AVG(SBA_IMPORTE * 0.15 + ABANDONO ) ABANDONO "
                            stSQL = stSQL + " FROM " + stDB + ".MID_DATOS "
                            stSQL = stSQL + " WHERE ANNO >= 2017 "
                            stSQL = stSQL + " AND MUESTRA <> " + str(idParticion) + " "
                            stSQL = stSQL + " AND GRUPO_PUESTO = '" + row[1][i]  + "'"
                            myCursor.execute(stSQL)
                            regAux = myCursor.fetchall()
                            stUtilidades = "0 0 0 0"
                            for resSQL in regAux:
                                stUtilidades = str(resSQL[0]) + " " + str(resSQL[1])  + " " + str(resSQL[2]) + " 0"
                            
                            arhcPGMX.write('                <Branch>\n')
                            arhcPGMX.write('                  <States>\n')
                            arhcPGMX.write('                    <State name="' + row[1][i] + '" />\n')
                            arhcPGMX.write('                  </States>\n')
                            arhcPGMX.write('                  <Potential type="Table">\n')
                            
                            stNombreNodo = 'UTILIDAD" timeSlice="1"'
                            if idTipoDiagrama == 1:
                                stNombreNodo = 'UTILIDAD_1"'
                            
                            arhcPGMX.write('                    <UtilityVariable name="' + stNombreNodo + ' />\n')
                            arhcPGMX.write('                    <Variables>\n')
                            
                            stNombreNodo = 'ES_SEPARACION" timeSlice="1"'
                            if idTipoDiagrama == 1:
                                stNombreNodo = 'ES_SEPARACION_1"'
                            
                            arhcPGMX.write('                      <Variable name="' + stNombreNodo + ' />\n')
                            
                            stNombreNodo = 'ES_ABANDONO" timeSlice="1"'
                            if idTipoDiagrama == 1:
                                stNombreNodo = 'ES_ABANDONO_1"'
                            
                            arhcPGMX.write('                      <Variable name="' + stNombreNodo + ' />\n')
                            arhcPGMX.write('                    </Variables>\n')
                            arhcPGMX.write('                    <Values>' + stUtilidades + '</Values>\n')
                            arhcPGMX.write('                  </Potential>\n')
                            arhcPGMX.write('                </Branch>\n')
                            
                arhcPGMX.write('              </Branches>\n')
                arhcPGMX.write('            </Potential>\n')
                arhcPGMX.write('          </Branch>\n')
                               
                #7.1.1.- Rama cuando hay fin de plaza            
                arhcPGMX.write('          <Branch>\n')
                arhcPGMX.write('            <States>\n')
                arhcPGMX.write('              <State name="1" />\n')
                arhcPGMX.write('            </States>\n')
                arhcPGMX.write('            <Potential type="Table">\n')
                
                stNombreNodo = 'UTILIDAD" timeSlice="1"'
                if idTipoDiagrama == 1:
                    stNombreNodo = 'UTILIDAD_1"'
                
                arhcPGMX.write('              <UtilityVariable name="' + stNombreNodo + ' />\n')
                arhcPGMX.write('              <Values>0.0</Values>\n')
                arhcPGMX.write('            </Potential>\n')
                arhcPGMX.write('          </Branch>\n')
                arhcPGMX.write('        </Branches>\n')
                arhcPGMX.write('      </Potential>\n')
            if idTipoDiagrama == 1:
                #7.1.- Diagrama de Influencia
                arhcPGMX.write('      <Potential type="Table" role="utility">\n')
                arhcPGMX.write('        <UtilityVariable name="UTILIDAD_1" />\n')
                arhcPGMX.write('        <Variables>\n')
                vecMax = []
                vecCon = []
                vVariableVal = []
                for row in vecVariables_Utilidad:
                    arhcPGMX.write('          <Variable name="' + row + '_1" />\n')
                    for auxNodo in vecNodo:
                        if row == auxNodo[0]:
                            vVariableVal.append(auxNodo[1])
                            vecMax.append(len(auxNodo[1])-1)
                            vecCon.append(0)
                
                vecUtilidad = []
                banAvanzar = True
                while True:
                    vecCampos = []
                    for i in range(0,len(vecCon)):
                        vecCampos.append(vVariableVal[i][vecCon[i]])
                    vecUtilidad.append(vecCampos)
                    
                    auxCol = 0 #(len(vecCon)-1) 
                    vecCon[auxCol] = vecCon[auxCol] + 1
                    for i in range(0,len(vecCon)):
                        auxCol = i   #(len(vecCon)-1) - i
                        if vecCon[auxCol] > vecMax[auxCol]:
                            vecCon[auxCol] = 0
                            auxCol = auxCol + 1
                            if auxCol < len(vecCon):
                                vecCon[auxCol] = vecCon[auxCol] + 1
                            else:
                                banAvanzar = False
                    if banAvanzar == False:
                        break
            
                stUtilidad = ""
                for vUtilidad in vecUtilidad:
                    stSQL = "SELECT "
                    stSQL = stSQL + " IFNULL((-1) * AVG(IFNULL((SBA_IMPORTE * 0.15 + SEPARACION) * ES_SEPARACION,0))  "
                    stSQL = stSQL + " + (-1) * AVG(IFNULL((SBA_IMPORTE * 0.15 + ABANDONO) * ES_ABANDONO,0)),0) "
                    stSQL = stSQL + " FROM " + stDB + ".MID_DATOS "
                    stSQL = stSQL + " WHERE ANNO >= 2017 "
                    stSQL = stSQL + " AND MUESTRA <> " + str(idParticion) + " "
                    stMismoCampoEstado_0 = ""
                    for i in range(0,len(vUtilidad)):
                        stSQL = stSQL + " AND " + vecVariables_Utilidad[i] + "='" + str(vUtilidad[i]) + "'"
          
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall() 
                    for resSQL in regAux:
                        stUtilidad  = stUtilidad  + ' ' + str(resSQL[0])
                
                arhcPGMX.write('        </Variables>\n')
                arhcPGMX.write('       <Values>' + stUtilidad + '</Values>\n')
                arhcPGMX.write('      </Potential>\n')
            
            
            #8.- Cálculo de los costos
            print('15.-- Costos de Decisión ' + time.strftime("%H:%M:%S"))
            for i in range(0,len(vecDecision)): 
                arhcPGMX.write('      <Potential type="Table" role="utility">\n')
                
                stNombreNodo = "COSTO_" + vecDecision[i] + '" timeSlice="0"'
                if idTipoDiagrama == 1:
                    stNombreNodo = "COSTO_" + vecDecision[i] + '_0"'
                
                arhcPGMX.write('        <UtilityVariable name="' + stNombreNodo + ' />\n')
                arhcPGMX.write('        <Variables>\n')
                stNombreNodo = vecDecision[i] + '" timeSlice="0"'
                if idTipoDiagrama == 1:
                    stNombreNodo = vecDecision[i] + '_0"'
                arhcPGMX.write('          <Variable name="' + stNombreNodo + ' />\n')
                arhcPGMX.write('        </Variables>\n')
                
                for auxNodo in vecNodo:
                    if vecDecision[i] == auxNodo[0]:
                        vClaseVal = auxNodo[1]
                stCostos = ""
                if i == 0:
                    stSQL = "SELECT IFNULL(AVG(B.SBA_IMPORTE - A.SBA_IMPORTE),0) "
                if i == 1:
                    stSQL = "SELECT IFNULL(AVG(B.NUM_CURSOS * B.PRE_FORMACION/ 3 - A.NUM_CURSOS * A.PRE_FORMACION/3),0) "
                stSQL = stSQL  + " FROM " + stDB + ".MID_DATOS A, " + stDB + ".MID_DATOS B "
                stSQL = stSQL  + " WHERE A.ID_EMPLEADO = B.ID_EMPLEADO "
                stSQL = stSQL  + " AND A.ANNO + 1 = B.ANNO "
                stSQL = stSQL  + " AND A.MUESTRA <> " + str(idParticion) + " "
                stSQL = stSQL  + " AND A." + vecDecision[i] + " <> 'S/D' "
                stSQL = stSQL  + " AND A." + vecDecision[i] + " <> B." + vecDecision[i] + " "
                stSQL = stSQL  + " AND A." + vecDecision[i] + "='"
                for row in vClaseVal:
                    myCursor.execute(stSQL + row + "'")
                    regAux = myCursor.fetchall() 
                    for resSQL in regAux:
                        stCostos = str(resSQL[0]) + ' ' + stCostos
                arhcPGMX.write('        <Values>'+ stCostos + '</Values>\n')
                
                #Aqui va los datos numéricos
                arhcPGMX.write('      </Potential>\n')
                
            arhcPGMX.write('    </Potentials>\n')
            
            arhcPGMX.write('  </ProbNet>\n')
            
            #9.- Opciones de Infirencia
            arhcPGMX.write('  <InferenceOptions>\n')
            arhcPGMX.write('    <MulticriteriaOptions>\n')
            arhcPGMX.write('      <SelectedAnalysisType>UNICRITERION</SelectedAnalysisType>\n')
            arhcPGMX.write('      <Unicriterion>\n')
            arhcPGMX.write('        <Scales>\n')
            arhcPGMX.write('          <Scale Criterion="---" Value="1.0" />\n')
            arhcPGMX.write('        </Scales>\n')
            arhcPGMX.write('      </Unicriterion>\n')
            arhcPGMX.write('      <CostEffectiveness>\n')
            arhcPGMX.write('        <Scales>\n')
            arhcPGMX.write('          <Scale Criterion="---" Value="1.0" />\n')
            arhcPGMX.write('        </Scales>\n')
            arhcPGMX.write('        <CE_Criteria>\n')
            arhcPGMX.write('          <CE_Criterion Criterion="---" Value="Cost" />\n')
            arhcPGMX.write('        </CE_Criteria>\n')
            arhcPGMX.write('      </CostEffectiveness>\n')
            arhcPGMX.write('    </MulticriteriaOptions>\n')
            if idTipoDiagrama == 0:
                arhcPGMX.write('    <TemporalOptions>\n')
                arhcPGMX.write('      <Slices>20</Slices>\n')
                arhcPGMX.write('      <Transition>BEGINNING</Transition>\n')
                arhcPGMX.write('      <DiscountRates>\n')
                arhcPGMX.write('        <DiscountRate Criterion="---" value="0.0" unit="YEAR" />\n')
                arhcPGMX.write('      </DiscountRates>\n')
                arhcPGMX.write('    </TemporalOptions>\n')
            arhcPGMX.write('  </InferenceOptions>\n')
            
            #10.- Políticas
            print('16.-- Potencias/Políticas Decisión ' + time.strftime("%H:%M:%S"))
            if banDecision == True:
                arhcPGMX.write('  <Policies>\n')
                for row in vecLinksDecision:
                    stVariables = row[0] + '_0'
                    
                    stTimeSlice = ' timeSlice="0"'
                    arhcPGMX.write('    <Potential type="Table" role="joinProbability">\n')
                    arhcPGMX.write('      <Variables>\n')
                    
                    stNombreNodo = row[0] + '" timeSlice="0"'
                    if idTipoDiagrama == 1:
                        stNombreNodo = row[0] + '_0"'
                    
                    arhcPGMX.write('        <Variable name="' + stNombreNodo + ' />\n')
                    vClaseVal = []
                    for auxNodo in vecNodo:
                        if row[0] == auxNodo[0]:
                            vClaseVal = auxNodo[1]
                    
                    vecMax = []
                    vecCon = []
                    vVariableVal = []
                    for i in range(0,len(row[1])):
                        
                        auxStVar = row[1][i] + '_0'
                        stTimeSlice = ' timeSlice="0"'
                        for j in range(0,len(vecVariables_Fijas)):
                            if row[1][i] == vecVariables_Fijas[j]:
                                stTimeSlice = ''
                                auxStVar = row[1][i]
                                
                        stNombreNodo = row[1][i] + '" ' + stTimeSlice
                        if idTipoDiagrama == 1 and stTimeSlice == ' timeSlice="0"':
                            stNombreNodo = row[1][i] + '_0"'
							
                        stVariables =  stVariables + "," + auxStVar

                        arhcPGMX.write('        <Variable name="' + stNombreNodo + ' />\n')
                        
                        #Prepara las variable auxiliares para el cálculo de las probabilidades
                        for auxNodo in vecNodo:
                            if row[1][i] == auxNodo[0]:
                                vVariableVal.append(auxNodo[1])
                                vecMax.append(len(auxNodo[1])-1)
                                vecCon.append(0)
                                
                    arhcPGMX.write('      </Variables>\n')
                    
                    
                    #Prepara el vector para las ID. de Probabilidades
                    stProb = Busca_Probabilidad(myConexion, stDB, idParticion, stVariables, stExperimento)
                    if stProb == "N/E":
                        Inicio = dt.datetime.today()
                        stProb = ""
                        vecProb = []
                        banAvanzar = True
                        while True:
                            vecCampos = []
                            for i in range(0,len(vecCon)):
                                vecCampos.append(vVariableVal[i][vecCon[i]])
                            vecProb.append(vecCampos)
                            
                            auxCol = 0 #(len(vecCon)-1) 
                            vecCon[auxCol] = vecCon[auxCol] + 1
                            for i in range(0,len(vecCon)):
                                auxCol = i   #(len(vecCon)-1) - i
                                if vecCon[auxCol] > vecMax[auxCol]:
                                    vecCon[auxCol] = 0
                                    auxCol = auxCol + 1
                                    if auxCol < len(vecCon):
                                        vecCon[auxCol] = vecCon[auxCol] + 1
                                    else:
                                        banAvanzar = False
                            if banAvanzar == False:
                                break
                        
                        
                        for vProb in vecProb:
                            stSQL = "SELECT  Count(*)"
                            stSQL = stSQL + "FROM " + stDB + ".MID_DATOS A "
                            stSQL = stSQL + " WHERE 1 = 1 "
                            stSQL = stSQL + " AND A.MUESTRA <> " + str(idParticion) + " "
                            for i in range(0,len(vProb)):
                                stCampo = "A." + row[1][i]
                                stSQL = stSQL + " AND " + stCampo + "='" + str(vProb[i]) + "'"
                             
                            vecProbRes = []
                            varTotal = 0
                            for i in range(0,len(vClaseVal)): 

                                myCursor.execute(stSQL + " AND A." + row[0] +"='" + str(vClaseVal[i]) + "'")
                                regAux = myCursor.fetchall()
                                
                                for resSQL in regAux:
                                    vecProbRes.append(resSQL[0])
                                    varTotal =  varTotal + resSQL[0]
                            
                            
                                           
                            #Correción de Laplace
                            if varTotal + vCorrecionLaplace * len(vClaseVal) > 0 :
                                for i in range(0,len(vecProbRes)):
                                    vecProbRes[i] = (vecProbRes[i] + vCorrecionLaplace ) / (varTotal + vCorrecionLaplace * len(vClaseVal))
                                            

                                    
                            for i in range(0,len(vecProbRes)):
                                stProb = stProb + ' ' + str(vecProbRes[i]) 

                        Fin = dt.datetime.today()
                        Tiempo = Dif_Tiempo(Inicio,Fin)
                        Guarda_Probabilidad(myConexion, stDB, idParticion, stVariables, stProb, stExperimento, Inicio, Fin, Tiempo)

                    arhcPGMX.write('      <Values>' + stProb + '</Values>\n')
                    arhcPGMX.write('    </Potential>\n')
                    
                arhcPGMX.write('  </Policies>\n')
            
            #99.- Cierra Red
            arhcPGMX.write('</ProbModelXML>\n')
            arhcPGMX.close()

    except Error as e:
        print("Error reading data from MySQL table", e)  
        myConexion.close()
        varResultado = -1        
                
    finally:
        pass
    
    #99.- Cierra conexiones
    myConexion.close()
    print('99.- Salgo '  + time.strftime("%H:%M:%S"))
    return varResultado
                
            