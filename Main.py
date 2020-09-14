# -*- coding: utf-8 -*-
"""
UNIVERSIDAD NACIONAL DE EDUCACION A DISTANCIA
Master: Inteligencia Artificial Avanzada 
Materia: Trabajo de Fin de Master 
Módulo: Main 
@author: Sergio Montes Vázquez
Noviembre 2019
"""

#Librerias
import datetime as dt
import time
import pymysql.cursors
from mysql.connector import Error
from Creacion_DataSet import Crea_DataSet
from Creacion_Particion import Crea_Particion
from Creacion_Parametros import Crea_Parametros
from Creacion_Modelo import Crea_Modelo

#0.- Subrutinas
#0.0.- Diferencia entre fechas
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

#0.1.- Creación de Registro Proceso
def Crea_Proceso(stHost, stUser, stPassword, stDB, stProceso, idPaso, numExperimento,stPaso, stExperimento, vInicio):

    myConexion = pymysql.connect(host = stHost, user = stUser, password = stPassword, db = stDB)
    
    try:
        with myConexion.cursor() as myCursor:
            stSQL = "INSERT INTO " + stDB + ".TB_REG_PROCESO "
            stSQL = stSQL + " (PROCESO, ID_PASO, NUM_EXPERIMENTO, DES_PASO, DES_EXPERIMENTO, INICIO) "
            stSQL = stSQL + " VALUES('" + stProceso + "'," + str(idPaso) + "," + str(numExperimento) 
            stSQL = stSQL + ",'" + stPaso + "','" + stExperimento + "','" + str(vInicio) + "')"
            myCursor.execute(stSQL)
            myConexion.commit()
            
            stSQL = "SELECT * FROM " + stDB + ".TB_REG_PROCESO "
            myCursor.execute(stSQL)
            regAux = myCursor.fetchall()
            
    except Error as e:
        print("Error reading data from MySQL table", e)  
        myConexion.close()      
                
    finally:
        pass
    
    myConexion.close()
    
#0.2.- Actualización de Registro Proceso
def Actualiza_Proceso(stHost, stUser, stPassword, stDB, stProceso, idPaso, numExperimento, vFin, vTiempo):

    myConexion = pymysql.connect(host = stHost, user = stUser, password = stPassword, db = stDB)
    
    try:
        with myConexion.cursor() as myCursor:
            stSQL = "UPDATE " + stDB + ".TB_REG_PROCESO "
            stSQL = stSQL + " SET FIN = '" + str(vFin) + "',"
            stSQL = stSQL + " TIEMPO = '" + vTiempo + "'"
            stSQL = stSQL + " WHERE PROCESO = '" + stProceso + "'"
            stSQL = stSQL + " AND ID_PASO = " + str(idPaso)
            stSQL = stSQL + " AND NUM_EXPERIMENTO = " + str(numExperimento)

            myCursor.execute(stSQL)
            myConexion.commit()
            
            stSQL = "SELECT * FROM " + stDB + ".TB_REG_PROCESO "
            myCursor.execute(stSQL)
            regAux = myCursor.fetchall()
            
    except Error as e:
        print("Error reading data from MySQL table", e)  
        myConexion.close()      
                
    finally:
        pass
    
    myConexion.close()

#1.- Declaración de Variables Generales
#1.1.- Conexión MySQL
stHost =  'localhost'
stUser = 'root'
stPassword = 'Bokwut_08'
stDB = "tfm_smv"

#1.2.- Indicadores de Ejecuciones y Variables Generales
parParticio = 0
banCreaDataset = 0
banCreaParticion = 0
banCreaParametros = 0
banCreaModelo = 0
banCreaModeloA = 1
banCreaModeloB = 1
tamParticion = 10

stRuta = "C:\\Users\\sergiomontes\\Documents\\00_TFM\\2020\\Modelo\\"


stProceso = 'CM_' + time.strftime("%y%m%d") + '_' + time.strftime("%H%M%S")
idPaso = 0
stPaso = ''
idExperimento = 0
stExperimento = ''
Inicio = dt.datetime

print ('0-> Inicia Proeso ' + stProceso)


#3.- Creación del DataSet
if banCreaDataset == 1:
    print ('1-> Entra en Crear DataSet')
    idPaso = 1
    stPaso = 'Crea DataSet'
    idExperimento = 0
    stExperimento = 'S/E'
    Inicio = dt.datetime.today()
    Crea_Proceso(stHost, stUser, stPassword, stDB, stProceso, idPaso, idExperimento,stPaso, stExperimento, Inicio)

    Crea_DataSet(stHost, stUser, stPassword, stDB)
    
    Fin = dt.datetime.today()
    Tiempo = Dif_Tiempo(Inicio,Fin)
    Actualiza_Proceso(stHost, stUser, stPassword, stDB, stProceso, idPaso, idExperimento, Fin, Tiempo)

    
#4.- Creación de las Particiones
if banCreaParticion == 1:
    print ('2-> Entra en Crear Particiones')
    idPaso = 2
    stPaso = 'Crea Particiones'
    idExperimento = 0
    stExperimento = 'S/E'
    Inicio = dt.datetime.today()
    Crea_Proceso(stHost, stUser, stPassword, stDB, stProceso, idPaso, idExperimento,stPaso, stExperimento, Inicio)

    Crea_Particion(stHost, stUser, stPassword, stDB, tamParticion)
    
    Fin = dt.datetime.today()
    Tiempo = Dif_Tiempo(Inicio,Fin)
    Actualiza_Proceso(stHost, stUser, stPassword, stDB, stProceso, idPaso, idExperimento, Fin, Tiempo)

#5.- Creación de Parámetros de los Modelos 
if banCreaParametros == 1:
    print ('3-> Entra en Crear Parámetros')
    idPaso = 3
    stPaso = 'Crea Parámetros'
    idExperimento = 0
    stExperimento = 'S/E'
    Inicio = dt.datetime.today()
    Crea_Proceso(stHost, stUser, stPassword, stDB, stProceso, idPaso, idExperimento,stPaso, stExperimento, Inicio)

    Crea_Parametros(stHost, stUser, stPassword, stDB, tamParticion)
    
    Fin = dt.datetime.today()
    Tiempo = Dif_Tiempo(Inicio,Fin)
    Actualiza_Proceso(stHost, stUser, stPassword, stDB, stProceso, idPaso, idExperimento, Fin, Tiempo)

#6.- Creacion y Prueba de Modelo
if banCreaModelo == 1:
    myConexion = pymysql.connect(host = stHost, user = stUser, password = stPassword, db = stDB)
    
    print ('4-> Entra en Crear/Probar Modelo')
    try:
        with myConexion.cursor() as myCursor:
            print ('4.0--> Limpia Experimentos')
            stSQL = "DELETE FROM " + stDB + ".TB_REG_PROCESO WHERE (TIEMPO IS NULL OR TIEMPO ='0:0:0')"
            if (parParticio > -1):
                stSQL = stSQL + " AND DES_EXPERIMENTO LIKE '%Partición: " + str(parParticio) + "%'"
            myCursor.execute(stSQL)
            
            stSQL = "UPDATE " + stDB + ".TB_MODELO_PARM SET Seleccionado = FALSE WHERE (Parametros = FALSE OR Parametros IS NULL )"
            if (parParticio > -1):
                stSQL = stSQL + " AND idParticion = " + str(parParticio)
            myCursor.execute(stSQL)
            
            print ('4.1--> Abre la tabla de Parámetros')
            idExperimento = 0
            stSQL = "SELECT * FROM " + stDB + ".TB_MODELO_PARM WHERE Seleccionado = False AND NumMaxCampos <= 8"
            if (parParticio > -1):
                 stSQL = stSQL + " AND idParticion = " + str(parParticio)
            myCursor.execute(stSQL)
            regAux = myCursor.fetchall()
            for i in range(0,len(regAux)):
                print(i,regAux[i])
                
                #6.0.- Se Prepara los datos del experimento
                varParametro = regAux[i] 
                
                #6.1.- Se hace una comporbación de que no se ha seleccionado este registro en otro proceso
                stSQL = "SELECT Count(*) FROM " + stDB + ".TB_MODELO_PARM "
                stSQL = stSQL + " WHERE stVer = '" + varParametro[0] + "'" 
                stSQL = stSQL + " AND NumMaxCampos =" + str(varParametro[1]) 
                stSQL = stSQL + " AND banIncluye_Estado_1 =" + str(varParametro[2])
                stSQL = stSQL + " AND banDecision =" + str(varParametro[3])
                stSQL = stSQL + " AND vCorte_GainR =" + str(varParametro[4])
                stSQL = stSQL + " AND vCorrecionLaplace =" + str(varParametro[5])
                stSQL = stSQL + " AND idTipoDiagrama =" + str(varParametro[6])
                stSQL = stSQL + " AND idParticion =" + str(varParametro[7])
                stSQL = stSQL + " AND Seleccionado = FALSE"
                myCursor.execute(stSQL)
                regCtl = myCursor.fetchall()
                
                if regCtl[0][0] == 1:
                
                    #6.2.- Se bloque el registro de prámetros
                    stSQL = "UPDATE " + stDB + ".TB_MODELO_PARM "
                    stSQL = stSQL + " SET Seleccionado = TRUE"
                    stSQL = stSQL + " WHERE stVer = '" + varParametro[0] + "'" 
                    stSQL = stSQL + " AND NumMaxCampos =" + str(varParametro[1]) 
                    stSQL = stSQL + " AND banIncluye_Estado_1 =" + str(varParametro[2])
                    stSQL = stSQL + " AND banDecision =" + str(varParametro[3])
                    stSQL = stSQL + " AND vCorte_GainR =" + str(varParametro[4])
                    stSQL = stSQL + " AND vCorrecionLaplace =" + str(varParametro[5])
                    stSQL = stSQL + " AND idTipoDiagrama =" + str(varParametro[6])
                    stSQL = stSQL + " AND idParticion =" + str(varParametro[7])
                    res1 = myCursor.execute(stSQL)
                    myConexion.commit()
                
                    #6.3.- Se crea el modelo
                    stExperimento = "Ver:" + varParametro[0] + " Nodos:" + str(varParametro[1]) 
                    stExperimento = stExperimento + " Estado 1: " + str(varParametro[2])
                    stExperimento = stExperimento + " Decisión: " + str(varParametro[3])
                    stExperimento = stExperimento + " Corte GainR: " + str(varParametro[4])
                    stExperimento = stExperimento + " Correlación Laplace: " + str(varParametro[5])
                    stExperimento = stExperimento + " Tipo Diagrama: " + str(varParametro[6])
                    stExperimento = stExperimento + " Partición: " + str(varParametro[7])
                    
                    print ('4.2---> Crea Modelo ' + stExperimento)
                    idPaso = 4
                    stPaso = 'Crea Modelo'
                    Inicio = dt.datetime.today()
                    Crea_Proceso(stHost, stUser, stPassword, stDB, stProceso, idPaso, idExperimento,stPaso, stExperimento, Inicio)
                
                    Crea_Modelo(stHost, stUser, stPassword, stDB, stRuta, varParametro, stExperimento)
                    
                    Fin = dt.datetime.today()
                    Tiempo = Dif_Tiempo(Inicio,Fin)
                    Actualiza_Proceso(stHost, stUser, stPassword, stDB, stProceso, idPaso, idExperimento, Fin, Tiempo)

                    #6.4.- Se prueba el modelo
                    
                    #6.5.- Se marca el modelo como ejecutado
                    stSQL = "UPDATE " + stDB + ".TB_MODELO_PARM "
                    stSQL = stSQL + " SET Parametros = TRUE"
                    stSQL = stSQL + " WHERE stVer = '" + varParametro[0] + "'" 
                    stSQL = stSQL + " AND NumMaxCampos =" + str(varParametro[1]) 
                    stSQL = stSQL + " AND banIncluye_Estado_1 =" + str(varParametro[2])
                    stSQL = stSQL + " AND banDecision =" + str(varParametro[3])
                    stSQL = stSQL + " AND vCorte_GainR =" + str(varParametro[4])
                    stSQL = stSQL + " AND vCorrecionLaplace =" + str(varParametro[5])
                    stSQL = stSQL + " AND idTipoDiagrama =" + str(varParametro[6])
                    stSQL = stSQL + " AND idParticion =" + str(varParametro[7])
                    myCursor.execute(stSQL)
                    myConexion.commit()

                idExperimento = idExperimento + 1 
    except Error as e:
        print("Error reading data from MySQL table", e)  
        myConexion.close()
        varResultado = -1        
                
    finally:
        pass
    
    #99.- Cierra conexiones
    myConexion.close()
    
if banCreaModeloA == 1:
    print ('5-> Entra en Crear Modelo Final A')
    
    stVer= '01'
    NumMaxCampos = 5
    banIncluye_Estado_1 = 1
    banDecision = 1 
    vCorte_GainR = 0.7500
    vCorrecionLaplace = 0.5000
    idTipoDiagrama = 0
    idParticion = -1
    
    varParametro = [stVer, NumMaxCampos, banIncluye_Estado_1,  banDecision, vCorte_GainR, vCorrecionLaplace, idTipoDiagrama, idParticion]
    
    stExperimento = "Ver:" + varParametro[0] + " Nodos:" + str(varParametro[1]) 
    stExperimento = stExperimento + " Estado 1: " + str(varParametro[2])
    stExperimento = stExperimento + " Decisión: " + str(varParametro[3])
    stExperimento = stExperimento + " Corte GainR: " + str(varParametro[4])
    stExperimento = stExperimento + " Correlación Laplace: " + str(varParametro[5])
    stExperimento = stExperimento + " Tipo Diagrama: " + str(varParametro[6])
    stExperimento = stExperimento + " Partición: " + str(varParametro[7])
    
    Crea_Modelo(stHost, stUser, stPassword, stDB, stRuta, varParametro, stExperimento)

if banCreaModeloB == 1:
    print ('6-> Entra en Crear Modelo Final B')
    
    stVer= '01'
    NumMaxCampos = 4
    banIncluye_Estado_1 = 0
    banDecision = 1 
    vCorte_GainR = 0.7500
    vCorrecionLaplace = 0.5000
    idTipoDiagrama = 0
    idParticion = -1
    
    varParametro = [stVer, NumMaxCampos, banIncluye_Estado_1,  banDecision, vCorte_GainR, vCorrecionLaplace, idTipoDiagrama, idParticion]
    
    stExperimento = "Ver:" + varParametro[0] + " Nodos:" + str(varParametro[1]) 
    stExperimento = stExperimento + " Estado 1: " + str(varParametro[2])
    stExperimento = stExperimento + " Decisión: " + str(varParametro[3])
    stExperimento = stExperimento + " Corte GainR: " + str(varParametro[4])
    stExperimento = stExperimento + " Correlación Laplace: " + str(varParametro[5])
    stExperimento = stExperimento + " Tipo Diagrama: " + str(varParametro[6])
    stExperimento = stExperimento + " Partición: " + str(varParametro[7])
    
    Crea_Modelo(stHost, stUser, stPassword, stDB, stRuta, varParametro, stExperimento)



print ('99-> Termina Proeso ' + stProceso)
