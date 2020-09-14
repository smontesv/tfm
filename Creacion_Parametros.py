# -*- coding: utf-8 -*-
"""
UNIVERSIDAD NACIONAL DE EDUCACION A DISTANCIA
Master: Inteligencia Artificial Avanzada 
Materia: Trabajo de Fin de Master 
Módulo: Creación de Parámetros para los Modelos
@author: Sergio Montes Vázquez
Noviembre 2019
"""

#0.- Librerias e iniciacHostión de variables 
import pymysql.cursors
from mysql.connector import Error

def Crea_Parametros(stHost, stUser, stPassword, stDB, numParticiones):
    varResultado = 1
    myConexion = pymysql.connect(host = stHost, user = stUser, password = stPassword, db = stDB)
    
    print ('-> 0.- Inicia Creación de Parámetros ')
    try:
        with myConexion.cursor() as myCursor:
            print ('--> 1.- Entra en Cursor MySql')

            stVer = "01"
            vCorte_GainR = 0.75
            vCorrecionLaplace = 0.5
            
            for NumMaxCampos in range(4,11):
                print ('--> 2.- Número de Campos: ' + str(NumMaxCampos))
                
                for banIncluye_Estado_1 in (False,True):
                    print ('---> 3.- Incluye estado 1: ' + str(banIncluye_Estado_1))
                    
                    for banDecision  in (False,True):
                        print ('----> 4.- Decisión: ' + str(banDecision))
                    
                        for idTipoDiagrama in (0,1):
                            print ('-----> 5.- Tipo de Diagrama: ' + str(idTipoDiagrama))
                            
                            for idParticion in range(0,numParticiones):
                                print ('------> 6.- Id. Partición: ' + str(idParticion))
                            
                                stSQL = "INSERT INTO " + stDB + ".TB_MODELO_PARM ("
                                stSQL = stSQL + "stVer, NumMaxCampos, banIncluye_Estado_1, banDecision, "
                                stSQL = stSQL + "vCorte_GainR, vCorrecionLaplace, idTipoDiagrama, idParticion, "
                                stSQL = stSQL + "Seleccionado, Ejecutado) "
                                stSQL = stSQL + "VALUES('" + stVer + "'," + str(NumMaxCampos) + ","
                                stSQL = stSQL + str(banIncluye_Estado_1) + "," +  str(banDecision) + ", "
                                stSQL = stSQL + str(vCorte_GainR) + ", " + str(vCorrecionLaplace) + ", " 
                                stSQL = stSQL + str(idTipoDiagrama) + ", " + str(idParticion) + ", "
                                stSQL = stSQL + "False,False) "
                                
                                myCursor.execute(stSQL)
                                myConexion.commit()
            
    except Error as e:
        print("Error reading data from MySQL table", e)  
        myConexion.close()
        varResultado = -1        
                
    finally:
        pass
    
    #99.- Cierra conexiones
    myConexion.close()
    print("-> 99.- Fin Creación de Parámetros")
    return varResultado