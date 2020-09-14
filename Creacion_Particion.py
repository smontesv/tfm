# -*- coding: utf-8 -*-
"""
UNIVERSIDAD NACIONAL DE EDUCACION A DISTANCIA
Master: Inteligencia Artificial Avanzada 
Materia: Trabajo de Fin de Master 
Módulo: Partición del DataSet
@author: Sergio Montes Vázquez
Noviembre 2019
"""

#0.- Librerias e iniciacHostión de variables 
import random
import pymysql.cursors
from mysql.connector import Error


def Crea_Particion(stHost, stUser, stPassword, stDB, numParticiones):
    
    varResultado = 1
    myConexion = pymysql.connect(host = stHost, user = stUser, password = stPassword, db = stDB)
    
    print ('-> 0.- Inicia particiones ')
    try:
        with myConexion.cursor() as myCursor:
            print ('--> 1.- Entra en Cursor MySql')
            
            #1.- limpia el campo de la Muestra la tabla de datos
            print ('--> 2.- Limpia el campo de la Muestra ')
            stSQL = "UPDATE " + stDB + ".MID_DATOS "
            stSQL = stSQL + " SET MUESTRA = NULL "
            stSQL = stSQL + " WHERE MUESTRA IS NOT NULL "
            myCursor.execute(stSQL)
            myConexion.commit()
            
            idParticion = 0
            banAvanzar = True
            
            #2.- Bucle de particiones hasta que se hayan cubierto todos los registros
            while banAvanzar == True:
            
                stSQL = "SELECT  COUNT(*) "
                stSQL = stSQL + " FROM " + stDB + ".MID_DATOS "
                stSQL = stSQL + " WHERE MUESTRA IS NULL" 
                myCursor.execute(stSQL)
                regAux = myCursor.fetchall()
                numReg = regAux[0][0]
                if numReg > 0:
                    idRegMarca = random.randrange(numReg)
                    
                    stSQL = " SELECT ID_EMPLEADO, ANNO "
                    stSQL = stSQL + " FROM " + stDB + ".MID_DATOS "
                    stSQL = stSQL + " WHERE MUESTRA IS NULL" 
                    stSQL = stSQL + " ORDER BY ID_EMPLEADO, ANNO "
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                    
                    stEmpleado = ""
                    if type(regAux[idRegMarca][0]) == "str":
                        stEmpleado = regAux[idRegMarca][0]
                    else:
                        stEmpleado = str(regAux[idRegMarca][0])
                    
                    
                    stSQL = "UPDATE " + stDB + ".MID_DATOS "
                    stSQL = stSQL + " SET MUESTRA = " + str(idParticion)
                    stSQL = stSQL + " WHERE ID_EMPLEADO  = '" + stEmpleado + "'"
                    stSQL = stSQL + " AND ANNO  = " + str(regAux[idRegMarca][1])
                    myCursor.execute(stSQL)
                    myConexion.commit()
                    idParticion = idParticion + 1
                    
                    if idParticion >= numParticiones:
                        idParticion = 0

                else:
                    banAvanzar = False
                
            stSQL = "SELECT  MUESTRA, count(*) "
            stSQL = stSQL + " FROM " + stDB + ".MID_DATOS "
            stSQL = stSQL + " GROUP BY MUESTRA " 
            stSQL = stSQL + " ORDER BY MUESTRA " 
            myCursor.execute(stSQL)
            regAux = myCursor.fetchall()
            for i in range (0,len(regAux)):
                print(str(i) + '.- ' + str(regAux[i][0]) + ', ' + str(regAux[i][1]) )
            
            
    except Error as e:
        print("Error reading data from MySQL table", e)  
        myConexion.close()
        varResultado = -1        
                
    finally:
        pass
    
    #99.- Cierra conexiones
    myConexion.close()
    print("-> 99.- Fin Particiones")
    return varResultado