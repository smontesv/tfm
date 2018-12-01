# -*- coding: utf-8 -*-
"""
UNIVERSIDAD NACIONAL DE EDUCACIÓN A DISTANCIA

Master Inteligencia Artificial Avanzada

Trabajo de Fin de Master
Visualización y complitud de Históricos

@author: Sergio Montes Vázquez
"""

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import pymysql.cursors
from datetime import date

#1.- Graficador de Históricos
def graficaHistorico(pHost, pUser, pPassword, pDB, pID_PRINCIPAL, pORD_PRINCIPAL):
     #1.0.- Inicialización de parámetro globales y conexión a base de datos
    Conexion = pymysql.connect(host = pHost, user = pUser, password = pPassword, db = pDB)
    fig = plt.figure()
    plt.plot(0, 50)
    plt.plot(50, 50)
    minX = 5
    maxX = 45
    
    try:
        with Conexion.cursor() as Cursor:
            #1.1.- Histórico Principal A
            stAux = "HIST_PRINCIPAL_A: ID_PRINCIPAL = " + str(pID_PRINCIPAL) + " ; ORD_PRINCIPAL = " + str(pORD_PRINCIPAL)
            plt.text(2,49, stAux , fontsize=8)
            stSQL = "SELECT FECHA_INICIO, FECHA_FIN FROM " + pDB + ".HIST_PRINCIPAL_A "
            stSQL = stSQL + "WHERE  ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s "
            Cursor.execute(stSQL,(str(pID_PRINCIPAL),str(pORD_PRINCIPAL)))
            auxHISTORICO  = Cursor.fetchall()
            plt.plot((minX,minX), (0,45), 'k--', linewidth = 1, label = 'y1')
            plt.plot((maxX,maxX), (0,45), 'k--', linewidth = 1, label = 'y1')
            plt.plot((minX,maxX), (45,45), 'b->',linewidth = 2)
            #ax.plot(x, y, 'ro')
            plt.text(minX-2,46, auxHISTORICO [0][0].strftime('%Y-%m-%d') , fontsize=7)
            plt.text(maxX-2,46, auxHISTORICO [0][1].strftime('%Y-%m-%d') , fontsize=7)
            limInicio = auxHISTORICO [0][0]
            limFin = auxHISTORICO [0][1]
            
            #1.2.- Histórico Secundario HIST_A1
            auxY = 42
            plt.text(-2,auxY, "HIST_A1" , fontsize=8)
            stSQL = "SELECT FECHA_INICIO, FECHA_FIN FROM " + pDB + ".HIST_A1 "
            stSQL = stSQL + "WHERE  ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s "
            stSQL = stSQL + "ORDER BY FECHA_INICIO"
            Cursor.execute(stSQL,(str(pID_PRINCIPAL),str(pORD_PRINCIPAL)))
            auxHISTORICO  = Cursor.fetchall()
            for i in range(0,len(auxHISTORICO)):
                auxXini = 5 + 40*(auxHISTORICO [i][0] - limInicio) / (limFin - limInicio)
                auxXfin = 5 + 40*(auxHISTORICO [i][1] - limInicio) / (limFin - limInicio)
                auxY = auxY - 4
                plt.text(auxXini-2,auxY+1, auxHISTORICO [i][0].strftime('%Y-%m-%d') , fontsize=7)
                plt.text(auxXfin-2,auxY+1, auxHISTORICO [i][1].strftime('%Y-%m-%d') , fontsize=7)
                stLinea = 'g->'
                if auxXini>auxXfin:
                    stLinea = 'r<-'
                plt.plot((auxXini,auxXfin), (auxY,auxY), stLinea,linewidth = 2)

            #1.3.- Histórico Secundario HIST_A2
            auxY = auxY - 4
            plt.text(-2,auxY, "HIST_A2" , fontsize=8)
            stSQL = "SELECT FECHA_INICIO, FECHA_FIN FROM " + pDB + ".HIST_A2 "
            stSQL = stSQL + "WHERE  ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s "
            stSQL = stSQL + "ORDER BY FECHA_INICIO"
            Cursor.execute(stSQL,(str(pID_PRINCIPAL),str(pORD_PRINCIPAL)))
            auxHISTORICO  = Cursor.fetchall()
            for i in range(0,len(auxHISTORICO)):
                auxXini = 5 + 40*(auxHISTORICO [i][0] - limInicio) / (limFin - limInicio)
                auxXfin = 5 + 40*(auxHISTORICO [i][1] - limInicio) / (limFin - limInicio)
                auxY = auxY - 4
                plt.text(auxXini-2,auxY+1, auxHISTORICO [i][0].strftime('%Y-%m-%d') , fontsize=7)
                plt.text(auxXfin-2,auxY+1, auxHISTORICO [i][1].strftime('%Y-%m-%d') , fontsize=7)
                stLinea = 'm->'
                if auxXini>auxXfin:
                    stLinea = 'r<-'
                plt.plot((auxXini,auxXfin), (auxY,auxY), stLinea,linewidth = 2)

            #1.4.- Histórico Secundario HIST_A_B
            auxY = auxY - 4
            plt.text(-2,auxY, "HIST_A_B" , fontsize=8)
            stSQL = "SELECT FECHA_INICIO, FECHA_FIN FROM " + pDB + ".HIST_A_B "
            stSQL = stSQL + "WHERE  ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s "
            stSQL = stSQL + "ORDER BY FECHA_INICIO"
            Cursor.execute(stSQL,(str(pID_PRINCIPAL),str(pORD_PRINCIPAL)))
            auxHISTORICO  = Cursor.fetchall()
            for i in range(0,len(auxHISTORICO)):
                auxXini = 5 + 40*(auxHISTORICO [i][0] - limInicio) / (limFin - limInicio)
                auxXfin = 5 + 40*(auxHISTORICO [i][1] - limInicio) / (limFin - limInicio)
                auxY = auxY - 4
                plt.text(auxXini-2,auxY+1, auxHISTORICO [i][0].strftime('%Y-%m-%d') , fontsize=7)
                plt.text(auxXfin-2,auxY+1, auxHISTORICO [i][1].strftime('%Y-%m-%d') , fontsize=7)
                stLinea = 'y->'
                if auxXini>auxXfin:
                    stLinea = 'r<-'
                plt.plot((auxXini,auxXfin), (auxY,auxY), stLinea,linewidth = 2)

            plt.show()
            Conexion.close()
    finally:
        pass

#2.- Análisis Histórico Completo
def AnalisisCompleto(pHost, pUser, pPassword, pDB, pID_PRINCIPAL, pORD_PRINCIPAL):
    #2.0.- Inicialización de parámetro globales y conexión a base de datos
    Conexion = pymysql.connect(host = pHost, user = pUser, password = pPassword, db = pDB)
    VecTabla = ["HIST_A1","HIST_A2","HIST_A_B"]
    vecCompleto = []
    banCompleto = 1
    try:
        with Conexion.cursor() as Cursor:
            stSQL = "SELECT FECHA_INICIO, FECHA_FIN FROM " + pDB + ".HIST_PRINCIPAL_A "
            stSQL = stSQL + "WHERE  ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s "
            Cursor.execute(stSQL,(str(pID_PRINCIPAL),str(pORD_PRINCIPAL)))
            auxHISTORICO = Cursor.fetchall()
            vFecha_Inicio_Principal = auxHISTORICO [0][0]
            vFecha_Fin_Principal = auxHISTORICO [0][1]
            vDias_Principal = (vFecha_Fin_Principal-vFecha_Inicio_Principal).days + 1
            for i in range(0,len(VecTabla)):
                stSQL = "SELECT FECHA_INICIO, FECHA_FIN FROM " + pDB + "." + VecTabla[i]
                stSQL = stSQL + " WHERE  ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s "
                stSQL = stSQL + " ORDER BY FECHA_INICIO " 
                Cursor.execute(stSQL,(str(pID_PRINCIPAL),str(pORD_PRINCIPAL)))
                auxHISTORICO = Cursor.fetchall()
                if len(auxHISTORICO) > 0:
                    vDias_Historico = vDias_Principal
                    for j in range(0,len(auxHISTORICO)): 
                        vFecha_Inicio = auxHISTORICO [j][0]
                        vFecha_Fin = auxHISTORICO [j][1]
                        if j == 0 and (VecTabla[i] != "HIST_A2" or vFecha_Inicio_Principal > vFecha_Inicio) :
                            vDias_Historico = vDias_Historico + abs(vFecha_Inicio_Principal-vFecha_Inicio).days
                        vDias_Historico = vDias_Historico  - ((vFecha_Fin-vFecha_Inicio).days + 1)
                        if j < len(auxHISTORICO) - 1:
                            if (VecTabla[i] != "HIST_A2" or vFecha_Fin < auxHISTORICO[j+1][0]):
                                vDias_Historico = vDias_Historico  + (auxHISTORICO[j+1][0]-vFecha_Fin).days - 1
                            if (VecTabla[i] == "HIST_A2" and vFecha_Fin < auxHISTORICO[j+1][0]):
                                vDias_Historico = vDias_Historico  - (auxHISTORICO[j+1][0]-vFecha_Fin).days + 1
                        if j == len(auxHISTORICO) - 1 and VecTabla[i] == "HIST_A2":
                            vDias_Historico = vDias_Historico  - ((vFecha_Fin_Principal-vFecha_Fin).days )
                    if vDias_Historico != 0:
                        banCompleto = 0
                        vecCompleto.append(VecTabla[i])    
            Conexion.close()
    finally:
        pass
    return(vecCompleto,banCompleto)
    
#3.- Conjunto de Datos
def ConjuntoDatos(pHost, pUser, pPassword, pDB):
     #3.0.- Inicialización de parámetro globales y conexión a base de datos
    Conexion = pymysql.connect(host = pHost, user = pUser, password = pPassword, db = pDB)
    vecResultado = []
    try:
        with Conexion.cursor() as Cursor:
            stSQL = "SELECT ID_PRINCIPAL, ORD_PRINCIPAL, TABLA, ID_ERROR, COMENTARIO, DIAS_FECHA_INICIO, DIAS_FECHA_FIN "
            stSQL = stSQL + "FROM " + pDB + ".COMPARATIO_ERROR "
            stSQL = stSQL + "WHERE 1 = 1 "
            stSQL = stSQL + " ORDER BY REFERENCIA"
            Cursor.execute(stSQL)
            auxHISTORICO  = Cursor.fetchall()
            if len(auxHISTORICO) > 0:
                for i in range(0,len(auxHISTORICO )):
                    vAnalisis = AnalisisCompleto(pHost,pUser,pPassword,pDB,auxHISTORICO[i][0],auxHISTORICO[i][1])
                    vTabla  = auxHISTORICO[i][2]
                    idError = auxHISTORICO[i][3]
                    stAux   = auxHISTORICO[i][4]
                    idCorregido = 0
                    if stAux.find("Se ha corregido") != -1:
                        idCorregido = 1
                    idDifDias = 1
                    if auxHISTORICO[i][5] !=0 or auxHISTORICO[i][6] !=0:
                        idDifDias = 0
                    vecResultado.append((vTabla,idError,idCorregido,idDifDias,vAnalisis[1]))
            Conexion.close()
    finally:
        pass
    return(vecResultado )
    
    