# -*- coding: utf-8 -*-
"""
UNIVERSIDAD NACIONAL DE EDUCACIÓN A DISTANCIA

Master Inteligencia Artificial Avanzada

Trabajo de Fin de Master
Correción de Históricos tipo I y II

@author: Sergio Montes Vázquez
"""

#0.- Librerias para conexión a base de datos
import pymysql.cursors
import random
from datetime import datetime, date, time, timedelta

#1.- Corrección de daos del tipo 1
def correccionErrorTipoI(pHost, pUser, pPassword, pDB):
     #1.0.- Inicialización de parámetro globales y conexión a base de datos
    Conexion = pymysql.connect(host = pHost, user = pUser, password = pPassword, db = pDB)
    VecTabla = ["HIST_A1","HIST_A2","HIST_A_B"]
    try:
        with Conexion.cursor() as Cursor:
            #1.1.- Corrección Histórico Principal
            stSQL = "SELECT ID_PRINCIPAL, ORD_PRINCIPAL, FECHA_INICIO, FECHA_FIN FROM " + pDB + ".HIST_PRINCIPAL_A WHERE FECHA_INICIO > FECHA_FIN"
            Cursor.execute(stSQL)
            HIST_PRINCIPAL_A = Cursor.fetchall()
            for i in range(0,len(HIST_PRINCIPAL_A)):
                vFec_Inicio = HIST_PRINCIPAL_A[i][2]
                vFec_Fin = HIST_PRINCIPAL_A[i][3]
                for j in range(0,len(VecTabla)):
                    stSQL = "SELECT MIN(FECHA_INICIO), MAX(FECHA_FIN), COUNT(*) FROM " + pDB + "." + VecTabla[j] + " "
                    stSQL = stSQL + "WHERE FECHA_INICIO < FECHA_FIN AND ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s "
                    Cursor.execute(stSQL,(str(HIST_PRINCIPAL_A[i][0]),str(HIST_PRINCIPAL_A[i][1])))
                    auxHISTORICO  = Cursor.fetchall()
                    auxFec_Inicio = auxHISTORICO[0][0]
                    auxFec_Fin    = auxHISTORICO[0][1]
                    auxNumReg     = auxHISTORICO[0][2]
                    if auxNumReg > 0:
                        if vFec_Inicio > auxFec_Inicio : 
                            vFec_Inicio = auxFec_Inicio
                        if vFec_Fin < auxFec_Fin : 
                            vFec_Fin = auxFec_Fin

                if vFec_Inicio < vFec_Fin:
                    stSQL = "UPDATE " + pDB + ".HIST_PRINCIPAL_A SET FECHA_INICIO = %s, FECHA_FIN = %s, COMENTARIO = CONCAT(COMENTARIO,' Se ha corregido error tipo 1 histórico principal.') "
                    stSQL = stSQL + "WHERE FECHA_INICIO > FECHA_FIN AND ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s "
                    try:
                        Cursor.execute(stSQL,(vFec_Inicio.strftime('%Y-%m-%d'),vFec_Fin.strftime('%Y-%m-%d'),str(HIST_PRINCIPAL_A[i][0]),str(HIST_PRINCIPAL_A[i][1])))
                    finally:
                        pass
                else:
                    stSQL = "UPDATE " + pDB + ".HIST_PRINCIPAL_A SET COMENTARIO = CONCAT(COMENTARIO,' No se ha corregido error tipo 1 histórico principal.') "
                    stSQL = stSQL + "WHERE FECHA_INICIO > FECHA_FIN AND ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s "
                    try:
                        Cursor.execute(stSQL,(str(HIST_PRINCIPAL_A[i][0]),str(HIST_PRINCIPAL_A[i][1])))
                    finally:
                        pass
            
            #1.2.- Corrección Históricos Secundarios
            for i in range(0,len(VecTabla)):
                #1.2.1.- Se recupera el diccionario de datos
                stSQL = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY "
                stSQL = stSQL + "FROM INFORMATION_SCHEMA.COLUMNS "
                stSQL = stSQL + "WHERE TABLE_SCHEMA = '" + pDB + "' "
                stSQL = stSQL + "AND TABLE_NAME = '" + VecTabla[i] + "' "
                stSQL = stSQL + "ORDER BY ORDINAL_POSITION"
                Cursor.execute(stSQL)
                auxDiccionario = Cursor.fetchall()
                
                #1.2.2.- Se recupera las instrucciones para recuperar las Tablas de Claves Foráneas
                stSQL="SELECT DISTINCT B.REFERENCED_TABLE_SCHEMA,B.REFERENCED_TABLE_NAME "
                stSQL = stSQL + "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS A, "
                stSQL = stSQL + "	    INFORMATION_SCHEMA.KEY_COLUMN_USAGE B, "
                stSQL = stSQL + "     INFORMATION_SCHEMA.COLUMNS C "
                stSQL = stSQL + "WHERE A.CONSTRAINT_CATALOG = B.CONSTRAINT_CATALOG "
                stSQL = stSQL + "AND A.CONSTRAINT_SCHEMA = B.CONSTRAINT_SCHEMA "
                stSQL = stSQL + "AND A.CONSTRAINT_NAME = B.CONSTRAINT_NAME "
                stSQL = stSQL + "AND A.TABLE_SCHEMA = B.TABLE_SCHEMA "
                stSQL = stSQL + "AND A.TABLE_NAME = B.TABLE_NAME "
                stSQL = stSQL + "AND B.REFERENCED_TABLE_SCHEMA = C.TABLE_SCHEMA "
                stSQL = stSQL + "AND B.REFERENCED_TABLE_NAME = C.TABLE_NAME "
                stSQL = stSQL + "AND  C.COLUMN_NAME IN ('FECHA_INICIO','FECHA_FIN') "
                stSQL = stSQL + "AND A.CONSTRAINT_TYPE = 'FOREIGN KEY' "
                stSQL = stSQL + "AND A.TABLE_SCHEMA ='" + pDB + "' "
                stSQL = stSQL + "AND A.TABLE_NAME = '" + VecTabla[i] + "' "
                Cursor.execute(stSQL)
                auxTabla = Cursor.fetchall()
                vecTablasForeneas =[]
                for j in range(0,len(auxTabla)):
                    stInsSQL = "SELECT FECHA_INICIO, FECHA_FIN FROM " + auxTabla[j][0] + "." + auxTabla[j][1] + " WHERE 1 = 1 "
                    stIdCol  = ""
                    stIdTipo = ""
                    stSQL="SELECT A.REFERENCED_COLUMN_NAME, B.ORDINAL_POSITION, B.DATA_TYPE "
                    stSQL = stSQL + "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A, INFORMATION_SCHEMA.COLUMNS B "
                    stSQL = stSQL + "WHERE A.TABLE_SCHEMA = B.TABLE_SCHEMA "
                    stSQL = stSQL + "AND A.TABLE_NAME = B.TABLE_NAME "
                    stSQL = stSQL + "AND A.COLUMN_NAME = B.COLUMN_NAME "
                    stSQL = stSQL + "AND A.REFERENCED_TABLE_SCHEMA ='" + auxTabla[j][0] + "' "
                    stSQL = stSQL + "AND A.REFERENCED_TABLE_NAME = '" + auxTabla[j][1] + "' "
                    stSQL = stSQL + "AND A.TABLE_SCHEMA ='" + pDB + "' "
                    stSQL = stSQL + "AND A.TABLE_NAME = '" + VecTabla[i] + "' "
                    Cursor.execute(stSQL)
                    auxFiltro = Cursor.fetchall()
                    for k in range(0,len(auxFiltro)):
                        stInsSQL = stInsSQL + " AND " + auxFiltro[k][0] + " = %s " 
                        stIdCol  = stIdCol + str(auxFiltro[k][1]) + ":"
                        stIdTipo = stIdTipo + auxFiltro[k][2] + ":"
                    vecTablasForeneas.append((stInsSQL,stIdCol,stIdTipo))
                        
                #1.2.3.- Recorre los registros de los históricos con fechas invertidas
                stSQL = "SELECT * FROM " + pDB + "." + VecTabla[i] + " WHERE FECHA_FIN < FECHA_INICIO "
                Cursor.execute(stSQL)
                auxHISTORICO = Cursor.fetchall()
                for j in range (0,len(auxHISTORICO)):
                    #1.2.4.- Recupera los límites de fechas de los tables padres
                    vFec_Inicio = date(2010, 1, 1) 
                    vFec_Fin = date(2100, 1, 1) 
                    for k in range (0,len(vecTablasForeneas)):
                        auxIdFiltro = vecTablasForeneas[k][1]
                        auxIdTipo = vecTablasForeneas[k][2]
                        vecWhere = []
                        while len(auxIdFiltro) > 1 :
                            auxId = auxIdFiltro.find(":")
                            idTabla = auxIdFiltro[:auxId]
                            auxIdFiltro = auxIdFiltro[len(idTabla)+1:] 
                            auxId = auxIdTipo.find(":")
                            idTipo = auxIdTipo[:auxId]
                            auxIdTipo = auxIdTipo[len(idTipo )+1:] 
                            if idTipo == "int":
                                vecWhere.append(str(auxHISTORICO[j][int(idTabla)-1]))
                            if idTipo == "date":
                                vecWhere.append(auxHISTORICO[j][int(idTabla)-1].strftime('%Y-%m-%d'))
                        Cursor.execute(vecTablasForeneas[k][0],vecWhere)
                        auxTablasForeneas = Cursor.fetchall()
                        if vFec_Inicio < auxTablasForeneas[0][0] :
                            vFec_Inicio = auxTablasForeneas[0][0] 
                        if vFec_Fin > auxTablasForeneas[0][1] :
                            vFec_Fin = auxTablasForeneas[0][1] 
                            
                    #1.2.5.- Recupera las fechas del histórico original
                    colFechaInicio = -1
                    colFechaFin = -1
                    colPrincial = -1
                    colOrdinal = -1
                    for k in range (0,len(auxDiccionario)):
                        if auxDiccionario[k][0] == "ID_PRINCIPAL":
                            colPrincial = k
                        if auxDiccionario[k][0] == "ORD_PRINCIPAL":
                            colOrdinal = k
                        if auxDiccionario[k][0] == "FECHA_INICIO":
                            colFechaInicio = k
                        if auxDiccionario[k][0] == "FECHA_FIN":
                            colFechaFin = k

                    #1.2.6.- Recorre los registros del propio histórico para comparar fechas
                    if colPrincial != -1 and colOrdinal != -1 and colFechaInicio != -1:
                        stSQL = "SELECT FECHA_INICIO, FECHA_FIN FROM " + pDB + "." + VecTabla[i] 
                        stSQL = stSQL + " WHERE ID_PRINCIPAL =  %s AND ORD_PRINCIPAL = %s AND FECHA_INICIO <> %s and FECHA_INICIO <= FECHA_FIN  "
                        stSQL = stSQL + " ORDER BY FECHA_INICIO"
                        Cursor.execute(stSQL,(str(auxHISTORICO[j][colPrincial]), str(auxHISTORICO[j][colOrdinal]),auxHISTORICO[j][colFechaInicio].strftime('%Y-%m-%d')))
                        auxHisResto = Cursor.fetchall()
                        for k in range(0,len(auxHisResto)):
                            if vFec_Inicio < auxHisResto[k][0] and auxHisResto[k][0] <= vFec_Fin:
                                vFec_Fin = auxHisResto[k][0]  + timedelta(days = -1)

                            if vFec_Fin > auxHisResto[k][1] and vFec_Inicio <= auxHisResto[k][1]:
                                vFec_Inicio = auxHisResto[k][1]  + timedelta(days = 1)
                    
                    stSQL = "SELECT COUNT(*) FROM " + pDB + "." + VecTabla[i] 
                    stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_INICIO = %s "
                    Cursor.execute(stSQL,(str(auxHISTORICO[j][colPrincial]), str(auxHISTORICO[j][colOrdinal]),vFec_Inicio.strftime('%Y-%m-%d')))
                    auxComprueba = Cursor.fetchall()
                    vComprueba = auxComprueba[0][0]

                    
                    if vFec_Inicio < vFec_Fin and vComprueba==0:
                        stSQL = "UPDATE " + pDB + "." + VecTabla[i] + " SET FECHA_INICIO = %s, FECHA_FIN = %s, COMENTARIO = CONCAT(COMENTARIO,' Se ha corregido error tipo 1 histórico secundario.') "
                        stSQL = stSQL + "WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_INICIO = %s"
                        try:
                            Cursor.execute(stSQL,(vFec_Inicio.strftime('%Y-%m-%d'),vFec_Fin.strftime('%Y-%m-%d'),str(auxHISTORICO[j][colPrincial]), str(auxHISTORICO[j][colOrdinal]),auxHISTORICO[j][colFechaInicio].strftime('%Y-%m-%d')))
                        finally:
                            pass
                    else:
                        stSQL = "UPDATE " + pDB + "." + VecTabla[i] + " SET COMENTARIO = CONCAT(COMENTARIO,' No se ha corregido error tipo 1 histórico secundario') "
                        stSQL = stSQL + "WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_INICIO = %s"
                        try:
                            Cursor.execute(stSQL,(str(auxHISTORICO[j][colPrincial]), str(auxHISTORICO[j][colOrdinal]),auxHISTORICO[j][colFechaInicio].strftime('%Y-%m-%d')))
                        finally:
                            pass

        Conexion.commit()
        Conexion.close()
    finally:
        pass

#2.- Corrección de daos del tipo 2
def correccionErrorTipoII(pHost, pUser, pPassword, pDB):
     #2.0.- Inicialización de parámetro globales y conexión a base de datos
    Conexion = pymysql.connect(host = pHost, user = pUser, password = pPassword, db = pDB)
    VecTabla = ["HIST_PRINCIPAL_A","HIST_A1","HIST_A2","HIST_A_B"]
    limFec_Inicio = datetime(2010, 1, 1)
    limFec_Fin = datetime(2010, 1, 1)
    try:
        with Conexion.cursor() as Cursor:
            #2.1.- Corrección de las fechas Inicio o Fin desfasadas
            stSQL = "SELECT A.ID_PRINCIPAL, A.ORD_PRINCIPAL, A.FECHA_INICIO, A.FECHA_FIN, "
            stSQL = stSQL + "MIN(A1.FECHA_INICIO) FECHA_INICIO_A1, MAX(A1.FECHA_FIN) FECHA_FIN_A1, "
            stSQL = stSQL + "MIN(A2.FECHA_INICIO) FECHA_INICIO_A2, MAX(A2.FECHA_FIN) FECHA_FIN_A2, "
            stSQL = stSQL + "MIN(A_B.FECHA_INICIO) FECHA_INICIO_A_B, MAX(A_B.FECHA_FIN) FECHA_FIN_A_B, "
            stSQL = stSQL + "A.COMENTARIO "
            stSQL = stSQL + "FROM " + pDB + ".HIST_PRINCIPAL_A A "
            stSQL = stSQL + "	LEFT JOIN " + pDB + ".HIST_A1 A1 ON (A.ID_PRINCIPAL = A1.ID_PRINCIPAL AND A.ORD_PRINCIPAL = A1.ORD_PRINCIPAL AND A1.FECHA_INICIO <= A1.FECHA_FIN) "
            stSQL = stSQL + "  LEFT JOIN " + pDB + ".HIST_A2 A2 ON (A.ID_PRINCIPAL = A2.ID_PRINCIPAL AND A.ORD_PRINCIPAL = A2.ORD_PRINCIPAL AND A2.FECHA_INICIO <= A2.FECHA_FIN) "
            stSQL = stSQL + "  LEFT JOIN " + pDB + ".HIST_A_B A_B ON (A.ID_PRINCIPAL = A_B.ID_PRINCIPAL AND A.ORD_PRINCIPAL = A_B.ORD_PRINCIPAL AND A_B.FECHA_INICIO <= A_B.FECHA_FIN) "
            stSQL = stSQL + "WHERE  1 = 1 "
            stSQL = stSQL + "AND A.FECHA_INICIO <= A.FECHA_FIN "
            stSQL = stSQL + "GROUP BY A.ID_PRINCIPAL, A.ORD_PRINCIPAL, A.FECHA_INICIO, A.FECHA_FIN, A.COMENTARIO "
            stSQL = stSQL + "HAVING (MIN(A1.FECHA_INICIO) - A.FECHA_INICIO <> 0 AND COUNT(A1.FECHA_INICIO) <> 0) "
            stSQL = stSQL + "    OR (MIN(A2.FECHA_INICIO) - A.FECHA_INICIO < 0 AND COUNT(A2.FECHA_INICIO) <> 0) "
            stSQL = stSQL + "    OR (MIN(A_B.FECHA_INICIO) - A.FECHA_INICIO <> 0 AND COUNT(A_B.FECHA_INICIO) <> 0) "
            stSQL = stSQL + "    OR (MAX(A1.FECHA_FIN) - A.FECHA_FIN <> 0 AND COUNT(A1.FECHA_FIN) <> 0) "
            stSQL = stSQL + "    OR (MAX(A2.FECHA_FIN) - A.FECHA_FIN > 0 AND COUNT(A2.FECHA_FIN) <> 0) "
            stSQL = stSQL + "    OR (MAX(A_B.FECHA_FIN) - A.FECHA_FIN <> 0 AND COUNT(A_B.FECHA_FIN) <> 0) "
            Cursor.execute(stSQL)
            auxFechas = Cursor.fetchall()
            for i in range(0,len(auxFechas)):
                vecFechasIni = []
                numMaxIni = 0
                posMaxIni = 0
                vecFechasFin = []
                numMaxFin = 0
                posMaxFin = 0
                for j in range(0,4):
                    auxValorIni = auxFechas[i][2+(j*2)]
                    auxValorFin = auxFechas[i][3+(j*2)]
                    if auxValorIni != None or auxValorFin != None:
                        if j == 0 :
                            vecFechasIni.append([auxValorIni,1])
                            vecFechasFin.append([auxValorFin,1])
                        else:
                            banAgregar = 1
                            for k in range(0,len(vecFechasIni)):
                                if vecFechasIni[k][0] == auxValorIni:
                                    banAgregar = 0
                                    vecFechasIni[k][1] = vecFechasIni[k][1] + 1
                            if banAgregar == 1 :
                                vecFechasIni.append([auxValorIni,1])
                            banAgregar = 1
                            for k in range(0,len(vecFechasFin)):
                                if vecFechasFin[k][0] == auxValorFin:
                                    banAgregar = 0
                                    vecFechasFin[k][1] = vecFechasFin[k][1] + 1
                            if banAgregar == 1 :
                                vecFechasFin.append([auxValorFin,1])
                                
                if len(vecFechasIni)>1:
                    numMaxIni = vecFechasIni[0][1]
                    for j in range(1,len(vecFechasIni)):
                        if numMaxIni < vecFechasIni[j][1]:
                            numMaxIni = vecFechasIni[j][1]
                            posMaxIni = j
                            
                    # Hay una fecha que más se repite y todas las demás se deben ajustar a ella
                    if numMaxIni > 1:
                        for j in range(0,4):
                            auxValorIni = auxFechas[i][2+(j*2)]
                            if auxValorIni != None:
                                if vecFechasIni[posMaxIni][0] != auxValorIni:
                                    stSQL = "SELECT COUNT(*) FROM " + pDB + "." + VecTabla[j]
                                    stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_INICIO = %s"
                                    Cursor.execute(stSQL,(str(auxFechas[i][0]),str(auxFechas[i][1]),vecFechasIni[posMaxIni][0].strftime('%Y-%m-%d')))
                                    auxVerifica = Cursor.fetchall()
                                    if auxVerifica[0][0] == 0:
                                        stSQL = "UPDATE " + pDB + "." + VecTabla[j]
                                        stSQL = stSQL + " SET FECHA_INICIO = %s, COMENTARIO = CONCAT(COMENTARIO,' Se ha corregido error tipo 2 fecha inicio desfase.') "
                                        stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_INICIO = %s"
                                        try:
                                            Cursor.execute(stSQL,(vecFechasIni[posMaxIni][0].strftime('%Y-%m-%d'),str(auxFechas[i][0]),str(auxFechas[i][1]),auxValorIni.strftime('%Y-%m-%d')))
                                        finally:
                                            pass
                                    else:
                                        stSQL = "UPDATE " + pDB + "." + VecTabla[j]
                                        stSQL = stSQL + " SET COMENTARIO = CONCAT(COMENTARIO,' No se ha corregido error tipo 2 fecha inicio desfase por duplicar clave primaria.') "
                                        stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_INICIO = %s"
                                        try:
                                            Cursor.execute(stSQL,(str(auxFechas[i][0]),str(auxFechas[i][1]),auxValorIni.strftime('%Y-%m-%d')))
                                        finally:
                                            pass
                    # Si no hay una fecha predominante entonces todas las demás se deben ajustar al histórico principal                 
                    if numMaxIni == 1:
                        auxValorIniPrincipal = auxFechas[i][2]
                        for j in range(1,4):
                            auxValorIni = auxFechas[i][2+(j*2)]
                            if auxValorIni != None:
                                if auxValorIniPrincipal != auxValorFin:
                                    stSQL = "SELECT COUNT(*) FROM " + pDB + "." + VecTabla[j]
                                    stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_INICIO = %s"
                                    Cursor.execute(stSQL,(str(auxFechas[i][0]),str(auxFechas[i][1]),auxValorIniPrincipal.strftime('%Y-%m-%d')))
                                    auxVerifica = Cursor.fetchall()
                                    if auxVerifica[0][0] == 0:
                                        stSQL = "UPDATE " + pDB + "." + VecTabla[j]
                                        stSQL = stSQL + " SET FECHA_INICIO = %s , COMENTARIO = CONCAT(COMENTARIO,' Se ha corregido error tipo 2 fecha fin desfase.') "
                                        stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_INICIO = %s"
                                        try:
                                            Cursor.execute(stSQL,(auxValorIniPrincipal.strftime('%Y-%m-%d'),str(auxFechas[i][0]),str(auxFechas[i][1]),auxValorIni.strftime('%Y-%m-%d')))
                                        finally:
                                            pass
                                    else:
                                        stSQL = "UPDATE " + pDB + "." + VecTabla[j]
                                        stSQL = stSQL + " SET COMENTARIO = CONCAT(COMENTARIO,' No se ha corregido error tipo 2 fecha inicio desfase por duplicar clave primaria.') "
                                        stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_INICIO = %s"
                                        try:
                                            Cursor.execute(stSQL,(str(auxFechas[i][0]),str(auxFechas[i][1]),auxValorIni.strftime('%Y-%m-%d')))
                                        finally:
                                            pass
                if len(vecFechasFin)>1:
                    numMaxFin = vecFechasFin[0][1]
                    for j in range(1,len(vecFechasFin)):
                        if numMaxFin < vecFechasFin[j][1]:
                            numMaxFin = vecFechasFin[j][1]
                            posMaxFin = j
                    # Hay una fecha que más se repite y todas las demás se deben ajustar a ella
                    if numMaxFin > 1:
                        for j in range(0,4):
                            auxValorFin = auxFechas[i][3+(j*2)]
                            if auxValorFin != None:
                                if vecFechasFin[posMaxFin][0] != auxValorFin:
                                    stSQL = "SELECT COUNT(*) FROM " + pDB + "." + VecTabla[j]
                                    stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_FIN = %s"
                                    Cursor.execute(stSQL,(str(auxFechas[i][0]),str(auxFechas[i][1]),vecFechasFin[posMaxFin][0].strftime('%Y-%m-%d')))
                                    auxVerifica = Cursor.fetchall()
                                    if auxVerifica[0][0] == 0:
                                        stSQL = "UPDATE " + pDB + "." + VecTabla[j]
                                        stSQL = stSQL + " SET FECHA_FIN = %s , COMENTARIO = CONCAT(COMENTARIO,' Se ha corregido error tipo 2 fecha fin desfase.') "
                                        stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_FIN = %s"
                                        try:
                                            Cursor.execute(stSQL,(vecFechasFin[posMaxFin][0].strftime('%Y-%m-%d'),str(auxFechas[i][0]),str(auxFechas[i][1]),auxValorFin.strftime('%Y-%m-%d')))
                                        finally:
                                            pass
                                    else:
                                        stSQL = "UPDATE " + pDB + "." + VecTabla[j]
                                        stSQL = stSQL + " SET COMENTARIO = CONCAT(COMENTARIO,' No se ha corregido error tipo 2 fecha inicio desfase por duplicar clave primaria.') "
                                        stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_FIN = %s"
                                        try:
                                            Cursor.execute(stSQL,(str(auxFechas[i][0]),str(auxFechas[i][1]),auxValorFin.strftime('%Y-%m-%d')))
                                        finally:
                                            pass
                    # Si no hay una fecha predominante entonces todas las demás se deben ajustar al histórico principal                 
                    if numMaxFin == 1:
                        auxValorFinPrincipal = auxFechas[i][3]
                        for j in range(1,4):
                            auxValorFin = auxFechas[i][3+(j*2)]
                            if auxValorFin != None:
                                if auxValorFinPrincipal != auxValorFin:
                                    stSQL = "SELECT COUNT(*) FROM " + pDB + "." + VecTabla[j]
                                    stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_FIN = %s"
                                    Cursor.execute(stSQL,(str(auxFechas[i][0]),str(auxFechas[i][1]),auxValorFinPrincipal.strftime('%Y-%m-%d')))
                                    auxVerifica = Cursor.fetchall()
                                    if auxVerifica[0][0] == 0:
                                        stSQL = "UPDATE " + pDB + "." + VecTabla[j]
                                        stSQL = stSQL + " SET FECHA_FIN = %s , COMENTARIO = CONCAT(COMENTARIO,' Se ha corregido error tipo 2 fecha fin desfase.') "
                                        stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_FIN = %s"
                                        try:
                                            Cursor.execute(stSQL,(auxValorFinPrincipal.strftime('%Y-%m-%d'),str(auxFechas[i][0]),str(auxFechas[i][1]),auxValorFin.strftime('%Y-%m-%d')))
                                        finally:
                                            pass
                                    else:
                                        stSQL = "UPDATE " + pDB + "." + VecTabla[j]
                                        stSQL = stSQL + " SET COMENTARIO = CONCAT(COMENTARIO,' No se ha corregido error tipo 2 fecha inicio desfase por duplicar clave primaria.') "
                                        stSQL = stSQL + " WHERE ID_PRINCIPAL = %s AND ORD_PRINCIPAL = %s AND FECHA_FIN = %s"
                                        try:
                                            Cursor.execute(stSQL,(str(auxFechas[i][0]),str(auxFechas[i][1]),auxValorFin.strftime('%Y-%m-%d')))
                                        finally:
                                            pass

            #2.2.- Corrección de tramos traslapados
            for i in range(1,len(VecTabla)):
                stSQL = "SELECT DISTINCT A.ID_PRINCIPAL, A.ORD_PRINCIPAL, "
                stSQL = stSQL + "If ( A.FECHA_INICIO < B.FECHA_INICIO, A.FECHA_INICIO, B.FECHA_INICIO) FECHA_INICIO_1, "
                stSQL = stSQL + "If ( A.FECHA_INICIO > B.FECHA_INICIO, A.FECHA_INICIO, B.FECHA_INICIO) FECHA_INICIO_2, "
                stSQL = stSQL + "If ( A.FECHA_FIN < B.FECHA_FIN, A.FECHA_FIN, B.FECHA_FIN) FECHA_FIN_1, "
                stSQL = stSQL + "If ( A.FECHA_FIN > B.FECHA_FIN, A.FECHA_FIN, B.FECHA_FIN) FECHA_FIN_2 "
                stSQL = stSQL + "FROM " + pDB + "." + VecTabla[i] + " A, " + pDB + "." + VecTabla[i] + " B "
                stSQL = stSQL + "WHERE A.ID_PRINCIPAL = B.ID_PRINCIPAL "
                stSQL = stSQL + "AND A.ORD_PRINCIPAL = B.ORD_PRINCIPAL " 
                stSQL = stSQL + "AND ((B.FECHA_INICIO < A.FECHA_INICIO AND A.FECHA_INICIO < B.FECHA_FIN) "
                stSQL = stSQL + " OR (B.FECHA_INICIO < A.FECHA_FIN AND A.FECHA_FIN < B.FECHA_FIN)) "
                Cursor.execute(stSQL)
                auxFechas = Cursor.fetchall()
                for j in range(0,len(auxFechas)):
                    vID_PRINCIPAL  = auxFechas[j][0]
                    vORD_PRINCIPAL = auxFechas[j][1]
                    vFECHA_INICIO_1 = auxFechas[j][2]
                    vFECHA_INICIO_2 = auxFechas[j][3]
                    vFECHA_FIN_1 = auxFechas[j][4]
                    vFECHA_FIN_2 = auxFechas[j][5]
                    banSeguir = 1
                    vFECHA_INICIO = (vFECHA_INICIO_1,vFECHA_INICIO_2)
                    vFECHA_FIN = (vFECHA_FIN_1,vFECHA_FIN_2)
                    #2.2.1.- Corrección de tramos traslapados fecha fin por fecha de inicio
                    for k in range(0,len(vFECHA_INICIO)):
                        stSQL = "SELECT MIN(B.FECHA_INICIO) "
                        stSQL = stSQL + "FROM " + pDB + "." + VecTabla[i] + " A, " + pDB + "." + VecTabla[i] + " B "
                        stSQL = stSQL + "WHERE A.ID_PRINCIPAL = B.ID_PRINCIPAL "
                        stSQL = stSQL + "AND A.ORD_PRINCIPAL = B.ORD_PRINCIPAL "
                        stSQL = stSQL + "AND A.ID_PRINCIPAL = %s "
                        stSQL = stSQL + "AND A.ORD_PRINCIPAL = %s "
                        stSQL = stSQL + "AND datediff(A.FECHA_FIN , %s)= -1 "
                        stSQL = stSQL + "AND B.FECHA_INICIO > %s "
                        stSQL = stSQL + "GROUP BY A.FECHA_INICIO, A.FECHA_FIN "
                        Cursor.execute(stSQL,(str(vID_PRINCIPAL),str(vORD_PRINCIPAL),vFECHA_INICIO[k].strftime('%Y-%m-%d'),vFECHA_INICIO[k].strftime('%Y-%m-%d')))
                        auxFechas_Fin = Cursor.fetchall()
                        if len(auxFechas_Fin) > 0 :
                            vFechas_Fin = auxFechas_Fin[0][0] + timedelta(days = -1)
                            stSQL = "UPDATE " + pDB + "." + VecTabla[i] 
                            stSQL = stSQL + " SET FECHA_FIN = %s, COMENTARIO = CONCAT(COMENTARIO,' Se ha corregido error tipo 2 modificación fecha fin.')  "
                            stSQL = stSQL + "WHERE ID_PRINCIPAL = %s "
                            stSQL = stSQL + "AND ORD_PRINCIPAL = %s "
                            stSQL = stSQL + "AND FECHA_INICIO = %s "
                            try:
                                Cursor.execute(stSQL,(vFechas_Fin.strftime('%Y-%m-%d'),str(vID_PRINCIPAL),str(vORD_PRINCIPAL),vFECHA_INICIO[k].strftime('%Y-%m-%d')))
                            finally:
                                pass
								
                    #2.2.2.- Corrección de tramos traslapados fecha inicio por fecha de fin
                    for k in range(0,len(vFECHA_FIN)):
                        stSQL = "SELECT MAX(B.FECHA_FIN) "
                        stSQL = stSQL + "FROM " + pDB + "." + VecTabla[i] + " A, " + pDB + "." + VecTabla[i] + " B "
                        stSQL = stSQL + "WHERE A.ID_PRINCIPAL = B.ID_PRINCIPAL "
                        stSQL = stSQL + "AND A.ORD_PRINCIPAL = B.ORD_PRINCIPAL "
                        stSQL = stSQL + "AND A.ID_PRINCIPAL = %s "
                        stSQL = stSQL + "AND A.ORD_PRINCIPAL = %s "
                        stSQL = stSQL + "AND datediff(A.FECHA_INICIO , %s)= 1 "
                        stSQL = stSQL + "AND B.FECHA_FIN < %s "
                        stSQL = stSQL + "GROUP BY A.FECHA_INICIO, A.FECHA_FIN "
                        Cursor.execute(stSQL,(str(vID_PRINCIPAL),str(vORD_PRINCIPAL),vFECHA_FIN[k].strftime('%Y-%m-%d'),vFECHA_FIN[k].strftime('%Y-%m-%d')))
                        auxFechas_Inicio = Cursor.fetchall()
                        if len(auxFechas_Inicio) > 0 :
                            vFechas_Inicio = auxFechas_Inicio[0][0] + timedelta(days = 1)
                            stSQL = "UPDATE " + pDB + "." + VecTabla[i] 
                            stSQL = stSQL + " SET FECHA_INICIO  = %s, COMENTARIO = CONCAT(COMENTARIO,' Se ha corregido error tipo 2 modificación fecha inicio.')   "
                            stSQL = stSQL + "WHERE ID_PRINCIPAL = %s "
                            stSQL = stSQL + "AND ORD_PRINCIPAL = %s "
                            stSQL = stSQL + "AND FECHA_FIN= %s "
                            try:
                                Cursor.execute(stSQL,(vFechas_Inicio.strftime('%Y-%m-%d'),str(vID_PRINCIPAL),str(vORD_PRINCIPAL),vFECHA_FIN[k].strftime('%Y-%m-%d')))
                            finally:
                                pass

            #2.3.- Corrección de Huecos
            for i in range(1,len(VecTabla)):
                if VecTabla[i] != "HIST_A2":
                    stSQL = "SELECT A.ID_PRINCIPAL, A.ORD_PRINCIPAL, A.FECHA_INICIO, MAX(B.FECHA_FIN) "
                    stSQL = stSQL + "FROM " + pDB + "." + VecTabla[i] + " A , " + pDB + "." + VecTabla[i] + " B "
                    stSQL = stSQL + "WHERE A.ID_PRINCIPAL = B.ID_PRINCIPAL "
                    stSQL = stSQL + " AND A.ORD_PRINCIPAL = B.ORD_PRINCIPAL "
                    stSQL = stSQL + " AND A.FECHA_INICIO > B.FECHA_INICIO "
                    stSQL = stSQL + " AND A.FECHA_INICIO > B.FECHA_FIN "
                    stSQL = stSQL + "GROUP BY A.ID_PRINCIPAL, A.ORD_PRINCIPAL, A.FECHA_INICIO "
                    stSQL = stSQL + "HAVING datediff(A.FECHA_INICIO,MAX(B.FECHA_FIN)) > 1 "
                    Cursor.execute(stSQL)
                    auxFechas = Cursor.fetchall()
                    for j in range(0,len(auxFechas)):
                        vID_PRINCIPAL  = auxFechas[j][0]
                        vORD_PRINCIPAL = auxFechas[j][1]
                        vFECHA_INICIO  = auxFechas[j][2]
                        vFECHA_FIN_OLD = auxFechas[j][3]
                        vFECHA_FIN_NEW = vFECHA_INICIO + timedelta(days = -1)
                        stSQL = "UPDATE " + pDB + "." + VecTabla[i] 
                        stSQL = stSQL + " SET FECHA_FIN = %s, COMENTARIO = CONCAT(COMENTARIO,' Se ha corregido error tipo 2 modificación fecha fin por hueco.')   "
                        stSQL = stSQL + "WHERE ID_PRINCIPAL = %s "
                        stSQL = stSQL + "AND ORD_PRINCIPAL = %s "
                        stSQL = stSQL + "AND FECHA_FIN = %s "
                        try:
                            Cursor.execute(stSQL,(vFECHA_FIN_NEW.strftime('%Y-%m-%d'),str(vID_PRINCIPAL),str(vORD_PRINCIPAL),vFECHA_FIN_OLD.strftime('%Y-%m-%d')))
                        finally:
                            pass
        Conexion.commit()
        Conexion.close()
    finally:
        pass

#99.- Rutina Principal
stHost =  'localhost'
stUser = 'root'
stPassword = 'xxxxxxxxxx'
stDB = 'tfm_smv'
correccionErrorTipoI(stHost,stUser,stPassword,stDB)
correccionErrorTipoII(stHost,stUser,stPassword,stDB)
