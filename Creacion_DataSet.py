# -*- coding: utf-8 -*-
"""
UNIVERSIDAD NACIONAL DE EDUCACION A DISTANCIA
Master: Inteligencia Artificial Avanzada 
Materia: Trabajo de Fin de Master 
Módulo: Carga y Transformación de los Datos en el DataSet 
@author: Sergio Montes Vázquez
Noviembre 2019
"""

#0.- Librerias e inicia variables 
import pymysql.cursors
from mysql.connector import Error


def Crea_DataSet(stHost, stUser, stPassword, stDB):
    
 
    #1.- Conexión MyServer
    varResultado = 1
    myConexion = pymysql.connect(host = stHost, user = stUser, password = stPassword, db = stDB)
    
    print ('-> 0.- Inicia creación ')
    try:
        with myConexion.cursor() as myCursor:
            print ('--> 1.- Entra en Cursor MySql')
            
            #2.- Creación de la tabla de orden
            print ('--> 2.- Creación de la tabla de orden')
            
            #2.1.- Se borra la tabla si es que existe
            stTabla = "CAMPO_VALOR_ORDEN"
            stSQL = "DROP TABLE IF EXISTS " + stDB + "." + stTabla
            myCursor.execute(stSQL)
            
            #2.2.- Se crea la estructura de la tabla
            stSQL = "CREATE TABLE IF NOT EXISTS " + stDB + "." + stTabla + " (\n"
            stSQL = stSQL + "  CAMPO   VARCHAR(50),\n"
            stSQL = stSQL + "  VALOR   VARCHAR(50),\n"
            stSQL = stSQL + "  ORDEN   INT )"
            myCursor.execute(stSQL)
            
            #2.3.- Preparación de los datos
            vecOrden = []
            vecOrden.append(["NIVEL_EDUCATIVO",["S/D","Sin Universidad","Con Universidad"]])
            vecOrden.append(["EDAD",["S/D","Joven","Adulto","Mayor"]])
            vecOrden.append(["FAMILIA",["S/D","Sin hijos","Con hijos","Familia numerosa"]])
            vecOrden.append(["GRUPO_PUESTO",["S/D","Administrativo","Coordinador","Responsable"]])
            vecOrden.append(["ANTIGUEDAD",["S/D","Poca","Mediana","Mucha"]])
            vecOrden.append(["SBA",["S/D","Bajo","Medio","Bueno","Excelente"]])
            vecOrden.append(["DES_PORC_AUM",["S/D","Descuento","Sin Aumento","Aumento"]])
            vecOrden.append(["FORMACION",["S/D","Insuficiente","Suficiente","Demasiado"]])
            vecOrden.append(["DESEMPENNO",["S/D","Bajo","Alto"]])
            vecOrden.append(["EVP",["S/D","Bajo","Alto"]])
            vecOrden.append(["ESTADO",["S/D","DES-/EVP-","DES-/EVP+","DES+/EVP-","DES+/EVP+","BAJA"]])
            for iOrden in vecOrden:
                for i in range(0,len(iOrden[1])):
                    stSQL = "INSERT INTO " + stDB + "." + stTabla + "(CAMPO,VALOR,ORDEN) VALUES('" + iOrden[0] + "','"
                    stSQL = stSQL + iOrden[1][i] + "'," + str(i) + ")"
                     
                    myCursor.execute(stSQL)
            
            myConexion.commit()
            
            #3.- Creación de la tabla de DataSet
            #3.1.- Se borra la tabla si es que existe
            stTabla = "MID_DATOS"
            stSQL = "DROP TABLE IF EXISTS " + stDB + "." + stTabla
            myCursor.execute(stSQL)
            
            #3.2.- Se crea la estructura de la tabla
            stSQL = "CREATE TABLE IF NOT EXISTS " + stDB + "." + stTabla + " (\n"
            stSQL = stSQL + "  ID_EMPLEADO      VARCHAR(12),\n"
            stSQL = stSQL + "  ANNO             INT,\n"
            #Variables propias de cada Empleado y el Puesto de Trabajo
            stSQL = stSQL + "  SEXO             VARCHAR(1),\n"
            stSQL = stSQL + "  ANNO_NAC         INT,\n"
            stSQL = stSQL + "  EDAD             VARCHAR(20),\n"     #Joven (menor de 30 años); Adulto (Mayor de 30 años y menor de 60) y Mayor (más de 60) años.
            stSQL = stSQL + "  NUM_HIJOS        INT,\n"             #Número de hijos
            stSQL = stSQL + "  FAMILIA          VARCHAR(20),\n"     #Sin hijos; con hijos (1 o 2 hijos o personas a cargo); familia numerosa (más de 2 hijos o personas a cargo).
            stSQL = stSQL + "  PUESTO   		VARCHAR(20),\n"
            stSQL = stSQL + "  GRUPO_PUESTO     VARCHAR(20),\n"     #Administrativo, Coordinador y Responsable.
            stSQL = stSQL + "  ANNO_ANTIGUEDAD  INT,\n"             #Años Antiguedad
            stSQL = stSQL + "  ANTIGUEDAD       VARCHAR(20),\n"     #Poca (menos de 3 años), Mediana (más de 3 y menos de 10) y Mucha (más de 10 Años).
            stSQL = stSQL + "  SBA_IMPORTE      NUMERIC(18,2),\n"   #Importe Salario Bruto Anual
            stSQL = stSQL + "  SBA              VARCHAR(20),\n"     #Bajo (Menos de 20.000,00€), Medio (más de 20,000.00€ y menos de 30,000.00€), Bueno (más de 30.000,00€ y menos de 45.000,00€) y Excelente (más de 45.000,00€).
            stSQL = stSQL + "  PORC_AUMENTO     NUMERIC(18,2),\n"   #Porcentaje de aumento con respecto al año anterior
            stSQL = stSQL + "  DES_PORC_AUM     VARCHAR(50),\n"     #Descripción Porcentaje de aumento con respecto al año anterior
            stSQL = stSQL + "  NIVEL_EDUCATIVO  VARCHAR(20),\n"     #Sin Estudios Universitarios, Con Universidad y Con Posgrado.
            stSQL = stSQL + "  NUM_CURSOS       INT,\n"             #Número de Cursos de Formación
            stSQL = stSQL + "  FORMACION        VARCHAR(20),\n"     #Insuficiente (0 ó 1 curso), Suficiente (2 cursos), Demasiado (más de 2 cursos).
            #Variables que se cálculan durante el modelaje y relaticvas al EVP
            stSQL = stSQL + "  DESEMPENNO_NUM   NUMERIC(8,2),\n"
            stSQL = stSQL + "  DESEMPENNO       VARCHAR(20),\n"     #Alto, Bajo
            stSQL = stSQL + "  EVP_APRENDIZAJE  NUMERIC(8,2),\n"
            stSQL = stSQL + "  EVP_CARRERA      NUMERIC(8,2),\n"
            stSQL = stSQL + "  EVP_EQUILIBRIO   NUMERIC(8,2),\n"
            stSQL = stSQL + "  EVP_RETRIBUCION  NUMERIC(8,2),\n"
            stSQL = stSQL + "  EVP_NUM          NUMERIC(8,2),\n"
            stSQL = stSQL + "  EVP              VARCHAR(20),\n"     #Alto, Bajo
            #Variables Propias de la cadena de Markov
            stSQL = stSQL + "  ESTADO           VARCHAR(20),\n"
            stSQL = stSQL + "  ESTADO_ANTERIOR  VARCHAR(20),\n"
            stSQL = stSQL + "  ES_SEPARACION    NUMERIC(1),\n"      #Es separación
            stSQL = stSQL + "  ES_ABANDONO      NUMERIC(1),\n"      #Es abandono
            #Clase que nos interesa obtener
            stSQL = stSQL + "  SEPARACION       NUMERIC(18,4),\n"
            stSQL = stSQL + "  ABANDONO         NUMERIC(18,4),\n"
            #Estimación de Precios y decisión de fin de puesto.
            stSQL = stSQL + "  PRE_AUMENTO      NUMERIC(18,4),\n"
            stSQL = stSQL + "  PRE_FORMACION    NUMERIC(18,4),\n"
            stSQL = stSQL + "  PRE_PRUEBA       NUMERIC(18,4),\n"
            stSQL = stSQL + "  FIN_PUESTO       NUMERIC(1),\n"      #Variable de Fin de Puesto
            stSQL = stSQL + "  MUESTRA          NUMERIC(1) )"
            myCursor.execute(stSQL)
            myConexion.commit()
            
            #4.- Creación del DataSet
            print ('--> 3.- Creación de la tabla DataSet')
            
            #4.1.- Se recorre la lista de Empleados y periodos
            stSQL = "SELECT A.STD_ID_PERSON, A.STD_ID_GENDER, ifnull(A.STD_DT_BIRTH,date_add(B.STD_DT_START,interval -30 YEAR)), B.STD_DT_START, B.STD_DT_END, ifnull(B.SSP_FEC_ANTIGUEDAD,B.STD_DT_START), ifnull(B.STD_ID_HRP_END_REA,'S/D') \n"
            stSQL = stSQL + "FROM " + stDB + ".STD_PERSON A, " + stDB + ".STD_HR_PERIOD B \n"
            stSQL = stSQL + "WHERE A.STD_ID_PERSON = B.STD_ID_HR \n"
            stSQL = stSQL + "AND NOT( A.STD_ID_PERSON LIKE 'BC%') \n"
            stSQL = stSQL + "ORDER BY A.STD_ID_PERSON, B.STD_DT_START "
            myCursor.execute(stSQL)
            regEmpleados = myCursor.fetchall()
            
            numRegistros = 0
            for i in range(0,len(regEmpleados)):
                idEmpleado = regEmpleados[i]
                stEmpleado = idEmpleado[0] 
                
                #4.2.- Se recuperan y transforman datos
                vSTD_DT_BIRTH = idEmpleado[2]
                vANNO_INI = idEmpleado[3].year
                vANNO_FIN = idEmpleado[4].year
                vFEC_ANTIGUEDAD = idEmpleado[5]
                vDT_START = idEmpleado[3]
                vSTD_ID_HRP_END_REA = idEmpleado[6]
                vANNO = vANNO_INI
                 
                if vANNO_FIN == 4000:
                    vANNO_FIN = 2017
                    
                vSTD_ID_GENDER = 'M'
                if idEmpleado[1] == '1':  
                    vSTD_ID_GENDER = 'H'
                
                #4.3.- Hace un recorrido para los años de cada empleado
                while vANNO <=  vANNO_FIN:
                    #4.4.- Se comprueba que exista el registro en MID_DATOS
                    stSQL = "SELECT COUNT(*) "
                    stSQL = stSQL + "FROM " + stDB + "." + stTabla
                    stSQL = stSQL + " WHERE ID_EMPLEADO = '" +  stEmpleado + "' "
                    stSQL = stSQL + " AND ANNO = " + str(vANNO)
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                    if regAux[0][0] == 0:
                        stSQL = "INSERT INTO " + stDB + "." + stTabla + "(ID_EMPLEADO,ANNO ) "
                        stSQL = stSQL + "VALUES('" + stEmpleado + "'," + str(vANNO) + ")"
                        myCursor.execute(stSQL)
                        
                    #4.5.- Recuperación de Datos Personales y del Puesto de Trabajo
                    #4.5.1.- Edad
                    auxEdad = vANNO - vSTD_DT_BIRTH.year
                    vEDAD = 'Joven'
                    if auxEdad > 30 and auxEdad <= 60 : vEDAD = 'Adulto'
                    if auxEdad > 60: vEDAD = 'Mayor'
                    
                    #4.5.2 Familia
                    vFAMILIA = 'Sin hijos'
                    
                    stSQL = "SELECT COUNT(*) "
                    stSQL = stSQL + " FROM " + stDB + ".M4CSP_FAM_EMPLEADOS "
                    stSQL = stSQL + " WHERE STD_ID_PERSON = '" + stEmpleado + "' "
                    stSQL = stSQL + " 	AND STD_ID_ACT_DEP_TYP = '2' "
                    stSQL = stSQL + " AND YEAR(STD_DT_BIRTH) < " + str(vANNO)
                    stSQL = stSQL + " 	AND " + str(vANNO) + " - YEAR(STD_DT_BIRTH) < 18 "
                     
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                    
                    vNUM_HIJOS = regAux[0][0] 
                    
                    if vNUM_HIJOS == 1 or vNUM_HIJOS == 2 : vFAMILIA = 'Con hijos'
                    if vNUM_HIJOS > 2: vFAMILIA = 'Familia numerosa'
                    
                    #4.5.3.- Grupo de Puesto
                    vPUESTO = 'S/D'
                    vGRUPO_PUESTO = 'S/D'
                    vAuxSBA = '0'
                    
                    stSQL = "SELECT  COUNT(*) "
                    stSQL = stSQL + " FROM " + stDB + ".M4SCO_H_HR_JOB A "
                    stSQL = stSQL + " LEFT JOIN " + stDB + ".STD_HT_JOB_DEF B ON "
                    stSQL = stSQL + "   (A.ID_ORGANIZATION = B.ID_ORGANIZATION AND B.STD_ID_JOB_CODE = A.SCO_ID_JOB_CODE "
                    stSQL = stSQL + "   and B.STD_DT_START <= A.SCO_DT_END AND B.STD_DT_END >= A.SCO_DT_START) "
                    stSQL = stSQL + " LEFT JOIN " + stDB + ".STD_LU_JOB_INT_CLA C ON (B.STD_ID_JOB_INT_CLA = C.STD_ID_JOB_INT_CLA) "
                    stSQL = stSQL + " LEFT JOIN " + stDB + ".STD_LU_JOB_INT_FAM D ON (C.STD_ID_JOB_INT_FAM = D.STD_ID_JOB_INT_FAM) "
                    stSQL = stSQL + " WHERE A.SCO_ID_HR = '" + stEmpleado + "' "
                    stSQL = stSQL + " AND YEAR(A.SCO_DT_START) <= " + str(vANNO)
                    stSQL = stSQL + " AND YEAR(A.SCO_DT_END) >= " + str(vANNO)
    
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                    
                    if regAux[0][0] == 1:
                        stSQL = "SELECT A.SCO_ID_JOB_CODE, ifnull(C.STD_ID_JOB_INT_FAM,'S/D'), ifnull(D.SCO_COMMENT , '-1') "
                        stSQL = stSQL + " FROM " + stDB + ".M4SCO_H_HR_JOB A "
                        stSQL = stSQL + " LEFT JOIN " + stDB + ".STD_HT_JOB_DEF B ON "
                        stSQL = stSQL + "   (A.ID_ORGANIZATION = B.ID_ORGANIZATION AND B.STD_ID_JOB_CODE = A.SCO_ID_JOB_CODE "
                        stSQL = stSQL + "   AND B.STD_DT_START <= A.SCO_DT_END AND B.STD_DT_END >= A.SCO_DT_START) "
                        stSQL = stSQL + " LEFT JOIN " + stDB + ".STD_LU_JOB_INT_CLA C ON (B.STD_ID_JOB_INT_CLA = C.STD_ID_JOB_INT_CLA) "
                        stSQL = stSQL + " LEFT JOIN " + stDB + ".STD_LU_JOB_INT_FAM D ON (C.STD_ID_JOB_INT_FAM = D.STD_ID_JOB_INT_FAM) "
                        stSQL = stSQL + " WHERE A.SCO_ID_HR = '" + stEmpleado + "' "
                        stSQL = stSQL + " AND A.SCO_DT_START = (SELECT MAX(SCO_DT_START) "
                        stSQL = stSQL + "       FROM " + stDB + ".M4SCO_H_HR_JOB "
                        stSQL = stSQL + "       WHERE SCO_ID_HR = '" + stEmpleado + "' "
                        stSQL = stSQL + "       AND YEAR(SCO_DT_START) <= " + str(vANNO)
                        stSQL = stSQL + "       AND YEAR(SCO_DT_END) >= " + str(vANNO) + ")"
                        
                        myCursor.execute(stSQL)
                        regAux = myCursor.fetchall()
                        
                        vPUESTO = regAux[0][0]
                        vGRUPO_PUESTO = regAux[0][1]
                        vAuxSBA = regAux[0][2]
                        
                        #Administrativo, Coordinador y Responsable.
                        if vGRUPO_PUESTO=='ADMIN' or vGRUPO_PUESTO=='SECRET' or vGRUPO_PUESTO=='TEC_ADMIN' or vGRUPO_PUESTO=='TECNICOS' or vGRUPO_PUESTO=='COND' or vGRUPO_PUESTO=='ALT' or vGRUPO_PUESTO=='ANALISTAS' or vGRUPO_PUESTO=='ASES_TECN' or vGRUPO_PUESTO=='AUX_ADMIN':
                            vGRUPO_PUESTO = 'Administrativo'
                            
                        if vGRUPO_PUESTO=='COMERCIAL' or vGRUPO_PUESTO=='COORD' or vGRUPO_PUESTO=='FJ0' or vGRUPO_PUESTO=='FJ1' or vGRUPO_PUESTO=='FJ2' or vGRUPO_PUESTO=='GER_COMER' or vGRUPO_PUESTO=='GES_COMER' or vGRUPO_PUESTO=='LETRADOS':
                            vGRUPO_PUESTO = 'Coordinador'
                            
                        if vGRUPO_PUESTO=='MB' or vGRUPO_PUESTO=='RA' or vGRUPO_PUESTO=='RD' or vGRUPO_PUESTO=='RDS' or vGRUPO_PUESTO=='RGC' or vGRUPO_PUESTO=='ROC' or vGRUPO_PUESTO=='RS' or vGRUPO_PUESTO=='RU':
                            vGRUPO_PUESTO = 'Responsable'
                            
                    #4.5.4.- Antigüedad
                    vANNO_ANTIGUEDAD = vANNO - vFEC_ANTIGUEDAD.year
                    vANTIGUEDAD = 'Poca'
                    if vANNO_ANTIGUEDAD > 3 and vANNO_ANTIGUEDAD <= 10: vANTIGUEDAD = 'Mediana'
                    if vANNO_ANTIGUEDAD > 10: vANTIGUEDAD = 'Mucha'
                    
                    #4.5.5.- SBA
                    if float(vAuxSBA) <=0:
                        stSQL = "SELECT COUNT(*) "
                        stSQL = stSQL + " FROM " + stDB + ".MID_DATOS "
                        stSQL = stSQL + " WHERE ID_EMPLEADO =  '" + stEmpleado + "' "
                        stSQL = stSQL + " AND ANNO + 1 = " + str(vANNO)
                        
                        myCursor.execute(stSQL)
                        regAux = myCursor.fetchall()
                        
                        if regAux[0][0] == 1:
                            stSQL = "SELECT GRUPO_PUESTO, SBA_IMPORTE "
                            stSQL = stSQL + " FROM " + stDB + ".MID_DATOS "
                            stSQL = stSQL + " WHERE ID_EMPLEADO = '" + stEmpleado + "' "
                            stSQL = stSQL + " AND ANNO + 1 = " + str(vANNO)
                            
                            myCursor.execute(stSQL)
                            regAux = myCursor.fetchall()
                            
                            vGRUPO_PUESTO = regAux[0][0]
                            vAuxSBA = str(regAux[0][1])
                        
                    if float(vAuxSBA) <=0: vSBA = 'S/D'
                    if float(vAuxSBA) > 0 and float(vAuxSBA) <= 20000: vSBA = 'Bajo'
                    if float(vAuxSBA) > 20000 and float(vAuxSBA) <= 30000: vSBA = 'Medio'
                    if float(vAuxSBA) > 30000 and float(vAuxSBA) <= 45000: vSBA = 'Bueno'
                    if float(vAuxSBA) > 45000: vSBA = 'Excelente'
                    
                    
                    
                    #4.5.6.- Nivel Educativo
                    vNIVEL_EDUCATIVO = 'S/D'
                    stSQL = "SELECT COUNT(*) "
                    stSQL = stSQL + " FROM " + stDB + ".STD_HR_ACAD_BACKGR A, " + stDB + ".STD_LU_EDU_TYPE B "
                    stSQL = stSQL + " 	WHERE A.STD_ID_EDU_TYPE = B.STD_ID_EDU_TYPE "
                    stSQL = stSQL + " 	AND A.STD_ID_HR = '" + stEmpleado + "' "
                    
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                        
                    if regAux[0][0] == 1:
                        stSQL = "SELECT STD_N_EDU_TYPEITA "
                        stSQL = stSQL + " FROM " + stDB + ".STD_HR_ACAD_BACKGR A, " + stDB + ".STD_LU_EDU_TYPE B "
                        stSQL = stSQL + " WHERE A.STD_ID_EDU_TYPE = B.STD_ID_EDU_TYPE "
                        stSQL = stSQL + " AND A.STD_ID_HR = '" + stEmpleado + "' "
                        
                        myCursor.execute(stSQL)
                        regAux = myCursor.fetchall()
                        
                        vNIVEL_EDUCATIVO = regAux[0][0]
                        
                    #4.5.7.- Formación
                    vFORMACION = 'Insuficiente'
                    if vANNO < 2016:
                        vNUM_CURSOS = 2
                    else:
                        stSQL = "SELECT COUNT(*) "
                        stSQL = stSQL + " FROM " + stDB + ".M4SCO_ENROLLMENT "
                        stSQL = stSQL + " WHERE STD_ID_PERSON = '" + stEmpleado + "' "
                        stSQL = stSQL + " AND YEAR(DT_END) = " + str(vANNO)
                        
                        myCursor.execute(stSQL)
                        regAux = myCursor.fetchall()
                        vNUM_CURSOS = regAux[0][0]
                        
                    if vNUM_CURSOS == 1 or vNUM_CURSOS == 2: vFORMACION = 'Suficiente'
                    if vNUM_CURSOS > 2 : vFORMACION = 'Demasiado'
                    
                    #4.6.- Recuperación de Desempeñio y EVP
                    #4.6.1.- Desempeño
                    vDESEMPENNO_NUM = 0
                    vDESEMPENNO = 'S/D'
                    stSQL = "SELECT COUNT(*) "
                    stSQL = stSQL + " FROM " + stDB + ".M4CSP_EVALUATOR "
                    stSQL = stSQL + " WHERE CSP_ID_HR = '" + stEmpleado + "' "
                    stSQL = stSQL + " AND YEAR(CSP_DT_START_EVAL) = " + str(vANNO)
                    
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                     
                    if regAux[0][0] == 1:
                        stSQL = "SELECT ifnull(CSP_EVAL_GLOBAL,'0') "
                        stSQL = stSQL + " FROM " + stDB + ".M4CSP_EVALUATOR "
                        stSQL = stSQL + " WHERE CSP_ID_HR = '" + stEmpleado + "' "
                        stSQL = stSQL + " AND YEAR(CSP_DT_START_EVAL) = " + str(vANNO)
                         
                        myCursor.execute(stSQL)
                        regAux = myCursor.fetchall()
                         
                        vDESEMPENNO_NUM = float(regAux[0][0])
                         
                    if vDESEMPENNO_NUM < 90 and vDESEMPENNO_NUM > 0: vDESEMPENNO = 'Bajo'
                    if vDESEMPENNO_NUM >= 90: vDESEMPENNO = 'Alto'
                    
                    #4.6.2.- EVP
                    #4.6.2.1.- Aprendizaje
                    vEVP_APRENDIZAJE = 0
                    if vNUM_CURSOS == 1: vEVP_APRENDIZAJE = 5
                    if vNUM_CURSOS >= 2: vEVP_APRENDIZAJE = 10
    
                    #4.6.2.2.- Plan de Carrera
                    vEVP_CARRERA = 0
                    stSQL = "SELECT COUNT(*) "
                    stSQL = stSQL + " FROM " + stDB + ".MID_DATOS " 
                    stSQL = stSQL + " WHERE ID_EMPLEADO = '" + stEmpleado + "' "
                    stSQL = stSQL + " AND ANNO < " + str(vANNO)
                    stSQL = stSQL + " 	AND PUESTO = '" + vPUESTO + "'"
                    
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                    
                    if regAux[0][0] <= 2: vEVP_CARRERA = 10
                    if regAux[0][0] > 2 and regAux[0][0] <= 5: vEVP_CARRERA = 5
                    
                    #4.6.2.3.- Equilibrio vida personal-profesional
                    vEVP_EQUILIBRIO  = 0
                    if auxEdad <= 30 and vNIVEL_EDUCATIVO == 'Sin Universidad': vEVP_EQUILIBRIO  = 10
                    if auxEdad > 30 and vNUM_HIJOS != 0: vEVP_EQUILIBRIO  = 10
                    
                    #4.6.2.4.- Retribución
                    vPORC_AUMENTO = 0
                    vEVP_RETRIBUCION = 0
                    vESTADO_ANTERIOR = 'S/D'
                    stSQL = "SELECT COUNT(*) "
                    stSQL = stSQL + " FROM " + stDB + ".MID_DATOS "
                    stSQL = stSQL + " 	WHERE ID_EMPLEADO = '" + stEmpleado + "' "
                    stSQL = stSQL + " 	AND ANNO + 1 = " + str(vANNO)
                    
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                    
                    if regAux[0][0] == 0: vPORC_AUMENTO = 100
                    if regAux[0][0] == 1:
                        stSQL = "SELECT SBA_IMPORTE, ESTADO "
                        stSQL = stSQL + " FROM " + stDB + ".MID_DATOS "
                        stSQL = stSQL + " WHERE ID_EMPLEADO = '" + stEmpleado + "' "
                        stSQL = stSQL + " AND ANNO + 1 = " + str(vANNO)
                        
                        myCursor.execute(stSQL)
                        regAux = myCursor.fetchall()
                        
                        if float(regAux[0][0]) > 0: 
                            vPORC_AUMENTO = 100 * (float(vAuxSBA) - float(regAux[0][0])) / float(regAux[0][0])
                        vESTADO_ANTERIOR = regAux[0][1]
                        
                    #4.6.2.5.- Descripción de Porcentaje de Aumento
                    if vPORC_AUMENTO <= 0: vDES_PORC_AUM = 'Descuento'
                    if vPORC_AUMENTO == 0: vDES_PORC_AUM = 'Sin Aumento'
                    if vPORC_AUMENTO > 0: vDES_PORC_AUM = 'Aumento'
                    
                    if vPORC_AUMENTO >=25: vEVP_RETRIBUCION = 10
                    if vPORC_AUMENTO >=10 and vPORC_AUMENTO < 25: vEVP_RETRIBUCION = 10
                    
                    vEVP_NUM = 0.27 * vEVP_APRENDIZAJE + 0.25 * vEVP_CARRERA + 0.25 * vEVP_EQUILIBRIO + 0.23 * vEVP_RETRIBUCION
                    vEVP = 'Bajo'
                    if vEVP_NUM > 5: vEVP = 'Alto'
                    
                    #4.7.- Estado para la cadena de Markov
                    vESTADO = 'S/D'
                    if vDESEMPENNO == 'Alto' and vEVP == 'Alto': vESTADO = 'DES+/EVP+'
                    if vDESEMPENNO == 'Alto' and vEVP == 'Bajo': vESTADO = 'DES+/EVP-'
                    if vDESEMPENNO == 'Bajo' and vEVP == 'Alto': vESTADO = 'DES-/EVP+'
                    if vDESEMPENNO == 'Bajo' and vEVP == 'Bajo': vESTADO = 'DES-/EVP-'
                    vES_ABANDONO = 0
                    vES_SEPARACION = 0
                    
                    #4.8.- Estados terminales de la cadena de Markov (si aplica)
                    if vANNO ==  vANNO_FIN and vSTD_ID_HRP_END_REA != 'S/D':
                      
                      stSQL = "SELECT COUNT(*) "
                      stSQL = stSQL + " FROM " + stDB + ".STD_HR_PERIOD "
                      stSQL = stSQL + " WHERE STD_ID_HR = '" + stEmpleado + "' " 
                      stSQL = stSQL + " AND STD_DT_START > '" + str(vDT_START) + "' "
                      
                      myCursor.execute(stSQL)
                      regAux = myCursor.fetchall()
                      
                      if regAux[0][0] == 0:
                          vESTADO = 'BAJA'
                          vES_ABANDONO = 1
                          if vSTD_ID_HRP_END_REA == '004' or vSTD_ID_HRP_END_REA == '009' or vSTD_ID_HRP_END_REA == '111' or vSTD_ID_HRP_END_REA == '54':
                              vES_ABANDONO = 0
                              vES_SEPARACION = 1
                              
                    #4.9.- Cálculo Separación
                    vSEPARACION = vANNO_ANTIGUEDAD * 22 * float(vAuxSBA)/365
                    if vSEPARACION > float(vAuxSBA) * 2: vSEPARACION = float(vAuxSBA) * 2
                    vSEPARACION = vSEPARACION + 3000 + float(vAuxSBA) / 2
                    
                    #4.10.- Cálculo Abandono
                    vABANDONO = 3000 + float(vAuxSBA) / 2
                    
                    #4.11.- Cálculo de Precios adicionales
                    vPRE_AUMENTO = float(vAuxSBA) * 0.20
                    vPRE_FORMACION = 3000
                    vPRE_PRUEBA = float(vAuxSBA) * 0.25
                    
                    #4.12.- Fin de Puesto
                    vFIN_PUESTO = 0
                    stSQL = "SELECT COUNT(*) "
                    stSQL = stSQL + " FROM " + stDB + ".STD_JOB "
                    stSQL = stSQL + " WHERE YEAR(STD_DT_END) = " + str(vANNO)
                    stSQL = stSQL + " AND STD_ID_JOB_CODE = '" + vPUESTO + "'"
                    
                    myCursor.execute(stSQL)
                    regAux = myCursor.fetchall()
                      
                    if regAux[0][0]  > 0: vFIN_PUESTO = 1
                    
                    
                    #4.99.- Se hace Update con los dato ya calculados
                    stSQL = "UPDATE " + stDB + "." + stTabla + " \n"
                    stSQL = stSQL + "SET  \n"
                    stSQL = stSQL + "   SEXO = '" + vSTD_ID_GENDER + "', \n"
                    stSQL = stSQL + "   ANNO_NAC = " + str(vSTD_DT_BIRTH.year) + ", \n"
                    stSQL = stSQL + "   EDAD = '" + vEDAD + "', \n"
                    stSQL = stSQL + "   NUM_HIJOS = " + str(vNUM_HIJOS) + ", \n"
                    stSQL = stSQL + "   FAMILIA = '" + vFAMILIA + "', \n"
                    stSQL = stSQL + "   PUESTO = '" + vPUESTO + "', \n"
                    stSQL = stSQL + "   GRUPO_PUESTO = '" + vGRUPO_PUESTO + "', \n"
                    stSQL = stSQL + "   ANNO_ANTIGUEDAD = " + str(vANNO_ANTIGUEDAD) + ", \n"
                    stSQL = stSQL + "   ANTIGUEDAD = '" + vANTIGUEDAD + "', \n"
                    stSQL = stSQL + "   SBA_IMPORTE = " + vAuxSBA + ", \n"
                    stSQL = stSQL + "   SBA = '" + vSBA + "', \n"
                    stSQL = stSQL + "   PORC_AUMENTO = " + str(vPORC_AUMENTO) + ", \n"
                    stSQL = stSQL + "   DES_PORC_AUM = '" + vDES_PORC_AUM + "', \n"
                    stSQL = stSQL + "   NIVEL_EDUCATIVO = '" + vNIVEL_EDUCATIVO + "', \n"
                    stSQL = stSQL + "   NUM_CURSOS = " + str(vNUM_CURSOS) + ", \n"
                    stSQL = stSQL + "   FORMACION = '" + vFORMACION + "', \n"
                    stSQL = stSQL + "   DESEMPENNO_NUM = " + str(vDESEMPENNO_NUM) + ", \n"
                    stSQL = stSQL + "   DESEMPENNO = '" + vDESEMPENNO + "', \n"
                    stSQL = stSQL + "   EVP_APRENDIZAJE = " + str(vEVP_APRENDIZAJE) + ", \n"
                    stSQL = stSQL + "   EVP_CARRERA = " + str(vEVP_CARRERA) + ", \n"
                    stSQL = stSQL + "   EVP_EQUILIBRIO = " + str(vEVP_EQUILIBRIO) + ", \n"
                    stSQL = stSQL + "   EVP_RETRIBUCION = " + str(vEVP_RETRIBUCION) + ", \n"
                    stSQL = stSQL + "   EVP_NUM = " + str(vEVP_NUM) + ", \n"
                    stSQL = stSQL + "   EVP = '" + vEVP + "', \n"
                    stSQL = stSQL + "   ESTADO = '" + vESTADO + "', \n"
                    stSQL = stSQL + "   ESTADO_ANTERIOR = '" + vESTADO_ANTERIOR + "', \n"
                    stSQL = stSQL + "   ES_SEPARACION = " + str(vES_SEPARACION) + ", \n"
                    stSQL = stSQL + "   ES_ABANDONO = " + str(vES_ABANDONO) + ", \n"
                    stSQL = stSQL + "   SEPARACION = " + str(vSEPARACION) + ", \n"
                    stSQL = stSQL + "   ABANDONO = " + str(vABANDONO) + ", \n"
                    stSQL = stSQL + "   PRE_AUMENTO = " + str(vPRE_AUMENTO) + ", \n"
                    stSQL = stSQL + "   PRE_FORMACION = " + str(vPRE_FORMACION) + ", \n"
                    stSQL = stSQL + "   PRE_PRUEBA = " + str(vPRE_PRUEBA) + ", \n"
                    stSQL = stSQL + "   FIN_PUESTO = " + str(vFIN_PUESTO) + " \n"
                    stSQL = stSQL + " WHERE ID_EMPLEADO = '" +  stEmpleado + "' \n"
                    stSQL = stSQL + " AND ANNO = " + str(vANNO)
                    myCursor.execute(stSQL)
                   
                    numRegistros = numRegistros + 1
                                        
                    myConexion.commit()
                    vANNO = vANNO + 1
  
    except Error as e:
        print("Error reading data from MySQL table", e)  
        myConexion.close()
        varResultado = -1        
                
    finally:
        pass
    
    #99.- Cierra conexiones
    myConexion.close()
    print("-> 99.- Fin Migración")
    return varResultado
