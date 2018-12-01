# -*- coding: utf-8 -*-
"""
UNIVERSIDAD NACIONAL DE EDUCACIÓN A DISTANCIA

Master Inteligencia Artificial Avanzada

Trabajo de Fin de Master
Esperimento prueba algoritmos corrección errores tipo I y II

@author: Sergio Montes Vázquez
"""

from 01_Generador_Casuisticas import *
from 02_Correccion_Historicos import *
from 03_Visualizacion_Historicos import *

stHost =  'localhost'
stUser = 'root'
stPassword = 'xxxxxxxxxx'
stDB = 'tfm_smv'
numSimulaciones = 100
NumEjemplos = 50
fichero = open ( 'Resultadoa.csv', 'w+' )
auxLinea =  "Simulación;Ejemplo;Tabla;Id. Error;Corregido;Dias;Completo\n"
fichero.write(auxLinea)
for i in range(0,numSimulaciones):
    try:
        CreaRegistrosIniciles(stHost,stUser,stPassword,stDB)
        CreaRegistrosErrorTipoI(stHost,stUser,stPassword,stDB,NumEjemplos)
        CreaRegistrosErrorTipoII(stHost,stUser,stPassword,stDB,NumEjemplos)

        correccionErrorTipoI(stHost,stUser,stPassword,stDB)
        correccionErrorTipoII(stHost,stUser,stPassword,stDB)

        vecAux = ConjuntoDatos(stHost,stUser,stPassword,stDB)
		
        if len(vecAux)>0:
            for j in range(0,len(vecAux)):
                auxLinea = str(i) + ";"
                auxLinea = auxLinea + str(j) + ";"
                auxLinea = auxLinea + vecAux[j][0] + ";"
                auxLinea = auxLinea + str(vecAux[j][1]) + ";"
                auxLinea = auxLinea + str(vecAux[j][2]) + ";"
                auxLinea = auxLinea + str(vecAux[j][3]) + ";"
                auxLinea = auxLinea + str(vecAux[j][4]) + ";"
                auxLinea = auxLinea + "\n"
                fichero.write(auxLinea)
    finally:
        pass

fichero.close() 
