import petl as etl
import psycopg2
import numpy as np
import pymysql

#datos de conexion postgres
host_postgres = '127.0.0.1'
port_postgres = 5432
user_postgres = 'postgres'
password_postgres = '1234'
database_postgres = 'data-lake'

host_mysql = '127.0.0.1'
port_mysql = 3306
user_mysql = 'root'
password_mysql = '1234'
database_mysql = 'data-lake'


conexion = psycopg2.connect(database=database_postgres, 
                            user=user_postgres, password=password_postgres,
                            host=host_postgres, port=port_postgres)
tabla_postgres = etl.fromdb(conexion,"""SELECT * FROM public."indice-calidad-aire"
 """)
connection = pymysql.connect(database=database_mysql, 
                            user=user_mysql, password=password_mysql,
                            host=host_mysql, port=port_mysql)

connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
tabla_maria = etl.fromdb(connection,'  SELECT * FROM `data-lake`.`indice-calidad-aire` ')

tabla_noroeste_p = etl.cut(tabla_postgres,0,2,3,4,5,6)
tabla_noreste_p = etl.cut(tabla_postgres,0,7,8,9,10,11)
tabla_centro_p = etl.cut(tabla_postgres,0,12,13,14,15,16)
tabla_suroeste_p = etl.cut(tabla_postgres,0,17,18,19,20,21)
tabla_sureste_p = etl.cut(tabla_postgres,0,22,23,24,25,26)

tabla_noroeste_m = etl.cut(tabla_maria,0,2,3,4,5,6)
tabla_noreste_m = etl.cut(tabla_maria,0,7,8,9,10,11)
tabla_centro_m  = etl.cut(tabla_maria,0,12,13,14,15,16)
tabla_suroeste_m = etl.cut(tabla_maria,0,17,18,19,20,21)
tabla_sureste_m = etl.cut(tabla_maria,0,22,23,24,25,26)

tabla_noroeste = etl.cat(tabla_noroeste_p,tabla_noroeste_m)
tabla_noreste = etl.cat(tabla_noreste_p,tabla_noreste_m)
tabla_centro = etl.cat(tabla_centro_p,tabla_centro_m)
tabla_suroeste = etl.cat(tabla_suroeste_p,tabla_suroeste_m)
tabla_sureste = etl.cat(tabla_sureste_p,tabla_sureste_m)



#noroeste

tabla_noroeste = etl.select(tabla_noroeste,lambda tabla: tabla[0]!="")
tabla_noroeste = etl.replaceall(tabla_noroeste,"","0");
tabla_noroeste = etl.convertnumbers(tabla_noroeste)
tabla_noroeste = etl.addfield(tabla_noroeste ,'maximo',lambda rec: max(int(rec[1]),int(rec[2]),int(rec[3]),int(rec[4]),int(rec[5])))
tabla_noroeste = etl.cut(tabla_noroeste,0,'maximo')
maximo_indice_por_dia = []
for key, group in etl.rowgroupby(tabla_noroeste, 'Fecha'):
    maximo_indice_por_dia.append(max(list(group),key=lambda item:item[1]))
tabla_noroeste = np.array(maximo_indice_por_dia,dtype='U10 , i4')
tabla_noroeste = etl.fromarray(tabla_noroeste)
tabla_noroeste= etl.setheader(tabla_noroeste,['Fecha','Maximo'])
tabla_noroeste = etl.convertnumbers(tabla_noroeste)


#noreste

tabla_noreste = etl.select(tabla_noreste,lambda tabla: tabla[0]!="")
tabla_noreste = etl.replaceall(tabla_noreste,"","0");
tabla_noreste = etl.convertnumbers(tabla_noreste)
tabla_noreste = etl.addfield(tabla_noreste ,'maximo',lambda rec: max(int(rec[1]),int(rec[2]),int(rec[3]),int(rec[4]),int(rec[5])))
tabla_noreste = etl.cut(tabla_noreste,0,'maximo')
maximo_indice_por_dia = []
for key, group in etl.rowgroupby(tabla_noreste, 'Fecha'):
    maximo_indice_por_dia.append(max(list(group),key=lambda item:item[1]))
tabla_noreste = np.array(maximo_indice_por_dia,dtype='U10 , i4')
tabla_noreste = etl.fromarray(tabla_noreste)
tabla_noreste= etl.setheader(tabla_noreste,['Fecha','Maximo'])
tabla_noreste = etl.convertnumbers(tabla_noreste)

#centro

tabla_centro = etl.select(tabla_centro,lambda tabla: tabla[0]!="")
tabla_centro = etl.replaceall(tabla_centro,"","0");
tabla_centro = etl.convertnumbers(tabla_centro)
tabla_centro = etl.addfield(tabla_centro ,'maximo',lambda rec: max(int(rec[1]),int(rec[2]),int(rec[3]),int(rec[4]),int(rec[5])))
tabla_centro = etl.cut(tabla_centro,0,'maximo')
maximo_indice_por_dia = []
for key, group in etl.rowgroupby(tabla_centro, 'Fecha'):
    maximo_indice_por_dia.append(max(list(group),key=lambda item:item[1]))
tabla_centro = np.array(maximo_indice_por_dia,dtype='U10 , i4')
tabla_centro = etl.fromarray(tabla_centro)
tabla_centro= etl.setheader(tabla_centro,['Fecha','Maximo'])
tabla_centro = etl.convertnumbers(tabla_centro)


#Suroeste

tabla_suroeste = etl.select(tabla_suroeste,lambda tabla: tabla[0]!="")
tabla_suroeste = etl.replaceall(tabla_suroeste,"","0");
tabla_suroeste = etl.convertnumbers(tabla_suroeste)
tabla_suroeste = etl.addfield(tabla_suroeste ,'maximo',lambda rec: max(int(rec[1]),int(rec[2]),int(rec[3]),int(rec[4]),int(rec[5])))
tabla_suroeste = etl.cut(tabla_suroeste,0,'maximo')
maximo_indice_por_dia = []
for key, group in etl.rowgroupby(tabla_suroeste, 'Fecha'):
    maximo_indice_por_dia.append(max(list(group),key=lambda item:item[1]))
tabla_suroeste = np.array(maximo_indice_por_dia,dtype='U10 , i4')
tabla_suroeste = etl.fromarray(tabla_suroeste)
tabla_suroeste= etl.setheader(tabla_suroeste,['Fecha','Maximo'])
tabla_suroeste = etl.convertnumbers(tabla_suroeste)

#Sureste

tabla_sureste = etl.select(tabla_sureste,lambda tabla: tabla[0]!="")
tabla_sureste = etl.replaceall(tabla_sureste,"","0");
tabla_sureste = etl.convertnumbers(tabla_sureste)
tabla_sureste = etl.addfield(tabla_sureste ,'maximo',lambda rec: max(int(rec[1]),int(rec[2]),int(rec[3]),int(rec[4]),int(rec[5])))
tabla_sureste = etl.cut(tabla_sureste,0,'maximo')
maximo_indice_por_dia = []
for key, group in etl.rowgroupby(tabla_sureste, 'Fecha'):
    maximo_indice_por_dia.append(max(list(group),key=lambda item:item[1]))
tabla_sureste = np.array(maximo_indice_por_dia,dtype='U10 , i4')
tabla_sureste = etl.fromarray(tabla_sureste)
tabla_sureste= etl.setheader(tabla_sureste,['Fecha','Maximo'])
tabla_sureste = etl.convertnumbers(tabla_sureste)

"""
tabla_noreste_fecha_validas = etl.select(tabla_noroeste,lambda tabla: tabla[0]!="")
tabla_noreste_fill_ceros = etl.replaceall(tabla_noreste_fecha_validas,"","0");
tabla_noroeste_entero = etl.convertnumbers(tabla_noreste_fill_ceros)
tabla_noroeste_maximo = etl.addfield(tabla_noroeste_entero ,'maximo',lambda rec: max(int(rec[1]),int(rec[2]),int(rec[3]),int(rec[4]),int(rec[5])))
tabla_noroeste_fecha_maximo = etl.cut(tabla_noroeste_maximo,0,'maximo')
maximo_indice_por_dia = []
for key, group in etl.rowgroupby(tabla_noroeste_fecha_maximo, 'Fecha'):
    maximo_indice_por_dia.append(max(list(group),key=lambda item:item[1]))
tabla_noroeste = np.array(maximo_indice_por_dia,dtype='U10 , i4')
tabla_noroeste = etl.fromarray(tabla_noroeste)
tabla_noroeste= etl.setheader(tabla_noroeste,['Fecha','Maximo'])
tabla_noroeste = etl.convertnumbers(tabla_noroeste)
"""
#


conexion = psycopg2.connect(database='datawarehouse', 
                            user=user_postgres, password=password_postgres,
                            host=host_postgres, port=port_postgres)
    
etl.todb(tabla_noroeste , conexion, 'Noroeste',create=True,constraints=False,drop=True)
etl.todb(tabla_noreste , conexion, 'Noreste',create=True,constraints=False,drop=True)
etl.todb(tabla_centro , conexion, 'Centro',create=True,constraints=False,drop=True)
etl.todb(tabla_suroeste , conexion, 'Suroeste',create=True,constraints=False,drop=True)
etl.todb(tabla_sureste , conexion, 'Sureste',create=True,constraints=True)



