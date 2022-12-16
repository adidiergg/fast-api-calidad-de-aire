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


#datos de conexion mysql mariadb
host_mysql = '127.0.0.1'
port_mysql = 3306
user_mysql = 'root'
password_mysql = '1234'
database_mysql = 'data-lake'


tabla0 = etl.fromcsv("data-lake/indice_2010.CSV",encoding='latin-1',delimiter=",")
tabla0 = etl.skip(tabla0,8)
tabla1 = etl.fromcsv("data-lake/indice_2011.CSV",encoding='latin-1',delimiter=",")
tabla1 = etl.skip(tabla1,8)
tabla2 = etl.fromcsv("data-lake/indice_2012.CSV",encoding='latin-1',delimiter=",")
tabla2 = etl.skip(tabla2,8)
tabla3 = etl.fromcsv("data-lake/indice_2013.CSV",encoding='latin-1',delimiter=",")
tabla3 = etl.skip(tabla3,8)
tabla4 = etl.fromcsv("data-lake/indice_2014.CSV",encoding='latin-1',delimiter=",")
tabla4 = etl.skip(tabla4,8)
tabla5 = etl.fromcsv("data-lake/indice_2015.CSV",encoding='latin-1',delimiter=",")
tabla5 = etl.skip(tabla5,8)
tabla_union_postgres =  etl.cat(tabla0,tabla1,tabla2,tabla3,tabla4,tabla5)
tabla_postgres = etl.cutout(tabla_union_postgres, 27,28,29,30,31)


conexion = psycopg2.connect(database=database_postgres, 
                            user=user_postgres, password=password_postgres,
                            host=host_postgres, port=port_postgres)

print(etl.typecounts(tabla_postgres,0))

etl.todb(tabla_postgres, conexion, 'indice-calidad-aire',create=True,constraints=False,drop=True)




tabla6 = etl.fromcsv("data-lake/indice_2016.CSV",encoding='latin-1',delimiter=",")
tabla6 = etl.skip(tabla6,8)
tabla7 = etl.fromcsv("data-lake/indice_2017.CSV",encoding='latin-1',delimiter=",")
tabla7 = etl.skip(tabla7,8)
tabla8 = etl.fromcsv("data-lake/indice_2018.CSV",encoding='latin-1',delimiter=",")
tabla8 = etl.skip(tabla8,8)
tabla9 = etl.fromcsv("data-lake/indice_2019.CSV",encoding='latin-1',delimiter=",")
tabla9 = etl.skip(tabla9,8)
tabla10 = etl.fromcsv("data-lake/indice_2020.CSV",encoding='latin-1',delimiter=",")
tabla10 = etl.skip(tabla10,8)
tabla11 = etl.fromcsv("data-lake/indice_2021.CSV",encoding='latin-1',delimiter=",")
tabla11 = etl.skip(tabla11,8)



tabla_union_maria =  etl.cat(tabla6,tabla7,tabla8,tabla9,tabla10,tabla11)
print(tabla_union_maria)
tabla_maria = etl.cutout(tabla_union_maria,27,28,29,30,31,32,33,34,35,36,37,38)


#tabla_noreste_fill_ceros = etl.replaceall(tabla_maria,NoneType,"");
#tabla_noreste_fill_ceros = etl.replaceall(tabla_maria,object,"");

print(etl.typecounts(tabla_maria,5))

print(tabla_maria)


connection = pymysql.connect(database=database_mysql, 
                            user=user_mysql, password=password_mysql,
                            host=host_mysql, port=port_mysql)

connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')

etl.todb(tabla_maria, connection, 'indice-calidad-aire',create=True,drop=True)


"""
etl.tojson("ppp.json",tabla_noreste_fill_ceros)



etl.todb(tabla_noreste_fill_ceros, conexion, 'indice-calidad-aire',create=True,constraints=False,drop=True)


connection = pymysql.connect(database=tabla_maria, 
                            user=user_mysql, password=password_mysql,
                            host=host_mysql, port=port_mysql)

connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')

etl.todb(tabla_maria, connection, 'indice-calidad-aire',create=True,drop=True)
"""


#etl.tojson("ppp.json",tabla_postgres)
#print(etl.typecounts(tabla_postgres,'Hora'))
