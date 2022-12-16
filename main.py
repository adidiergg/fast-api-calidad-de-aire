from fastapi import FastAPI,HTTPException
import psycopg2
from fastapi import FastAPI
import json
import pymysql
import psycopg2.extensions as ext
import numpy as np
import petl as etl

ext.string_types.pop(ext.JSON.values[0], None)
from fastapi.middleware.cors import CORSMiddleware
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

app = FastAPI()

origins = [
    "http://127.0.0.1:3000"
    "http://127.0.0.1:8000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)





@app.get('/status/limpieza')
async def getStatusLimpieza():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        cursor = conn.cursor()
        cursor.execute('  SELECT * FROM public."Centro" ')
        Centro = cursor.fetchall()
        cursor.execute('  SELECT * FROM public."Noreste" ')
        Noreste = cursor.fetchall()
        cursor.execute('  SELECT * FROM public."Noroeste" ')
        Noroeste = cursor.fetchall()
        cursor.execute('  SELECT * FROM public."Sureste" ')
        Sureste = cursor.fetchall()
        cursor.execute('  SELECT * FROM public."Suroeste" ')
        Suroeste = cursor.fetchall()
        
        if len(Centro)==0 or len(Noreste)==0 or len(Noroeste)==0  or len(Sureste)==0 or len(Suroeste)==0 :
            return {'status_code':200,'data':"false"}
        else:
            return {'status_code':200,'data':"true"}
    except:
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")




@app.get('/datawarehouse/centro')
async def getDataWareHouseCentro():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        cursor = conn.cursor()
        cursor.execute('  SELECT * FROM public."Centro" ')
        datos = cursor.fetchall()
        
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
     
       
        if len(datos)!=0:
            resultado = []
            for p,i in enumerate(datos):
                resultado.append({'id':p ,
                '0':i[0],
                '1':i[1], 
                })
                
                
            conn.close()
            return {'status_code':200,'data':resultado}
                    
                    
        else:
            conn.close()
            return HTTPException(status_code=404, detail="no se encontro el recurso")
    except:
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")




@app.get('/resultado/centro')
async def getResultadoCentro():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        datos = []
        cursor = conn.cursor()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2010';  """)
        rs2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2011';  """)
        rs2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2012';  """)
        rs2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2013';  """)
        rs2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2014';  """)
        rs2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2015';  """)
        rs2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2016';  """)
        rs2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2017';  """)
        rs2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2018';  """)
        rs2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2019';  """)
        rs2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2020';  """)
        rs2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2021';  """)
        rs2021 = cursor.fetchone()
        
        
        
        
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2010';  """)
        rl2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2011';  """)
        rl2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2012';  """)
        rl2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2013';  """)
        rl2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2014';  """)
        rl2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2015';  """)
        rl2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2016';  """)
        rl2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2017';  """)
        rl2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2018';  """)
        rl2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2019';  """)
        rl2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2020';  """)
        rl2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2021';  """)
        rl2021 = cursor.fetchone()
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
        resultado = []
        resultado.append(['Año','Días sucios','Días limpios'])
        resultado.append(['2010',rs2010[0],rl2010[0]])
        resultado.append(['2011',rs2011[0],rl2011[0]])
        resultado.append(['2012',rs2012[0],rl2012[0]])
        resultado.append(['2013',rs2013[0],rl2013[0]])
        resultado.append(['2014',rs2014[0],rl2014[0]])
        resultado.append(['2015',rs2015[0],rl2015[0]])
        resultado.append(['2016',rs2016[0],rl2016[0]])
        resultado.append(['2017',rs2017[0],rl2017[0]])
        resultado.append(['2018',rs2018[0],rl2018[0]])
        resultado.append(['2019',rs2019[0],rl2019[0]])
        resultado.append(['2020',rs2020[0],rl2020[0]])
        resultado.append(['2021',rs2021[0],rl2021[0]])
        
        return {'status_code':200,'data':resultado}
    except Exception as e:
        print(e)
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")


@app.get('/resultado/noreste')
async def getResultadoNoreste():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        datos = []
        cursor = conn.cursor()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2010';  """)
        rs2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2011';  """)
        rs2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2012';  """)
        rs2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2013';  """)
        rs2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2014';  """)
        rs2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2015';  """)
        rs2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2016';  """)
        rs2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2017';  """)
        rs2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2018';  """)
        rs2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2019';  """)
        rs2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2020';  """)
        rs2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2021';  """)
        rs2021 = cursor.fetchone()
        
        
        
        
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2010';  """)
        rl2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2011';  """)
        rl2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2012';  """)
        rl2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2013';  """)
        rl2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2014';  """)
        rl2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2015';  """)
        rl2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2016';  """)
        rl2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2017';  """)
        rl2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2018';  """)
        rl2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2019';  """)
        rl2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2020';  """)
        rl2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2021';  """)
        rl2021 = cursor.fetchone()
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
        resultado = []
        resultado.append(['Año','Días sucios','Días limpios'])
        resultado.append(['2010',rs2010[0],rl2010[0]])
        resultado.append(['2011',rs2011[0],rl2011[0]])
        resultado.append(['2012',rs2012[0],rl2012[0]])
        resultado.append(['2013',rs2013[0],rl2013[0]])
        resultado.append(['2014',rs2014[0],rl2014[0]])
        resultado.append(['2015',rs2015[0],rl2015[0]])
        resultado.append(['2016',rs2016[0],rl2016[0]])
        resultado.append(['2017',rs2017[0],rl2017[0]])
        resultado.append(['2018',rs2018[0],rl2018[0]])
        resultado.append(['2019',rs2019[0],rl2019[0]])
        resultado.append(['2020',rs2020[0],rl2020[0]])
        resultado.append(['2021',rs2021[0],rl2021[0]])
        
        return {'status_code':200,'data':resultado}
    except Exception as e:
        print(e)
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")
    
    
@app.get('/resultado/noroeste')
async def getResultadoNoroeste():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        datos = []
        cursor = conn.cursor()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2010';  """)
        rs2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2011';  """)
        rs2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2012';  """)
        rs2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2013';  """)
        rs2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2014';  """)
        rs2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2015';  """)
        rs2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2016';  """)
        rs2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2017';  """)
        rs2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2018';  """)
        rs2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2019';  """)
        rs2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2020';  """)
        rs2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2021';  """)
        rs2021 = cursor.fetchone()
        
        
        
        
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2010';  """)
        rl2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2011';  """)
        rl2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2012';  """)
        rl2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2013';  """)
        rl2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2014';  """)
        rl2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2015';  """)
        rl2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2016';  """)
        rl2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2017';  """)
        rl2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2018';  """)
        rl2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2019';  """)
        rl2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2020';  """)
        rl2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2021';  """)
        rl2021 = cursor.fetchone()
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
        resultado = []
        resultado.append(['Año','Días sucios','Días limpios'])
        resultado.append(['2010',rs2010[0],rl2010[0]])
        resultado.append(['2011',rs2011[0],rl2011[0]])
        resultado.append(['2012',rs2012[0],rl2012[0]])
        resultado.append(['2013',rs2013[0],rl2013[0]])
        resultado.append(['2014',rs2014[0],rl2014[0]])
        resultado.append(['2015',rs2015[0],rl2015[0]])
        resultado.append(['2016',rs2016[0],rl2016[0]])
        resultado.append(['2017',rs2017[0],rl2017[0]])
        resultado.append(['2018',rs2018[0],rl2018[0]])
        resultado.append(['2019',rs2019[0],rl2019[0]])
        resultado.append(['2020',rs2020[0],rl2020[0]])
        resultado.append(['2021',rs2021[0],rl2021[0]])
        
        return {'status_code':200,'data':resultado}
    except Exception as e:
        print(e)
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")
   
   
@app.get('/resultado/sureste')
async def getResultadoSureste():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        datos = []
        cursor = conn.cursor()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2010';  """)
        rs2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2011';  """)
        rs2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2012';  """)
        rs2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2013';  """)
        rs2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2014';  """)
        rs2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2015';  """)
        rs2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2016';  """)
        rs2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2017';  """)
        rs2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2018';  """)
        rs2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2019';  """)
        rs2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2020';  """)
        rs2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2021';  """)
        rs2021 = cursor.fetchone()
        
        
        
        
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2010';  """)
        rl2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2011';  """)
        rl2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2012';  """)
        rl2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2013';  """)
        rl2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2014';  """)
        rl2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2015';  """)
        rl2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2016';  """)
        rl2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2017';  """)
        rl2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2018';  """)
        rl2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2019';  """)
        rl2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2020';  """)
        rl2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2021';  """)
        rl2021 = cursor.fetchone()
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
        resultado = []
        resultado.append(['Año','Días sucios','Días limpios'])
        resultado.append(['2010',rs2010[0],rl2010[0]])
        resultado.append(['2011',rs2011[0],rl2011[0]])
        resultado.append(['2012',rs2012[0],rl2012[0]])
        resultado.append(['2013',rs2013[0],rl2013[0]])
        resultado.append(['2014',rs2014[0],rl2014[0]])
        resultado.append(['2015',rs2015[0],rl2015[0]])
        resultado.append(['2016',rs2016[0],rl2016[0]])
        resultado.append(['2017',rs2017[0],rl2017[0]])
        resultado.append(['2018',rs2018[0],rl2018[0]])
        resultado.append(['2019',rs2019[0],rl2019[0]])
        resultado.append(['2020',rs2020[0],rl2020[0]])
        resultado.append(['2021',rs2021[0],rl2021[0]])
        
        return {'status_code':200,'data':resultado}
    except Exception as e:
        print(e)
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos") 
    
    
@app.get('/resultado/suroeste')
async def getResultadoSuroeste():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        datos = []
        cursor = conn.cursor()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2010';  """)
        rs2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2011';  """)
        rs2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2012';  """)
        rs2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2013';  """)
        rs2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2014';  """)
        rs2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2015';  """)
        rs2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2016';  """)
        rs2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2017';  """)
        rs2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2018';  """)
        rs2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2019';  """)
        rs2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2020';  """)
        rs2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2021';  """)
        rs2021 = cursor.fetchone()
        
        
        
        
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2010';  """)
        rl2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2011';  """)
        rl2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2012';  """)
        rl2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2013';  """)
        rl2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2014';  """)
        rl2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2015';  """)
        rl2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2016';  """)
        rl2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2017';  """)
        rl2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2018';  """)
        rl2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2019';  """)
        rl2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2020';  """)
        rl2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2021';  """)
        rl2021 = cursor.fetchone()
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
        resultado = []
        resultado.append(['Año','Días sucios','Días limpios'])
        resultado.append(['2010',rs2010[0],rl2010[0]])
        resultado.append(['2011',rs2011[0],rl2011[0]])
        resultado.append(['2012',rs2012[0],rl2012[0]])
        resultado.append(['2013',rs2013[0],rl2013[0]])
        resultado.append(['2014',rs2014[0],rl2014[0]])
        resultado.append(['2015',rs2015[0],rl2015[0]])
        resultado.append(['2016',rs2016[0],rl2016[0]])
        resultado.append(['2017',rs2017[0],rl2017[0]])
        resultado.append(['2018',rs2018[0],rl2018[0]])
        resultado.append(['2019',rs2019[0],rl2019[0]])
        resultado.append(['2020',rs2020[0],rl2020[0]])
        resultado.append(['2021',rs2021[0],rl2021[0]])
        
        
        return {'status_code':200,'data':resultado}
    except Exception as e:
        print(e)
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")  
    
@app.get('/estadistica/centro')
async def getEstadisticaCentro():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        datos = []
        cursor = conn.cursor()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2010';  """)
        rs2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2011';  """)
        rs2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2012';  """)
        rs2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2013';  """)
        rs2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2014';  """)
        rs2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2015';  """)
        rs2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2016';  """)
        rs2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2017';  """)
        rs2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2018';  """)
        rs2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2019';  """)
        rs2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2020';  """)
        rs2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo">101 and  "Fecha" like '%2021';  """)
        rs2021 = cursor.fetchone()
        
        
        
        
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2010';  """)
        rl2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2011';  """)
        rl2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2012';  """)
        rl2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2013';  """)
        rl2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2014';  """)
        rl2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2015';  """)
        rl2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2016';  """)
        rl2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2017';  """)
        rl2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2018';  """)
        rl2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2019';  """)
        rl2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2020';  """)
        rl2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Centro" where "Maximo"<101 and  "Fecha" like '%2021';  """)
        rl2021 = cursor.fetchone()
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
        resultado = []
        resultado.append({'id':0,'0':'2010','1':rs2010[0],'2':rl2010[0]})
        resultado.append({'id':1,'0':'2011','1':rs2011[0],'2':rl2011[0]})
        resultado.append({'id':2,'0':'2012','1':rs2012[0],'2':rl2012[0]})
        resultado.append({'id':3,'0':'2013','1':rs2013[0],'2':rl2013[0]})
        resultado.append({'id':4,'0':'2014','1':rs2014[0],'2':rl2014[0]})
        resultado.append({'id':5,'0':'2015','1':rs2015[0],'2':rl2015[0]})
        resultado.append({'id':6,'0':'2016','1':rs2016[0],'2':rl2016[0]})
        resultado.append({'id':7,'0':'2017','1':rs2017[0],'2':rl2017[0]})
        resultado.append({'id':8,'0':'2018','1':rs2018[0],'2':rl2018[0]})
        resultado.append({'id':9,'0':'2019','1':rs2019[0],'2':rl2019[0]})
        resultado.append({'id':10,'0':'2020','1':rs2020[0],'2':rl2020[0]})
        resultado.append({'id':11,'0':'2021','1':rs2021[0],'2':rl2021[0]})
        
        return {'status_code':200,'data':resultado}
    except Exception as e:
        print(e)
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")
    
    
@app.get('/estadistica/noreste')
async def getEstadisticaNoreste():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        datos = []
        cursor = conn.cursor()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2010';  """)
        rs2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2011';  """)
        rs2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2012';  """)
        rs2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2013';  """)
        rs2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2014';  """)
        rs2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2015';  """)
        rs2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2016';  """)
        rs2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2017';  """)
        rs2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2018';  """)
        rs2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2019';  """)
        rs2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2020';  """)
        rs2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo">101 and  "Fecha" like '%2021';  """)
        rs2021 = cursor.fetchone()
        
        
        
        
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2010';  """)
        rl2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2011';  """)
        rl2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2012';  """)
        rl2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2013';  """)
        rl2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2014';  """)
        rl2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2015';  """)
        rl2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2016';  """)
        rl2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2017';  """)
        rl2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2018';  """)
        rl2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2019';  """)
        rl2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2020';  """)
        rl2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noreste" where "Maximo"<101 and  "Fecha" like '%2021';  """)
        rl2021 = cursor.fetchone()
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
        resultado = []
        resultado.append({'id':0,'0':'2010','1':rs2010[0],'2':rl2010[0]})
        resultado.append({'id':1,'0':'2011','1':rs2011[0],'2':rl2011[0]})
        resultado.append({'id':2,'0':'2012','1':rs2012[0],'2':rl2012[0]})
        resultado.append({'id':3,'0':'2013','1':rs2013[0],'2':rl2013[0]})
        resultado.append({'id':4,'0':'2014','1':rs2014[0],'2':rl2014[0]})
        resultado.append({'id':5,'0':'2015','1':rs2015[0],'2':rl2015[0]})
        resultado.append({'id':6,'0':'2016','1':rs2016[0],'2':rl2016[0]})
        resultado.append({'id':7,'0':'2017','1':rs2017[0],'2':rl2017[0]})
        resultado.append({'id':8,'0':'2018','1':rs2018[0],'2':rl2018[0]})
        resultado.append({'id':9,'0':'2019','1':rs2019[0],'2':rl2019[0]})
        resultado.append({'id':10,'0':'2020','1':rs2020[0],'2':rl2020[0]})
        resultado.append({'id':11,'0':'2021','1':rs2021[0],'2':rl2021[0]})
        
        return {'status_code':200,'data':resultado}
    except Exception as e:
        print(e)
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")
    
    
    
@app.get('/estadistica/noroeste')
async def getEstadisticaNoroeste():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        datos = []
        cursor = conn.cursor()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2010';  """)
        rs2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2011';  """)
        rs2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2012';  """)
        rs2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2013';  """)
        rs2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2014';  """)
        rs2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2015';  """)
        rs2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2016';  """)
        rs2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2017';  """)
        rs2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2018';  """)
        rs2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2019';  """)
        rs2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2020';  """)
        rs2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo">101 and  "Fecha" like '%2021';  """)
        rs2021 = cursor.fetchone()
        
        
        
        
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2010';  """)
        rl2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2011';  """)
        rl2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2012';  """)
        rl2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2013';  """)
        rl2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2014';  """)
        rl2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2015';  """)
        rl2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2016';  """)
        rl2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2017';  """)
        rl2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2018';  """)
        rl2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2019';  """)
        rl2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2020';  """)
        rl2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Noroeste" where "Maximo"<101 and  "Fecha" like '%2021';  """)
        rl2021 = cursor.fetchone()
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
        resultado = []
        resultado.append({'id':0,'0':'2010','1':rs2010[0],'2':rl2010[0]})
        resultado.append({'id':1,'0':'2011','1':rs2011[0],'2':rl2011[0]})
        resultado.append({'id':2,'0':'2012','1':rs2012[0],'2':rl2012[0]})
        resultado.append({'id':3,'0':'2013','1':rs2013[0],'2':rl2013[0]})
        resultado.append({'id':4,'0':'2014','1':rs2014[0],'2':rl2014[0]})
        resultado.append({'id':5,'0':'2015','1':rs2015[0],'2':rl2015[0]})
        resultado.append({'id':6,'0':'2016','1':rs2016[0],'2':rl2016[0]})
        resultado.append({'id':7,'0':'2017','1':rs2017[0],'2':rl2017[0]})
        resultado.append({'id':8,'0':'2018','1':rs2018[0],'2':rl2018[0]})
        resultado.append({'id':9,'0':'2019','1':rs2019[0],'2':rl2019[0]})
        resultado.append({'id':10,'0':'2020','1':rs2020[0],'2':rl2020[0]})
        resultado.append({'id':11,'0':'2021','1':rs2021[0],'2':rl2021[0]})
        
        return {'status_code':200,'data':resultado}
    except Exception as e:
        print(e)
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")
    
    
    
@app.get('/estadistica/sureste')
async def getEstadisticaSureste():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        datos = []
        cursor = conn.cursor()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2010';  """)
        rs2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2011';  """)
        rs2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2012';  """)
        rs2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2013';  """)
        rs2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2014';  """)
        rs2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2015';  """)
        rs2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2016';  """)
        rs2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2017';  """)
        rs2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2018';  """)
        rs2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2019';  """)
        rs2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2020';  """)
        rs2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo">101 and  "Fecha" like '%2021';  """)
        rs2021 = cursor.fetchone()
        
        
        
        
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2010';  """)
        rl2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2011';  """)
        rl2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2012';  """)
        rl2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2013';  """)
        rl2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2014';  """)
        rl2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2015';  """)
        rl2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2016';  """)
        rl2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2017';  """)
        rl2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2018';  """)
        rl2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2019';  """)
        rl2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2020';  """)
        rl2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Sureste" where "Maximo"<101 and  "Fecha" like '%2021';  """)
        rl2021 = cursor.fetchone()
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
        resultado = []
        resultado.append({'id':0,'0':'2010','1':rs2010[0],'2':rl2010[0]})
        resultado.append({'id':1,'0':'2011','1':rs2011[0],'2':rl2011[0]})
        resultado.append({'id':2,'0':'2012','1':rs2012[0],'2':rl2012[0]})
        resultado.append({'id':3,'0':'2013','1':rs2013[0],'2':rl2013[0]})
        resultado.append({'id':4,'0':'2014','1':rs2014[0],'2':rl2014[0]})
        resultado.append({'id':5,'0':'2015','1':rs2015[0],'2':rl2015[0]})
        resultado.append({'id':6,'0':'2016','1':rs2016[0],'2':rl2016[0]})
        resultado.append({'id':7,'0':'2017','1':rs2017[0],'2':rl2017[0]})
        resultado.append({'id':8,'0':'2018','1':rs2018[0],'2':rl2018[0]})
        resultado.append({'id':9,'0':'2019','1':rs2019[0],'2':rl2019[0]})
        resultado.append({'id':10,'0':'2020','1':rs2020[0],'2':rl2020[0]})
        resultado.append({'id':11,'0':'2021','1':rs2021[0],'2':rl2021[0]})
        
        return {'status_code':200,'data':resultado}
    except Exception as e:
        print(e)
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")
    

@app.get('/estadistica/suroeste')
async def getEstadisticaSuroeste():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        datos = []
        cursor = conn.cursor()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2010';  """)
        rs2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2011';  """)
        rs2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2012';  """)
        rs2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2013';  """)
        rs2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2014';  """)
        rs2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2015';  """)
        rs2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2016';  """)
        rs2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2017';  """)
        rs2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2018';  """)
        rs2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2019';  """)
        rs2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2020';  """)
        rs2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo">101 and  "Fecha" like '%2021';  """)
        rs2021 = cursor.fetchone()
        
        
        
        
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2010';  """)
        rl2010 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2011';  """)
        rl2011 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2012';  """)
        rl2012 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2013';  """)
        rl2013 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2014';  """)
        rl2014 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2015';  """)
        rl2015 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2016';  """)
        rl2016 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2017';  """)
        rl2017 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2018';  """)
        rl2018 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2019';  """)
        rl2019 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2020';  """)
        rl2020 = cursor.fetchone()
        cursor.execute(""" select count(*) from public."Suroeste" where "Maximo"<101 and  "Fecha" like '%2021';  """)
        rl2021 = cursor.fetchone()
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
        resultado = []
        resultado.append({'id':0,'0':'2010','1':rs2010[0],'2':rl2010[0]})
        resultado.append({'id':1,'0':'2011','1':rs2011[0],'2':rl2011[0]})
        resultado.append({'id':2,'0':'2012','1':rs2012[0],'2':rl2012[0]})
        resultado.append({'id':3,'0':'2013','1':rs2013[0],'2':rl2013[0]})
        resultado.append({'id':4,'0':'2014','1':rs2014[0],'2':rl2014[0]})
        resultado.append({'id':5,'0':'2015','1':rs2015[0],'2':rl2015[0]})
        resultado.append({'id':6,'0':'2016','1':rs2016[0],'2':rl2016[0]})
        resultado.append({'id':7,'0':'2017','1':rs2017[0],'2':rl2017[0]})
        resultado.append({'id':8,'0':'2018','1':rs2018[0],'2':rl2018[0]})
        resultado.append({'id':9,'0':'2019','1':rs2019[0],'2':rl2019[0]})
        resultado.append({'id':10,'0':'2020','1':rs2020[0],'2':rl2020[0]})
        resultado.append({'id':11,'0':'2021','1':rs2021[0],'2':rl2021[0]})
        
        return {'status_code':200,'data':resultado}
    except Exception as e:
        print(e)
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")  
    
    
@app.get('/datawarehouse/noreste')
async def getDataWareHouseNoreste():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        cursor = conn.cursor()
        cursor.execute('  SELECT * FROM public."Noreste" ')
        datos = cursor.fetchall()
        
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
     
       
        if len(datos)!=0:
            resultado = []
            for p,i in enumerate(datos):
                resultado.append({'id':p ,
                '0':i[0],
                '1':i[1], 
                })
                
                
            conn.close()
            return {'status_code':200,'data':resultado}
                    
                    
        else:
            conn.close()
            return HTTPException(status_code=404, detail="no se encontro el recurso")
    except:
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")
    
    
@app.get('/datawarehouse/noroeste')
async def getDataWareHouseNoroeste():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        cursor = conn.cursor()
        cursor.execute('  SELECT * FROM public."Noroeste" ')
        datos = cursor.fetchall()
        
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
     
       
        if len(datos)!=0:
            resultado = []
            for p,i in enumerate(datos):
                resultado.append({'id':p ,
                '0':i[0],
                '1':i[1], 
                })
                
                
            conn.close()
            return {'status_code':200,'data':resultado}
                    
                    
        else:
            conn.close()
            return HTTPException(status_code=404, detail="no se encontro el recurso")
    except:
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")
    
    
@app.get('/datawarehouse/sureste')
async def getDataWareHouseSureste():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        cursor = conn.cursor()
        cursor.execute('  SELECT * FROM public."Sureste" ')
        datos = cursor.fetchall()
        
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
     
       
        if len(datos)!=0:
            resultado = []
            for p,i in enumerate(datos):
                resultado.append({'id':p ,
                '0':i[0],
                '1':i[1], 
                })
                
                
            conn.close()
            return {'status_code':200,'data':resultado}
                    
                    
        else:
            conn.close()
            return HTTPException(status_code=404, detail="no se encontro el recurso")
    except:
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")
    
@app.get('/datawarehouse/suroeste')
async def getDataWareHouseSuroeste():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="datawarehouse")
        cursor = conn.cursor()
        cursor.execute('  SELECT * FROM public."Suroeste" ')
        datos = cursor.fetchall()
        
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
     
       
        if len(datos)!=0:
            resultado = []
            for p,i in enumerate(datos):
                resultado.append({'id':p ,
                '0':i[0],
                '1':i[1], 
                })
                
                
            conn.close()
            return {'status_code':200,'data':resultado}
                    
                    
        else:
            conn.close()
            return HTTPException(status_code=404, detail="no se encontro el recurso")
    except:
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")


@app.get('/data-lake-postgres')
async def getDataLakePostgres():
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="data-lake")
        cursor = conn.cursor()
        cursor.execute('  SELECT * FROM public."indice-calidad-aire" ')
        datos = cursor.fetchall()
        
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
     
       
        if len(datos)!=0:
            resultado = []
            for p,i in enumerate(datos):
                resultado.append({'id':p ,
                '0':i[0],
                '1':i[1], 
                '2':i[2],
                '3':i[3],
                '4':i[4],
                '5':i[5],
                '6':i[6],
                '7':i[7],
                '8':i[8],
                '8':i[8],
                '8':i[8],
                '9':i[9],
                '10':i[10],
                '11':i[11],
                '12':i[12],
                '13':i[13],
                '14':i[14],
                '15':i[15],
                '16':i[16],
                '17':i[17],
                '18':i[18],
                '19':i[19],
                '20':i[20],
                '21':i[21],
                '22':i[22],
                '23':i[23],
                '24':i[24],
                '25':i[25],
                '26':i[26],
                })
                
                
            conn.close()
            return {'status_code':200,'data':resultado}
                    
                    
        else:
            conn.close()
            return HTTPException(status_code=404, detail="no se encontro el recurso")
    except:
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")
    
    
@app.get('/data-lake-maria')
async def getDataLakeMaria():
    con = pymysql.connect(host=host_mysql,
            port=port_mysql,
            user=user_mysql,
            password=password_mysql,
            database="data-lake")

    try:

        with con.cursor() as cur:

            cur.execute('  SELECT * FROM `data-lake`.`indice-calidad-aire` ')

            datos = cur.fetchall()
            print(len(datos))
            if len(datos)!=0:
                resultado = []
                for p,i in enumerate(datos):
                    resultado.append({'id':p ,
                    '0':i[0],
                    '1':i[1], 
                    '2':i[2],
                    '3':i[3],
                    '4':i[4],
                    '5':i[5],
                    '6':i[6],
                    '7':i[7],
                    '8':i[8],
                    '8':i[8],
                    '8':i[8],
                    '9':i[9],
                    '10':i[10],
                    '11':i[11],
                    '12':i[12],
                    '13':i[13],
                    '14':i[14],
                    '15':i[15],
                    '16':i[16],
                    '17':i[17],
                    '18':i[18],
                    '19':i[19],
                    '20':i[20],
                    '21':i[21],
                    '22':i[22],
                    '23':i[23],
                    '24':i[24],
                    '25':i[25],
                    '26':i[26],
                    })
                    
                    
                con.close()
                return {'status_code':200,'data':resultado}
            else:
                con.close()
                return HTTPException(status_code=404, detail="no se encontro el recurso")
                
                
                
                
    except:
        con.close()
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")
    
    
@app.get('/limpieza')
async def limpiar():
    try:
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

        conexion = psycopg2.connect(database='datawarehouse', 
                                    user=user_postgres, password=password_postgres,
                                    host=host_postgres, port=port_postgres)
            
        etl.todb(tabla_noroeste , conexion, 'Noroeste',create=True,constraints=False,drop=True)
        etl.todb(tabla_noreste , conexion, 'Noreste',create=True,constraints=False,drop=True)
        etl.todb(tabla_centro , conexion, 'Centro',create=True,constraints=False,drop=True)
        etl.todb(tabla_suroeste , conexion, 'Suroeste',create=True,constraints=False,drop=True)
        etl.todb(tabla_sureste , conexion, 'Sureste',create=True,constraints=True,drop=True)

                    
                    
        return {'status_code':200,'data':"ok"}
    except Exception as e:
        print(e)
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")

    

        
    
    """
    try:
        conn = psycopg2.connect(host=host_postgres,
            port=port_postgres,
            user=user_postgres,
            password=password_postgres,
            database="data-lake")
        cursor = conn.cursor()
        cursor.execute('  SELECT * FROM public."indice-calidad-aire" ')
        datos = cursor.fetchall()
        
        #print(json.dumps(datos))
        #json.dumps(cur.fetchall())
     
       
        if len(datos)!=0:
            resultado = []
            for p,i in enumerate(datos):
                resultado.append({'id':p ,
                '0':i[0],
                '1':i[1], 
                '2':i[2],
                '3':i[3],
                '4':i[4],
                '5':i[5],
                '6':i[6],
                '7':i[7],
                '8':i[8],
                '8':i[8],
                '8':i[8],
                '9':i[9],
                '10':i[10],
                '11':i[11],
                '12':i[12],
                '13':i[13],
                '14':i[14],
                '15':i[15],
                '16':i[16],
                '17':i[17],
                '18':i[18],
                '19':i[19],
                '20':i[20],
                '21':i[21],
                '22':i[22],
                '23':i[23],
                '24':i[24],
                '25':i[25],
                '26':i[26],
                })
                
                
            conn.close()
            return {'data':resultado}
                    
                    
        else:
            conn.close()
            return HTTPException(status_code=404, detail="no se encontro el recurso")
    except:
        return HTTPException(status_code=500, detail="Error de conexion con la base de datos")
    """


