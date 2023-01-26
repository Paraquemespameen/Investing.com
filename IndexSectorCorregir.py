import os
import datetime as dt
import pandas as pd
import pandas_market_calendars as mcal


#Importaciones para leer la web
import requests
from bs4 import BeautifulSoup as bs
import lxml

#Quitar warnings
import warnings
warnings.filterwarnings(action="once")#"ignore")


def calendario_laboral(f_inicio="2018-01-01"):
    ''' Función para comprobar que hoy es laborable'''
    #print(dt.datetime.now().strftime("%H:%M"), f_inicio)
    #Definimos hoy, en función de la hora que es
    if dt.datetime.now().strftime("%H:%M")>="22:30":
        hoy = dt.date.today()
    else:
        hoy = dt.date.today()-dt.timedelta(days=1)
    #Cargamos calendario
    nyse = mcal.get_calendar("NYSE")
    #definimos los días lectivos

    lectivos = nyse.valid_days(start_date=f_inicio, end_date=hoy).date

    return lectivos

def abrir_archivo(path, tick):
    '''Módulo para abrir archivo'''

    df = pd.read_csv(f"{path}/{tick}.csv",
                        usecols=["Date", "Open", "High",
                                "Low", "Close", "Volume"],
                        index_col="Date", parse_dates=True)
    return df

def ema(df):
    '''Hallar las emas'''
    list_periodos = [20, 40, 200]
    for period in list_periodos:
        df[f"Ema_{period}"] = df["Close"].ewm(span=period,
                             adjust=False).mean().map("{:,.2f}".format)
        #print(df["Ema_{period}")].dtype)
        #Si hay algo de texto, le quitamos la "," cuestión de formatos
        df[f"Ema_{period}"].replace(",", "", regex=True, inplace=True)
        #la pasamos a números
        df[f"Ema_{period}"] = pd.to_numeric(df[f"Ema_{period}"], errors="coerce")
    return df

def dataframe_valores(path, listado, f_ini, f_fin, tick=""):
    '''Bucle para generar las urls y descargarlas'''
    import time

    listado = pd.read_csv(listado, index_col=[0])
    #Para filtrar el bucle
    #listado = listado[(listado["Tick"]>="DJUSGU")&(listado["Tick"]<="DJUSWU")]
    #listado = listado.iloc[(listado.index >= 161)&(listado.index < 188)]
    #listado de las que no se actualizan por yahoo
    #lista = ["DXY", "MOEX.ME", "IBEX35", "MERV", "BIVA", "IBRX", "TA35", "JTOPI",
    #         "MASI", "PLE", "AMGNRLX", "EGX30", "PTDOW", "NCI", "SMI", "AMX"]
    #listado = listado[listado["Tick"].isin (lista)]
    #Para un valor
    if tick != "":
        listado = listado[(listado["Tick"]==tick)]

    def rutina(path,row):
        '''Función para la rutina de descarga'''

        #Como hay dos valores que tienen como la hemos cambiado por ;,
        #Así que hay que devolverlo al valor original
        df = proceso(row, f_ini, f_fin)

        if not isinstance(df, str):
            df_junto = juntar_df(path, row["Tick"], df)
            df_junto.to_csv(f"{path}/{row['Tick']}.csv",
                            float_format=f"{'%.2f'}",
                            index_label="Num")
        else:
            print(f"{row['Tick']} ---> Ha habido un error al conseguir los datos")

    for _, row in listado.iterrows():
        rutina(path, row)
        time.sleep(1)

def descarga_old(row, f_ini, f_fin):
    '''Función para descargar y crear un dataframe con los datos de Invest
    No es necesario descargar todo, pero lo hago para mantener la compatibilidad
    con un navegador normal'''

    #Variables
    url = f"https://www.investing.com/indices/{row['nombre'].replace(';',',')}-historical-data"
    link = "https://www.investing.com/instruments/HistoricalDataAjax"

    payload = {
            "curr_id": str(row["curr_id"]),
            "smlID": str(row["smlID"]),#No es necesaria
            "header": str(row["header"]),#No es necesaria
            "st_date": f_ini,
            "end_date": f_fin,
            "interval_sec": "Daily",
            "sort_col": "date",
            "sort_ord": "DESC",
            "action": "historical_data"
    }

    cabecera = {
                "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0",
                "Referer": f"https://www.investing.com/indices/{url}-historical-data",
                "X-Requested-With":"XMLHttpRequest"
    }
    #Peticiones
    req = requests.post(link,
                        headers = cabecera,
                        #data = payload,
                        timeout=(2, 10))
    return req

def descarga(row, f_ini, f_fin):
    #url
    link = f"https://www.investing.com/indices/{row['nombre'].replace(';',',')}-historical-data"
    url = f"https://api.investing.com/api/financialdata/historical/{row['curr_id']}"
    f_ini = dt.datetime.strptime(f_ini, '%m/%d/%Y')
    f_ini = f_ini.strftime("%Y-%m-%d")

    parametros = {
        "start-date": f_ini,
        "end-date": f_fin,
        "time-frame": "Daily",
        "add-missing-rows": "false"
    }

    cabecera = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
        "Access-Control-Request-Headers": "www",
		"Access-Control-Request-Method": "GET",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "DNT": "1",
        "Host": "api.investing.com",
		"Origin": "https://www.investing.com",
		"Pragma": "no-cache",
        "Referer": "https://www.investing.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "TE": "trailers",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0"
	}

    cabecera_get = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "DNT": "1",
        "domain-id": "www",
        "Host": "api.investing.com",
        "Origin": "https://www.investing.com",
        "Pragma": "no-cache",
        "Referer": "https://www.investing.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "TE": "trailers",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0"
    }

    with requests.session() as s:
        s_req = s.get(link,
                      headers = cabecera_get)
        print(s.cookies)

        #Peticiones 
        req = s.options(url,
                        headers = cabecera,
                        data = parametros,
                        timeout=(2, 10))
        return req

def proceso(row, f_ini, f_fin):
    '''Módulo para el proceso de la descarga'''

    #Hacemos la llamada a la web con los parámetros establecidos
    req = descarga(row, f_ini, f_fin)
    #Vemos la respuesta
    estado = req.status_code

    #Si el estado no es correcto que nos de un error
    if estado == 200:
        datos = req.content
        rasca = rascado(datos)
        req.close()
    else:
        print(row["Tick"], estado)
        rasca = pd.DataFrame()

    if not rasca.empty:
        return rasca
    else:
        return "Error"

def rascado(datos):
    '''Función para rascar los datos de la web'''

    #Creamos diccionario auxiliar y el dataframe
    df = pd.DataFrame(columns=("Date","Open",
                                "High","Low",
                                "Close","Volume"))
    dicc = {}

    #Creamos la sopa y buscamos el cuerpo de la tabla
    sopa = bs(datos, "lxml")
    sopa = sopa.findAll("table")[1]
    sopa = sopa.tbody

    #Buscamos todas las etiquetas tr dentro del cuerpo de la tabla
    datos = sopa.find_all("tr")

    #Iteramos para rellenar el Dataframe
    try:
        for tr in datos:
            filas = tr.find_all("td")
            #Borramos las sentencias if-elif por las nuevas match-case
            for num, elemento in enumerate(filas[:6]):
                match num:
                    case 0:
                        dicc["Date"] = elemento["data-real-value"].replace(",","")
                    case 1:
                        dicc["Close"] = elemento["data-real-value"].replace(",","")
                    case 2:
                        dicc["Open"] = elemento["data-real-value"].replace(",","")
                    case 3:
                        dicc["High"] = elemento["data-real-value"].replace(",","")
                    case 4:
                        dicc["Low"] = elemento["data-real-value"].replace(",","")
                    case 5:
                        dicc["Volume"] = elemento["data-real-value"]
                    case _:
                        print("Fallo en los datos")
            #Creamos el dataframe con el diccionario
            df_1 = pd.DataFrame([dicc])
            df = pd.concat([df, df_1], ignore_index=True)
            #Reiniciamos el diccionario
            dicc = {}
        #Pasamos las fechas de Unix a Date
        df["Date"]= pd.to_datetime(df["Date"],unit="s")
        #Formateamos el df descargado
        df = df[::-1]
        df.set_index("Date", drop=True, inplace=True)
        #Cambiamos el tipo de variable
        typeColumns = {"Open":"float",
                    "High":"float",
                    "Low":"float",
                    "Close":"float",
                    "Volume":"int64"}
        df = df.astype(typeColumns)

    except Exception as e:
        print(f"Error --->  {e}")

    return df

def crear_listado_url(path):
    '''Función para crear un archivo con las url de cada sector
    Esta función solamente se puede usar cuando se agrega otro encabezado'''
    #Lista de archivos csv
    lista_archivos = [valor for valor in os.listdir(path) if ".csv" in valor]
    listado = pd.DataFrame(columns=("Tick", "nombre"))

    for tick in lista_archivos:
        try:
            archivo = pd.read_csv(f"{path}/{tick}")
            #Si la última columna no es Ema200 es porque hemos agregado otro cabecero
            if not archivo.columns[-1] == "Ema_200":
                listado = listado.append({"Tick":tick.split(".csv")[0],
                                           "nombre":archivo.columns[-1]},
                                           ignore_index=True)
        except Exception as e:
            print(f"{tick}  ---->  {e}")
    listado.to_csv(f"{path}/listado_url_1.txt")

def juntar_df(path, tick, df, df_archivo=pd.DataFrame):
    '''Módulo para juntar el df existente y el descargado'''

    #Abrimos el archivo
    if df_archivo.empty:
        try:
            df_archivo = abrir_archivo(path,tick)
        except Exception as e:
            print("{tick} ---> {e}")
            #Si el archivo no existe hacemos que df_archivo tenga la cabecera
            #de la descarga
            df_archivo = df[:2]

    #Juntamos
    #df = df.iloc[:-1]#Por si le queremos quitar el día de hoy  porque no se hq terminado
    df_archivo = pd.concat([df_archivo, df], sort=True)
    #Quitamos los índices duplicados
    df_archivo = df_archivo.loc[~df_archivo.index.duplicated(keep="last")]
    #Reordenamos
    df_archivo.sort_index(inplace=True)
    #Aplicamos emas
    df_archivo = ema(df_archivo)
    #Reseteamos el índice para cuadrar la numeración
    df_archivo.reset_index(inplace=True)
    #Ordenamos las columnas
    df_archivo = df_archivo[["Date","Open", "High",
                             "Low", "Close","Volume",
                             "Ema_20", "Ema_40", "Ema_200"]]

    return df_archivo

def corregir(path, listado, tick=""):
    '''Función para corregir los archivos por los días laborales'''
    #Para corregir, deberíamos poder cuadrar y descargar el archivo con las
    #especificaciones que queramos ... a investigar

    listado = pd.read_csv(listado, index_col=[0])
    #Para acotar la corrección
    listado = listado[(listado["Tick"]>="DJUSAE")&(listado["Tick"]<="DJUSWU")]
    #listado = listado.iloc[(listado.index >= 0)&(listado.index < 173)]

    if tick != "":
        listado = listado[listado["Tick"]==tick]

    def proceso_corregir(path, row):
        '''Función para el proceso de corrección'''
        #Abrimos el archivo
        df = abrir_archivo(path, row["Tick"])

        print (len(df))
        #Seleccionamos la primera fecha
        f_inicio = df.iloc[:1].index.date.item()
        #Hallamos el calendario laboral y lo pasamos de array a pd.index
        dias = pd.DatetimeIndex(calendario_laboral(f_inicio))
        #Ver los días que hay en el archivo que nos faltan
        dias_faltan = dias.difference(df.index)
        if not dias_faltan.empty:
            print(f"Dias que faltan por descargar: {dias_faltan}")
            df_aux = pd.DataFrame()
            #Pasamos los días en el formato elegido
            for dia in dias_faltan.strftime("%m/%d/%Y"):
                #descargamos la línea del día que corresponde
                linea = proceso(row, dia, dia)
                #Lo añadimos al df_aux
                if df_aux.empty:
                    df_aux = linea
                else:
                    df_aux = pd.concat([df_aux,linea], sort=True)
            #Juntamos el df y lo que nos faltaba
            df = juntar_df(path, row["Tick"], df_aux, df)
            #Pasamos el índice a la fecha porque la otra función nos lo cambiaba
            df.set_index("Date", drop=True, inplace=True)
            print (len(df))
            del dia, df_aux, linea
        #Ver los días no lectivos que nos sobran
        dias_no_lectivos = df.index.difference(dias)
        if not dias_no_lectivos.empty:
            print(f"Dias no lectivos que sobran: {dias_no_lectivos}")
            #Rehacemos el df con los días que no están en la lista de los que sobran
            df = (df[~df.index.isin(dias_no_lectivos)])
            df = ema(df)

        print (len(df))
        #Reseteamos el índice de nuevo
        df.reset_index(inplace=True)
        #Guardamos
        df.to_csv(f"{path}/{row['Tick']}.csv",
                  float_format=f"{'%.2f'}",
                  index_label="Num")
        del df, dias,f_inicio,dias_faltan,dias_no_lectivos

    for a, row in listado.iterrows():
        print(f"{a} ---> {row['Tick']}")
        try:
            proceso_corregir(path, row)
        except Exception as e:
            print(f"{row['Tick']} ---> {e}")

def principal():
    '''Función Principal'''

    #Variables de ruta y archivo
    path = "./IndexSector"
    listado = f"{path}/listado_url.txt"

    #fecha dia, mes y año
    #f_ini="26/12/2019"
    #fecha mes, dia y año
    f_ini="01/15/2022"

    #Definimos hoy, en función de la hora que es
    if dt.datetime.now().strftime("%H:%M")>="22:30":
        hoy = dt.date.today()
    else:
        hoy = dt.date.today()-dt.timedelta(days=1)

    #Atento a la fecha ... mes/dia/año 4 cifras
    #f_fin=f"{hoy.strftime('%m/%d/%Y')}"
    #Ha cambiado
    f_fin = f"{hoy.strftime('%Y-%m-%d')}"

    #Funciones
    #crear_listado_url(path)#Solamente cuando se agrega un encabezado a los archivos
    dataframe_valores(path, listado, f_ini, f_fin)#, "DJI")#Borrar para hacer un sólo valor
    #corregir(path, listado)#,"DJUSCP")#Para corregir los Archivos

if __name__ == "__main__":
    #Contador de tiempo
    inicio = dt.datetime.now().replace(microsecond=0)
    print(f"Comenzado a las {inicio}")
    #Función principal
    principal()
    #Contador de tiempo y cálculo de la ejecución
    final = dt.datetime.now().replace(microsecond=0)
    print(f"Terminado a las {final}")
    print(f"La duración ha sido de {final-inicio}")
