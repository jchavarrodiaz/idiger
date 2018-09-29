# coding=utf-8

from __future__ import print_function
import os
import datetime
import sqlite3
import sys
import ftplib

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import simplekml
import six

from config_utils import get_pars_from_ini
from web_utils import *


def bart_upload(name_file):
    ftp_server = get_pars_from_ini('../config/config.ini')['bart']['ftp_server']
    ftp_user = get_pars_from_ini('../config/config.ini')['bart']['ftp_user']
    ftp_key = get_pars_from_ini('../config/config.ini')['bart']['ftp_key']
    ftp_root = get_pars_from_ini('../config/config.ini')['bart']['ftp_root']

    source_file = '../results/kml/pronos/{}.kml'.format(name_file)
    target_file = '{}.kml'.format(name_file)

    try:
        s = ftplib.FTP(ftp_server, ftp_user, ftp_key)
        try:
            f = open(source_file, 'rb')
            s.cwd(ftp_root)
            s.storbinary('STOR ' + target_file, f)
            f.close()
            s.quit()
        except:
            print
            "No se ha podido encontrar el fichero " + source_file
    except:
        print
        "No se ha podido conectar al servidor " + ftp_server


def get_pronos(releases, str_date, mode):
    path_db = '../data/pronos/pronosticos_bogota.db'
    conn = sqlite3.connect(path_db)  # crear la conexion
    cursor = conn.cursor()

    query = """
        select *
        from pronosticos2
        where Fecha like '{}%'
        and (Jornada = '{}' or Jornada = '{}' or Jornada = '{}' or Jornada = '{}')
        order by Fecha
        """.format(str_date, releases[0], releases[1], releases[2], releases[3])

    results = cursor.execute(query)
    columns = [i[0] for i in results.description]
    data = results.fetchall()
    df_pronos = pd.DataFrame(data, columns=columns).drop_duplicates(subset=['Jornada', 'Zona'], keep='last')
    temp_eval = df_pronos.copy()
    df_pronos['TS_Min'] = np.where(temp_eval['TS_Min'] > temp_eval['TS_Max'], temp_eval['TS_Max'], temp_eval['TS_Min'])
    df_pronos['TS_Max'] = np.where(temp_eval['TS_Max'] > temp_eval['TS_Min'], temp_eval['TS_Max'], temp_eval['TS_Min'])

    if mode == 'operation':
        df_filter_pronos = df_pronos[df_pronos['Meterologo'] != 7]
    else:
        df_filter_pronos = df_pronos.copy()

    return df_filter_pronos


def get_geometry():
    path_db = '../data/pronos/pronosticos_bogota.db'
    conn = sqlite3.connect(path_db)  # crear la conexion
    cursor = conn.cursor()

    query = """
            select *
            from geometria
            """

    results = cursor.execute(query)
    columns = [i[0] for i in results.description]
    data = results.fetchall()
    return pd.DataFrame(data, columns=columns)


def get_icons():
    path_db = '../data/pronos/pronosticos_bogota.db'
    conn = sqlite3.connect(path_db)  # crear la conexion
    cursor = conn.cursor()

    query = """
                select *
                from iconos
                """

    results = cursor.execute(query)
    columns = [i[0] for i in results.description]
    data = results.fetchall()
    return pd.DataFrame(data, columns=columns)


def get_meteorologo():
    path_db = '../data/pronos/pronosticos_bogota.db'
    conn = sqlite3.connect(path_db)  # crear la conexion
    cursor = conn.cursor()

    query = """
                select *
                from meteorologo
                """

    results = cursor.execute(query)
    columns = [i[0] for i in results.description]
    data = results.fetchall()
    return pd.DataFrame(data, columns=columns)


def gen_kml(df_data, forecast):
    desc_zones = {1: 'Suba, Engativá',
                  2: 'Usaquén',
                  3: 'Chapinero, B. unidos, Teusaquillo, Pte. Aranda, Mártires, R. Uribe, A. Nariño, Santa Fe, Candelaria, San Cristobal',
                  4: 'Fontibón, Kennedy, Bosa',
                  5: 'Cuidad Bolívar, Tunjuelito, Usme'}

    kml = simplekml.Kml(open=1)
    logo_path = get_pars_from_ini('../config/config.ini')['Paths']['logo_pronos']
    df_coords = get_geometry()
    df_meteo = get_meteorologo()
    df_icons = get_icons()

    if not df_data.empty:
        pronos = df_data.copy()
        for zone in pronos.Zona:
            pnt = kml.newpoint()
            pnt.name = df_coords[df_coords.Cod_Zona == zone].Descripcion.values[0]

            dominio = desc_zones[zone]
            meteo = df_meteo[df_meteo.Id == int(pronos.Meterologo.values[0])].Nombre.values[0]
            edition_time = pronos[pronos.Zona == zone].Fecha_Update.values[0]
            tmin = pronos[pronos.Zona == zone].TS_Min.values[0]
            tmax = pronos[pronos.Zona == zone].TS_Max.values[0]
            tiempo = df_icons[df_icons.Cod_Icono == pronos[pronos.Zona == zone].Codigo_PT.values[0]].Descripcion.values[0]
            condiciones = pronos[pronos.Zona == zone].Hidro_Meteo.values[0]

            if len(condiciones) == 0:
                pnt.description = 'Dominio: {} <br><br> ' \
                                  'Condiciones Actuales: <br><br>' \
                                  'Tiempo Esperado: {} <br>' \
                                  'Temp. Mínima °C: {} <br>' \
                                  'Temp. Máxima °C: {} <br>' \
                                  'Meteorólogo: {} <br>' \
                                  'Fecha Edición: {} <br>' \
                                  '<br><br>' \
                                  '<img src="{}" alt="picture" width="145" height="40" align="right" >' \
                                  '<br>' \
                                  '<br>'.format(dominio, tiempo.encode('utf8'), tmin, tmax, meteo.encode('utf8'), edition_time, logo_path).decode('utf8')
            else:
                pnt.description = 'Dominio: {} <br><br> ' \
                                  'Condiciones Actuales: {} <br><br>' \
                                  'Tiempo Esperado: {} <br>' \
                                  'Temp. Mínima °C: {} <br>' \
                                  'Temp. Máxima °C: {} <br>' \
                                  'Meteorólogo: {} <br>' \
                                  'Fecha Edición: {} <br>' \
                                  '<br><br>' \
                                  '<img src="{}" alt="picture" width="145" height="40" align="right" >' \
                                  '<br>' \
                                  '<br>'.format(dominio, condiciones.encode('utf8'), tiempo.encode('utf8'), tmin, tmax, meteo.encode('utf8'), edition_time, logo_path).decode('utf8')

            pnt.coords = [(float(df_coords[df_coords.Cod_Zona == zone].Geometria.values[0].replace('POINT(', '')[:-1].split(' ')[0]),
                           float(df_coords[df_coords.Cod_Zona == zone].Geometria.values[0].replace('POINT(', '')[:-1].split(' ')[1]))]

            pnt.style.labelstyle.scale = 1.0
            pnt.style.iconstyle.scale = 2.0
            pnt.style.iconstyle.icon.href = df_icons[df_icons.Cod_Icono == pronos[pronos.Zona == zone].Codigo_PT.values[0]].url.values[0]
        #
        # # Save the KML
        if u'Ma\xf1ana' == u'{}'.format(forecast.decode('utf-8')):
            kml.save(u'../results/kml/pronos/Manana.kml')
            # kml.save('{}/Manana.kml'.format(out_path))
        else:
            kml.save(u'../results/kml/pronos/{}.kml'.format(forecast.decode('utf8')))
            # kml.save('{}/{}.kml'.format(out_path, forecast.decode('utf8')))
        print(forecast)
        bart_upload(forecast)

    else:

        print(u'No existen registros de pronóstico en la base de datos')
        sys.exit(0)


def main():
    dt_config = get_pars_from_ini('../config/config.ini')
    get_all_files(ls_names=['pronosticos_bogota.db'], path_src=dt_config['Paths']['bd_pronos_bogota'], path_out='../data/pronos/')

    str_date = datetime.datetime.today().strftime('%Y-%m-%d')
    str_date = '2018-09-27'

    # In trial mode, the script only takes the data of jchavarro user
    # (trial's user) and makes the kml files. In operation mode,
    # the script only takes the data of the official meteorologists.
    mode = 'operation'  # or trail for evaluations

    releases_pronos = ['Mañana', 'Tarde', 'Noche', 'Madrugada']
    df_pronos = get_pronos(releases=releases_pronos, str_date=str_date, mode=mode)

    for j in releases_pronos:
        gen_kml(df_data=df_pronos[df_pronos['Jornada'] == j.decode('utf8')], forecast=j)


if __name__ == '__main__':
    main()
