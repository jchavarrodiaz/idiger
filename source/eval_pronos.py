# -*- coding: utf-8 -*-

import sys
import sqlite3
import pandas as pd
import datetime

from web_utils import *
from config_utils import get_pars_from_ini


def get_pronos(releases, str_date):
    pars = get_pars_from_ini('../config/config.ini')

    path_db = '../data/pronos/pronosticos_bogota.db'
    conn = sqlite3.connect(path_db)  # crear la conexion
    cursor = conn.cursor()

    for release in releases:

        query = """
            select *
            from pronosticos2
            where Fecha like '{}%'
            and Jornada = '{}'
            order by Zona
            """.format(str_date, release)

        results = cursor.execute(query)
        columns = [i[0] for i in results.description]
        data = results.fetchall()

        return pd.DataFrame(data, columns=columns)


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


def main():

    dt_config = get_pars_from_ini('../config/config.ini')
    get_all_files(ls_names=['pronosticos_bogota.db'], path_src=dt_config['Paths']['bd_pronos_bogota'], path_out='../data/pronos/')

    # hour = datetime.datetime.now().hour
    hour = 8
    # str_date = datetime.datetime.today().strftime('%Y-%m-%d')
    str_date = '2018-05-24'

    if hour == 8:
        releases_pronos = ['Mañana', 'Tarde']

    elif hour == 18:
        releases_pronos = ['Noche', 'Madrugada']

    else:
        print('Emision de Pronostico por fuera de la hora oficial')
        sys.exit(0)

    get_pronos(releases=releases_pronos, str_date=str_date)


if __name__ == '__main__':
    main()
