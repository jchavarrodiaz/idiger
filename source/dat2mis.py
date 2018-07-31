# -*- coding: utf-8 -*-

import glob
import os
import shutil
import time
import datetime
import numpy as np
import pandas as pd

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer


def last_data(dat_name):

    idiger_structure = {'majority': ['CODE', 'YEAR', 'JDAY', 'HOUR', 'RAINFALL', 'BATTERY', 'TEMP. EXT', 'HUM. EXT.', 'TEMP. INT', 'HUM. INT', 'LEVEL'],
                        'fopae': ['CODE', 'YEAR', 'JDAY', 'HOUR', 'ZERO', 'RAINFALL'],
                        'na_value': 7999}

    idiger_stations_code = {'101': '0025405060'}

    with open(dat_name, "r") as f:
        last_line = f.readlines()[-1]

    if dat_name == 'ESTACIÒN FOPAE_Lluviam':
        cols = idiger_structure['fopae']
    else:
        cols = idiger_structure['majority']

    data = [x.replace('\n', '') for x in np.array(last_line.split(','))]
    df_data = pd.DataFrame(columns=cols)
    df_data.ix[0, :] = data
    df_data['Date'] = (datetime.datetime(int(df_data.loc[0, 'YEAR']), 1, 1) + datetime.timedelta(int(df_data.loc[0, 'JDAY']) - 1)).strftime('%Y%m%d')

    mis_header = '<STATION>{}</STATION><SENSOR>0240</SENSOR><DATEFORMAT>YYYYMMDD</DATEFORMAT>'.format(idiger_stations_code[df_data.loc[0, 'CODE']])
    line = [df_data.loc[0, 'Date'], '{}:{}:00'.format(df_data.loc[0, 'HOUR'][0:-2], df_data.loc[0, 'HOUR'][-2:]), np.float16(df_data.loc[0, 'RAINFALL'])]

    '''
    FALTA ESCRIBIR LOS DATOS Y GUARDAR EN .MIS
    CONDICIONAL QUE REVISE SI EXISTE EL .MIS, SI NO EXISTE CREARLO
    SI EXISTE ADD LINEA (LINE)
    
    '''

    print df_data.to_string()


class MyHandler(PatternMatchingEventHandler):

    patterns = ["*.dat"]

    def process(self):
        """
        event.event_type
            'modified' | 'created' | 'moved' | 'deleted'
        event.is_directory
            True | False
        event.src_path
            path/to/observed/file
        """

        files = glob.iglob(os.path.join('Q:/Registros/IDIGER', "*.dat"))

        for f in files:
            if os.path.isfile(f):
                try:
                    shutil.copy2(f, 'E:\jchavarro\OSPA\idiger\data')
                    last_data(f)
                except IOError as e:
                    print('Error: %s' % e.strerror, 'Copying file: ', f, time.strftime("%H:%M:%S"))
                    time.sleep(5)
                    shutil.copy2(f, 'E:\jchavarro\OSPA\idiger\data')

    def on_modified(self, event):
        time.sleep(60)
        self.process()


def main():
    observer = Observer()
    observer.schedule(MyHandler(), path='Q:/Registros/IDIGER', recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == '__main__':
    main()

