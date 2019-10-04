#!/usr/bin/env python
""" dataclean.py

This module contain routines for cleaning txt files from the air pollution dataset
"""

__author__ = "Alejandro de la Calle"
__copyright__ = "Copyright 2018"
__credits__ = [""]
__license__ = ""
__version__ = "0.1"
__maintainer__ = "Alejandro de la Calle"
__email__ = "alejandrodelacallenegro@gmail.com"
__status__ = "Development"


import os
import pandas as pd
import glob
import datetime
#import json


##############################################
## Dictionaries useful for pollution and weather data

estaciones_aire = {'28079001': 'Plz. Recoletos', '28079002': 'Glta. Carlos V', '28079003': 'Pza. Carmen (<2011)',
              '28079035': 'Plz. Carmen', '28079004': 'Plz. España', '28079005': 'Barrio del Pilar (<2011)',
              '28079039': 'Barrio del Pilar', '28079006': 'Plz. Dr. Marañon', '28079007': 'Plz. M. de Salamanca',
              '28079008': 'Escuelas Aguirre', '28079009': 'Plz. Luca de Tena', '28079010': 'Cuatro Caminos (<2011)',
              '28079038': 'Cuatro Caminos', '28079011': 'Av. Ramón y Cajal', '28079012': 'Plz. Manuel Becerra',
              '28079013': 'Vallecas (<2011)', '28079040': 'Vallecas', '28079014': 'Plz. Fernández Ladreda (<2009)',
              '28079015': 'Plz. Castilla (<2008)', '28079016': 'Arturo Soria', '28079017': 'Villaverde Alto',
              '28079018': 'C/ Farolillo', '28079019': 'Huerta Castañeda', '28079020': 'Moratalaz (<2011)',
              '28079036': 'Moratalaz', '28079021': 'Pza. Cristo Rey', '28079022': 'P. Pontones', '28079023': 'Final Alcalá',
              '28079024': 'Casa campo', '28079025': 'St. Eugenia', '28079026': 'Urb. Embajada-Barajas (<2010)',
              '28079027': 'Barajas', '28079047': 'Mendez Álvaro', '28079048': 'P. Castellana', '28079049': 'Retiro',
              '28079050': 'Plz. Castilla', '28079054': 'Ensanche Vallecas', '28079055': 'Urb. Embajada-Barajas',
              '28079056': 'Plz. Fdz Ladreda', '28079057': 'Sanchinarro', '28079058': 'El Pardo',
              '28079059': 'Parque Juan Carlos I', '28079086': 'Tres Olivos (<2011)', '28079060': 'Tres Olivos',
              '28079099': 'Media global'}

sustancias = {'01': 'SO2', '06': 'CO', '07': 'NO', '08': 'NO2', '09': 'PM2.5', '10': 'PM10', '12': 'NOx', '14': 'O3',
              '20': 'TOL', '30': 'BEN', '35': 'EBE', '37': 'MXY', '38': 'PXY', '39': 'OXY', '42': 'TCH', '43': 'CH4',
              '44': 'NMHC', '58': 'HCl'}

mes = {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
       9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'}

estaciones_meteo = {'Retiro': '3195', 'Aeropuerto': '3129', 'Ciudad_Universitaria': '3194U', 'Cuatro_Vientos': '3196'}

################################################################################


def get_date(year, month, day, hour=0, minute=0):
    return pd.to_datetime('{}-{}-{} {}:{}'.format(year, month, day, hour, minute))

def create_date_column(df):
    df['date'] = datetime.date(year=df['año'], month=df['mes'], dia=df['dia'])

def parse_pollution_txt(txt_file):
    horasstr = ['H{:02d}'.format(h) for h in range(1, 25)]
    valstr = ['V{:02d}'.format(v) for v in range(1, 25)]

    cod_provincia = []
    cod_municipio = []
    cod_estacion = []
    cod_magnitud = []
    cod_tecnica = []
    anno = []
    mes = []
    dia = []
    horas = []
    val = []

    with open(txt_file) as f:
        lines = f.readlines()
        for line in lines:
            line.replace('\n', '')
            cod_provincia.append(line[:2])
            cod_municipio.append(line[2:5])
            cod_estacion.append(line[5:8])
            cod_magnitud.append(line[8:10])
            cod_tecnica.append(line[10:12])
            anno.append(line[14:16])
            mes.append(line[16:18])
            dia.append(line[18:20])
            hh = [line[20 + h * 6:25 + h * 6] for h in range(24)]
            horas.append(hh)
            vv = [line[25 + v * 6] for v in range(24)]
            val.append(vv)

    # Transform hhh to have only 24 columns (one per hour), and the length of each column
    # is the number of measures
    horas = list(zip(*horas))
    val = list(zip(*val))

    measures = dict(zip(horasstr, horas))
    validez = dict(zip(valstr, val))

    cols1 = {'PROVINCIA': cod_provincia, 'MUNICIPIO': cod_municipio, 'ESTACION': cod_estacion,
             'MAGNITUD': cod_magnitud, 'TECNICA': cod_tecnica, 'ANO': anno, 'MES': mes, 'DIA': dia}

    cols = {**cols1, **measures, **validez}

    dataf = pd.DataFrame(data=cols)
    return dataf

def parse_pollution_csv(csv_file):
    dd = pd.read_csv(csv_file, delimiter=';')
    dd['TECNICA'] = dd['PUNTO_MUESTREO'].apply(lambda x: int(x.split('_')[-1]))
    dd.drop(columns=['PUNTO_MUESTREO'], inplace=True)
    return dd

def extract_pollution_data(txt_path):

    folders = sorted(glob.glob(os.path.join(txt_path, 'raw', 'Anio*')))
    #print('Getting files from year {}'.format(os.path.basename(folder)[4:8]))

    txt_files = [sorted(glob.glob(os.path.join(folder, '*txt'))) for folder in folders]
    csv_files = [sorted(glob.glob(os.path.join(folder, '*csv'))) for folder in folders]
    # Flatten list
    if len(txt_files) > 1:
        txt_files = [item for sublist in txt_files for item in sublist]
    if len(csv_files) > 1:
        csv_files = [item for sublist in csv_files for item in sublist]

    print('Parsing data...')
    if len(txt_files) > 0:
        data_from_txt = [parse_pollution_txt(file) for file in txt_files]

    if len(csv_files) > 0:
        data_from_csv = [parse_pollution_csv(file) for file in csv_files]

    return data_from_txt, data_from_csv


##############################################################################
##############################################################################

if __name__ == '__main__':

    ROOT = "/Users/adelacalle/Desktop"
    #DATASET_PATH = os.path.join(ROOT, "data/calidad_aire_madrid")
    DATASET_PATH = '/Users/adelacalle/Downloads'
    #DESTINATION_PATH = os.path.join(DATASET_PATH, 'csv')

    extract_pollution_data(DATASET_PATH)

    print('Script dataclean finished')