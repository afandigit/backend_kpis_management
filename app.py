from flask import Flask, request, jsonify, session
from flask_bcrypt import Bcrypt
from flask_session import Session
from flask_cors import CORS, cross_origin
from config import ApplicationConfig
from models import db, User
import mysql.connector
from threading import Lock
from flask_socketio import SocketIO
import json
from time import sleep

import pandas as pd
import pickle
import joblib
from datetime import datetime, timedelta


from getE1Data import load_e1_data, get_unique_from_e1_dataset
from getOEEData import load_OEE_data, get_unique_from_oee_dataset

e1_dataset_file_path = '../DataSource/efficience.csv'
oee_dataset_file_path = '../DataSource/oee.csv'

# files of prediction model and the data preprocessing objects
random_forest_regressor_file_path = '../DataSource/dataScience/random_forest_regressor.pkl'
scaler_file_path = '../DataSource/dataScience/scaler.pkl'
encoder_file_path = '../DataSource/dataScience/encoder.pkl'
labels_scaler_file_path = '../DataSource/dataScience/labels_scaler.pkl'




"""
Background Thread
"""
thread = None
thread_lock = Lock()

app = Flask(__name__)
app.config.from_object(ApplicationConfig)

bcrypt = Bcrypt(app)
SESSION_TYPE = "filesystem"
server_session = Session(app)
CORS(app, supports_credentials=True)
socketio = SocketIO(app, cors_allowed_origins='*')
server_session = Session(app)
db.init_app(app)

with app.app_context():
    db.create_all()

MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root'
MYSQL_DATABASE_NAME = "kpis_management"
EFFICIENCE_MYSQL_TABLE_NAME = "efficience"
OEE_MYSQL_TABLE_NAME = "oee"

def get_mysql_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database= MYSQL_DATABASE_NAME
    )

def is_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

def query_mysql_database(query):
    connection = get_mysql_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data



# ------------------------ KPI OEE : Urls ------------------------

@app.route('/sql/scrap', methods=['GET'])
@app.route('/sql/scrap/<serialNumber>', methods=['GET'])
@app.route('/sql/scrap/<serialNumber>/<shift>', methods=['GET'])
def get_scrap_data(serialNumber=None, shift=None):
    try :
        where_serialNumber_condition =  " where Serial_Number='"+serialNumber+"'" if serialNumber is not None else ""
        where_shift_condition =  " and shift="+shift if shift is not None else ""
        where_condition = where_serialNumber_condition + where_shift_condition
        results =  query_mysql_database(f"SELECT BU, Machine_Type, Serial_Number, Performance, Performance_losses, Batch_size, QuantityPerH, Quantity, Avg_lengthPerMm, ScrapPerM, Scrap, shift, Day, Month, Year, Date FROM {OEE_MYSQL_TABLE_NAME}" + where_condition)
        transformed_data = []
        for row in results:
            transformed_row = { 'BU': row[0],
                                'Machine_Type': row[1], 
                                'Serial_Number': row[2], 
                                'Performance': 0 if row[3] == "" or row[3].lower() == "nan" or row[3] == "#DIV/0!" else float(row[3]) , 
                                'Performance_losses': 0 if row[3] == "" or row[4].lower() == "nan" or row[4] == "#DIV/0!" else float(row[4]) , 
                                'Batch_size': 0 if row[3] == "" or row[5].lower() == "nan" or row[5] == "#DIV/0!" else float(row[5]) , 
                                'QuantityPerH': 0 if row[3] == "" or row[6].lower() == "nan" or row[6] == "#DIV/0!" else float(row[6]) , 
                                'Quantity': 0 if row[3] == "" or row[7].lower() == "nan" or row[7] == "#DIV/0!" else float(row[7]) , 
                                'Avg_lengthPerMm': 0 if row[3] == "" or row[8].lower() == "nan" or row[8] == "#DIV/0!" else float(row[8]) , 
                                'ScrapPerM': 0 if row[3] == "" or row[9].lower() == "nan" or row[9] == "#DIV/0!" else float(row[9]), 
                                'Scrap': 0 if row[3] == "" or row[10].lower() == "nan" or row[10] == "#DIV/0!" else float(row[10]), 
                                'shift': int(row[11]),
                                'Day': int(row[12]), 
                                'Month': int(row[13]),
                                'Year': int(row[14]),
                                'Date': row[15].split(' ')[0]
                }
            
            transformed_data.append(transformed_row)
        del results
        return transformed_data
    except mysql.connector.Error as error:
        return jsonify({'error': str(error), 'code': 1})

@app.route('/sql/oee', methods=['GET'])
@app.route('/sql/oee/<serialNumber>', methods=['GET'])
@app.route('/sql/oee/<serialNumber>/<shift>', methods=['GET'])
def get_oee_data_from_mysql(serialNumber=None, shift=None):
    try:
        where_serialNumber_condition = " where Serial_Number='"+serialNumber+"'" if serialNumber is not None else ""
        where_shift_condition =  " and shift="+shift if shift is not None else ""
        where_condition = where_serialNumber_condition + where_shift_condition
        results = query_mysql_database(f"SELECT BU, Machine_Type, Serial_Number, Unused_Machines_J1, Unused_Machines_UNP, Unused_Machines_PL, OEE, OEE_J1, OEE_J1_H1_G_G6, Availability, Performance, Planned_Downtime, Unplanned_Downtime, Performance_losses, Batch_size, SetuptimePerMin, WaitingtimePerMin, QuantityPerH, Quantity, Avg_lengthPerMm, ShiftTimePerH, Netto_StandardTimePerH, DowntimePerH, Dw_J1, Planned_DowntimePerH, Unplanned_DowntimePerH, Urgent_Orders_A16, Disciplinary_responsible_vs_Shifthours, Not_disciplinary_responsible_vs_Shifthours, No_Orders, ScrapPerM, Scrap, Avg_No_Machines, Somme_de_Dw_B, shift, Day, Month, Year, Date from {OEE_MYSQL_TABLE_NAME} "+where_condition)
        transformed_data = []
        for row in results:
            transformed_row = { 'BU': row[0],
                                'Machine_Type': row[1], 
                                'Serial_Number': row[2], 
                                'Unused_Machines_J1': 0 if row[3] == "" or row[3].lower() == "nan" or row[3] == "#DIV/0!" else float(row[3]), 
                                'Unused_Machines_UNP': 0 if row[3] == "" or row[4].lower() == "nan" or row[4] == "#DIV/0!" else float(row[4]), 
                                'Unused_Machines_PL': 0 if row[3] == "" or row[5].lower() == "nan" or row[5] == "#DIV/0!" else float(row[5]), 
                                'OEE': 0 if row[3] == "" or row[6].lower() == "nan" or row[6] == "#DIV/0!" else float(row[6]), 
                                'OEE_J1': 0 if row[3] == "" or row[7].lower() == "nan" or row[7] == "#DIV/0!" else float(row[7]), 
                                'OEE_J1_H1_G_G6': 0 if row[3] == "" or row[8].lower() == "nan" or row[8] == "#DIV/0!" else float(row[8]), 
                                'Availability': 0 if row[3] == "" or row[9].lower() == "nan" or row[9] == "#DIV/0!" else float(row[9]), 
                                'Performance': 0 if row[3] == "" or row[10].lower() == "nan" or row[10] == "#DIV/0!" else float(row[10]) , 
                                'Planned_Downtime': 0 if row[3] == "" or row[11].lower() == "nan" or row[11] == "#DIV/0!" else float(row[11]) , 
                                'Unplanned_Downtime': 0 if row[3] == "" or row[12].lower() == "nan" or row[12] == "#DIV/0!" else float(row[12]) , 
                                'Performance_losses': 0 if row[3] == "" or row[13].lower() == "nan" or row[13] == "#DIV/0!" else float(row[13]) , 
                                'Batch_size': 0 if row[3] == "" or row[14].lower() == "nan" or row[14] == "#DIV/0!" else float(row[14]) , 
                                'SetuptimePerMin': 0 if row[3] == "" or row[15].lower() == "nan" or row[15] == "#DIV/0!" else float(row[15]) , 
                                'WaitingtimePerMin': 0 if row[3] == "" or row[16].lower() == "nan" or row[16] == "#DIV/0!" else float(row[16]) , 
                                'QuantityPerH': 0 if row[3] == "" or row[17].lower() == "nan" or row[17] == "#DIV/0!" else float(row[17]) , 
                                'Quantity': 0 if row[3] == "" or row[18].lower() == "nan" or row[18] == "#DIV/0!" else float(row[18]) , 
                                'Avg_lengthPerMm': 0 if row[3] == "" or row[19].lower() == "nan" or row[19] == "#DIV/0!" else float(row[19]) , 
                                'ShiftTimePerH': 0 if row[3] == "" or row[20].lower() == "nan" or row[20] == "#DIV/0!" else float(row[20]) , 
                                'Netto_StandardTimePerH': 0 if row[3] == "" or row[21].lower() == "nan" or row[21] == "#DIV/0!" else float(row[21]) , 
                                'DowntimePerH': 0 if row[3] == "" or row[22].lower() == "nan" or row[22] == "#DIV/0!" else float(row[22]) , 
                                'Dw_J1': 0 if row[3] == "" or row[23].lower() == "nan" or row[23] == "#DIV/0!" else float(row[23]) , 
                                'Planned_DowntimePerH': 0 if row[3] == "" or row[24].lower() == "nan" or row[24] == "#DIV/0!" else float(row[24]) , 
                                'Unplanned_DowntimePerH': 0 if row[3] == "" or row[25].lower() == "nan" or row[25] == "#DIV/0!" else float(row[25]) , 
                                'Urgent_Orders_A16': 0 if row[3] == "" or row[26].lower() == "nan" or row[26] == "#DIV/0!" else float(row[26]) , 
                                'Disciplinary_responsible_vs_Shifthours': 0 if row[3] == "" or row[27].lower() == "nan" or row[27] == "#DIV/0!" else float(row[27]) , 
                                'Not_disciplinary_responsible_vs_Shifthours': 0 if row[3] == "" or row[28].lower() == "nan" or row[28] == "#DIV/0!" else float(row[28]) , 
                                'No_Orders': 0 if row[3] == "" or row[29].lower() == "nan" or row[29] == "#DIV/0!" else float(row[29]) , 
                                'ScrapPerM': 0 if row[3] == "" or row[30].lower() == "nan" or row[30] == "#DIV/0!" else float(row[30]) , 
                                'Scrap': 0 if row[3] == "" or row[31].lower() == "nan" or row[31] == "#DIV/0!" else float(row[31]) , 
                                'Avg_No_Machines': 0 if row[3] == "" or row[32].lower() == "nan" or row[32] == "#DIV/0!" else float(row[32]) , 
                                'Somme_de_Dw_B': 0 if row[3] == "" or row[33].lower() == "nan" or row[33] == "#DIV/0!" else float(row[33]), 
                                'shift': int(row[34]),
                                'Day': int(row[35]), 
                                'Month': int(row[36]),
                                'Year': int(row[37]),
                                'Date': row[38].split(' ')[0]
                }
                   
            transformed_data.append(transformed_row)
        del results
        return jsonify({'data': transformed_data, 'code': 0})
    except mysql.connector.Error as error:
        return jsonify({'error': str(error), 'code': 1})

@app.route('/sql/oee/lastWeek', methods=['GET'])
@app.route('/sql/oee/lastWeek/<serialNumber>/<shift>', methods=['GET'])
def get_last_week_oee_from_mysql(serialNumber=None, shift=None):
    try:
        date_condition = f" where Date >= ( SELECT MAX(Date) FROM {OEE_MYSQL_TABLE_NAME} ) - INTERVAL 7 DAY AND date <= ( SELECT MAX(date) FROM {OEE_MYSQL_TABLE_NAME} )"
        where_serialNumber_condition =  " and Serial_Number='"+serialNumber+"'" if serialNumber is not None else ""
        where_shift_condition =  " and shift="+shift if shift is not None else ""
        where_condition = date_condition + where_serialNumber_condition + where_shift_condition
        results =  query_mysql_database(f"SELECT BU, Machine_Type, Serial_Number, Unused_Machines_J1, Unused_Machines_UNP, Unused_Machines_PL, OEE, OEE_J1, OEE_J1_H1_G_G6, Availability, Performance, Planned_Downtime, Unplanned_Downtime, Performance_losses, Batch_size, SetuptimePerMin, WaitingtimePerMin, QuantityPerH, Quantity, Avg_lengthPerMm, ShiftTimePerH, Netto_StandardTimePerH, DowntimePerH, Dw_J1, Planned_DowntimePerH, Unplanned_DowntimePerH, Urgent_Orders_A16, Disciplinary_responsible_vs_Shifthours, Not_disciplinary_responsible_vs_Shifthours, No_Orders, ScrapPerM, Scrap, Avg_No_Machines, Somme_de_Dw_B, shift, Day, Month, Year, Date FROM {OEE_MYSQL_TABLE_NAME}" + where_condition + "order by Date ASC")
        transformed_data = []
        for row in results:
            transformed_row = { 'BU': row[0],
                                'Machine_Type': row[1], 
                                'Serial_Number': row[2], 
                                'Unused_Machines_J1': 0 if row[3] == "" or row[3].lower() == "nan" or row[3] == "#DIV/0!" else float(row[3]), 
                                'Unused_Machines_UNP': 0 if row[4] == "" or row[4].lower() == "nan" or row[4] == "#DIV/0!" else float(row[4]), 
                                'Unused_Machines_PL': 0 if row[5] == "" or row[5].lower() == "nan" or row[5] == "#DIV/0!" else float(row[5]), 
                                'OEE': 0 if row[6] == "" or row[6].lower() == "nan" or row[6] == "#DIV/0!" else float(row[6]), 
                                'OEE_J1': 0 if row[7] == "" or row[7].lower() == "nan" or row[7] == "#DIV/0!" else float(row[7]), 
                                'OEE_J1_H1_G_G6': 0 if row[8] == "" or row[8].lower() == "nan" or row[8] == "#DIV/0!" else float(row[8]), 
                                'Availability': 0 if row[9] == "" or row[9].lower() == "nan" or row[9] == "#DIV/0!" else float(row[9]), 
                                'Performance': 0 if row[10] == "" or row[10].lower() == "nan" or row[10] == "#DIV/0!" else float(row[10]) , 
                                'Planned_Downtime': 0 if row[11] == "" or row[11].lower() == "nan" or row[11] == "#DIV/0!" else float(row[11]) , 
                                'Unplanned_Downtime': 0 if row[12] == "" or row[12].lower() == "nan" or row[12] == "#DIV/0!" else float(row[12]) , 
                                'Performance_losses': 0 if row[13] == "" or row[13].lower() == "nan" or row[13] == "#DIV/0!" else float(row[13]) , 
                                'Batch_size': 0 if row[14] == "" or row[14].lower() == "nan" or row[14] == "#DIV/0!" else float(row[14]) , 
                                'SetuptimePerMin': 0 if row[15] == "" or row[15].lower() == "nan" or row[15] == "#DIV/0!" else float(row[15]) , 
                                'WaitingtimePerMin': 0 if row[16] == "" or row[16].lower() == "nan" or row[16] == "#DIV/0!" else float(row[16]) , 
                                'QuantityPerH': 0 if row[17] == "" or row[17].lower() == "nan" or row[17] == "#DIV/0!" else float(row[17]) , 
                                'Quantity': 0 if row[18] == "" or row[18].lower() == "nan" or row[18] == "#DIV/0!" else float(row[18]) , 
                                'Avg_lengthPerMm': 0 if row[19] == "" or row[19].lower() == "nan" or row[19] == "#DIV/0!" else float(row[19]) , 
                                'ShiftTimePerH': 0 if row[20] == "" or row[20].lower() == "nan" or row[20] == "#DIV/0!" else float(row[20]) , 
                                'Netto_StandardTimePerH': 0 if row[21] == "" or row[21].lower() == "nan" or row[21] == "#DIV/0!" else float(row[21]) , 
                                'DowntimePerH': 0 if row[22] == "" or row[22].lower() == "nan" or row[22] == "#DIV/0!" else float(row[22]) , 
                                'Dw_J1': 0 if row[23] == "" or row[23].lower() == "nan" or row[23] == "#DIV/0!" else float(row[23]) , 
                                'Planned_DowntimePerH': 0 if row[24] == "" or row[24].lower() == "nan" or row[24] == "#DIV/0!" else float(row[24]) , 
                                'Unplanned_DowntimePerH': 0 if row[25] == "" or row[25].lower() == "nan" or row[25] == "#DIV/0!" else float(row[25]) , 
                                'Urgent_Orders_A16': 0 if row[26] == "" or row[26].lower() == "nan" or row[26] == "#DIV/0!" else float(row[26]) , 
                                'Disciplinary_responsible_vs_Shifthours': 0 if row[27] == "" or row[27].lower() == "nan" or row[27] == "#DIV/0!" else float(row[27]) , 
                                'Not_disciplinary_responsible_vs_Shifthours': 0 if row[28] == "" or row[28].lower() == "nan" or row[28] == "#DIV/0!" else float(row[28]) , 
                                'No_Orders': 0 if row[29] == "" or row[29].lower() == "nan" or row[29] == "#DIV/0!" else float(row[29]) , 
                                'ScrapPerM': 0 if row[30] == "" or row[30].lower() == "nan" or row[30] == "#DIV/0!" else float(row[30]) , 
                                'Scrap': 0 if row[31] == "" or row[31].lower() == "nan" or row[31] == "#DIV/0!" else float(row[31]) , 
                                'Avg_No_Machines': 0 if row[32] == "" or row[32].lower() == "nan" or row[32] == "#DIV/0!" else float(row[32]) , 
                                'Somme_de_Dw_B': 0 if row[33] == "" or row[33].lower() == "nan" or row[33] == "#DIV/0!" else float(row[33]), 
                                'shift': int(row[34]),
                                'Day': int(row[35]), 
                                'Month': int(row[36]),
                                'Year': int(row[37]),
                                'Date': row[38].split(' ')[0]
                }
                   
            transformed_data.append(transformed_row)
        del results

        return transformed_data
    except mysql.connector.Error as error:
        return jsonify({'error': str(error), 'code': 1})

@app.route('/sql/oee/week_befor_lastWeek', methods=['GET'])
@app.route('/sql/oee/week_befor_lastWeek/<serialNumber>/<shift>', methods=['GET'])
def get_week_befor_last_week_oee_from_mysql(serialNumber=None, shift=None):
    try:
        date_condition = f" where Date >= ( SELECT MAX(Date) FROM {OEE_MYSQL_TABLE_NAME}  ) - INTERVAL 14 DAY AND date <= ( SELECT MAX(date) FROM {OEE_MYSQL_TABLE_NAME}  ) - INTERVAL 7 DAY "
        where_serialNumber_condition =  " and Serial_Number='"+serialNumber+"'" if serialNumber is not None else ""
        where_shift_condition =  " and shift="+shift if shift is not None else ""
        where_condition = date_condition + where_serialNumber_condition + where_shift_condition
        results =  query_mysql_database(f"SELECT BU, Machine_Type, Serial_Number, Unused_Machines_J1, Unused_Machines_UNP, Unused_Machines_PL, OEE, OEE_J1, OEE_J1_H1_G_G6, Availability, Performance, Planned_Downtime, Unplanned_Downtime, Performance_losses, Batch_size, SetuptimePerMin, WaitingtimePerMin, QuantityPerH, Quantity, Avg_lengthPerMm, ShiftTimePerH, Netto_StandardTimePerH, DowntimePerH, Dw_J1, Planned_DowntimePerH, Unplanned_DowntimePerH, Urgent_Orders_A16, Disciplinary_responsible_vs_Shifthours, Not_disciplinary_responsible_vs_Shifthours, No_Orders, ScrapPerM, Scrap, Avg_No_Machines, Somme_de_Dw_B, shift, Day, Month, Year, Date FROM {OEE_MYSQL_TABLE_NAME}" + where_condition + "order by Date ASC")
        transformed_data = []
        for row in results:
            transformed_row = { 'BU': row[0],
                                'Machine_Type': row[1], 
                                'Serial_Number': row[2], 
                                'Unused_Machines_J1': 0 if row[3] == "" or row[3].lower() == "nan" or row[3] == "#DIV/0!" else float(row[3]), 
                                'Unused_Machines_UNP': 0 if row[4] == "" or row[4].lower() == "nan" or row[4] == "#DIV/0!" else float(row[4]), 
                                'Unused_Machines_PL': 0 if row[5] == "" or row[5].lower() == "nan" or row[5] == "#DIV/0!" else float(row[5]), 
                                'OEE': 0 if row[6] == "" or row[6].lower() == "nan" or row[6] == "#DIV/0!" else float(row[6]), 
                                'OEE_J1': 0 if row[7] == "" or row[7].lower() == "nan" or row[7] == "#DIV/0!" else float(row[7]), 
                                'OEE_J1_H1_G_G6': 0 if row[8] == "" or row[8].lower() == "nan" or row[8] == "#DIV/0!" else float(row[8]), 
                                'Availability': 0 if row[9] == "" or row[9].lower() == "nan" or row[9] == "#DIV/0!" else float(row[9]), 
                                'Performance': 0 if row[10] == "" or row[10].lower() == "nan" or row[10] == "#DIV/0!" else float(row[10]) , 
                                'Planned_Downtime': 0 if row[11] == "" or row[11].lower() == "nan" or row[11] == "#DIV/0!" else float(row[11]) , 
                                'Unplanned_Downtime': 0 if row[12] == "" or row[12].lower() == "nan" or row[12] == "#DIV/0!" else float(row[12]) , 
                                'Performance_losses': 0 if row[13] == "" or row[13].lower() == "nan" or row[13] == "#DIV/0!" else float(row[13]) , 
                                'Batch_size': 0 if row[14] == "" or row[14].lower() == "nan" or row[14] == "#DIV/0!" else float(row[14]) , 
                                'SetuptimePerMin': 0 if row[15] == "" or row[15].lower() == "nan" or row[15] == "#DIV/0!" else float(row[15]) , 
                                'WaitingtimePerMin': 0 if row[16] == "" or row[16].lower() == "nan" or row[16] == "#DIV/0!" else float(row[16]) , 
                                'QuantityPerH': 0 if row[17] == "" or row[17].lower() == "nan" or row[17] == "#DIV/0!" else float(row[17]) , 
                                'Quantity': 0 if row[18] == "" or row[18].lower() == "nan" or row[18] == "#DIV/0!" else float(row[18]) , 
                                'Avg_lengthPerMm': 0 if row[19] == "" or row[19].lower() == "nan" or row[19] == "#DIV/0!" else float(row[19]) , 
                                'ShiftTimePerH': 0 if row[20] == "" or row[20].lower() == "nan" or row[20] == "#DIV/0!" else float(row[20]) , 
                                'Netto_StandardTimePerH': 0 if row[21] == "" or row[21].lower() == "nan" or row[21] == "#DIV/0!" else float(row[21]) , 
                                'DowntimePerH': 0 if row[22] == "" or row[22].lower() == "nan" or row[22] == "#DIV/0!" else float(row[22]) , 
                                'Dw_J1': 0 if row[23] == "" or row[23].lower() == "nan" or row[23] == "#DIV/0!" else float(row[23]) , 
                                'Planned_DowntimePerH': 0 if row[24] == "" or row[24].lower() == "nan" or row[24] == "#DIV/0!" else float(row[24]) , 
                                'Unplanned_DowntimePerH': 0 if row[25] == "" or row[25].lower() == "nan" or row[25] == "#DIV/0!" else float(row[25]) , 
                                'Urgent_Orders_A16': 0 if row[26] == "" or row[26].lower() == "nan" or row[26] == "#DIV/0!" else float(row[26]) , 
                                'Disciplinary_responsible_vs_Shifthours': 0 if row[27] == "" or row[27].lower() == "nan" or row[27] == "#DIV/0!" else float(row[27]) , 
                                'Not_disciplinary_responsible_vs_Shifthours': 0 if row[28] == "" or row[28].lower() == "nan" or row[28] == "#DIV/0!" else float(row[28]) , 
                                'No_Orders': 0 if row[29] == "" or row[29].lower() == "nan" or row[29] == "#DIV/0!" else float(row[29]) , 
                                'ScrapPerM': 0 if row[30] == "" or row[30].lower() == "nan" or row[30] == "#DIV/0!" else float(row[30]) , 
                                'Scrap': 0 if row[31] == "" or row[31].lower() == "nan" or row[31] == "#DIV/0!" else float(row[31]) , 
                                'Avg_No_Machines': 0 if row[32] == "" or row[32].lower() == "nan" or row[32] == "#DIV/0!" else float(row[32]) , 
                                'Somme_de_Dw_B': 0 if row[33] == "" or row[33].lower() == "nan" or row[33] == "#DIV/0!" else float(row[33]), 
                                'shift': int(row[34]),
                                'Day': int(row[35]), 
                                'Month': int(row[36]),
                                'Year': int(row[37]),
                                'Date': row[38].split(' ')[0]
                }
                   
            transformed_data.append(transformed_row)
        del results

        return transformed_data
    except mysql.connector.Error as error:
        return jsonify({'error': str(error), 'code': 1})

@app.route('/sql/average/oee/lastWeek', methods=['GET'])
@app.route('/sql/average/oee/lastWeek/<serialNumber>/<shift>', methods=['GET'])
def get_last_week_average_oee_from_mysql(serialNumber=None, shift=None):
    try:
        date_condition = f" where Date >= ( SELECT MAX(Date) FROM {OEE_MYSQL_TABLE_NAME}  ) - INTERVAL 7 DAY AND date <= ( SELECT MAX(date) FROM {OEE_MYSQL_TABLE_NAME}  )"
        where_serialNumber_condition =  " and Serial_Number='"+serialNumber+"'" if serialNumber is not None else ""
        where_shift_condition =  " and shift="+shift if shift is not None else ""
        where_condition = date_condition + where_serialNumber_condition + where_shift_condition
        results =  query_mysql_database(f"SELECT Date, round(AVG(OEE), 2) FROM {OEE_MYSQL_TABLE_NAME}" + where_condition + " GROUP BY Date ORDER BY Date ASC")
        transformed_data = []
        for row in results:
            transformed_row = { 
                'date': row[0].split(' ')[0],
                'value': float(row[1]) if row[1] != '#DIV/0!' else 0
                }   
            transformed_data.append(transformed_row)
        del results

        return transformed_data
    except mysql.connector.Error as error:
        return jsonify({'error': str(error), 'code': 1})

@app.route('/sql/oee/number_machines/lastWeek', methods=['GET'])
def get_number_of_cutting_area_machines():
    try:
        date_condition = f" where Date >= ( SELECT MAX(Date) FROM {OEE_MYSQL_TABLE_NAME} ) - INTERVAL 7 DAY AND date <= ( SELECT MAX(date) FROM {OEE_MYSQL_TABLE_NAME} )"
        where_condition = date_condition
        query = f"SELECT count(distinct Serial_Number) FROM {OEE_MYSQL_TABLE_NAME}"
        results =  query_mysql_database(query+where_condition)
        
        return jsonify({'data': results[0][0], 'code': 0})
    except mysql.connector.Error as error:
        return jsonify({'error': str(error), 'code': 1})
    
@app.route('/sql/oee/top5/downtime/lastWeek', methods=['GET'])
def get_top_5_downtime_machines_of_cutting_area():
    try:
        date_condition = f" where Date >= ( SELECT MAX(Date) FROM {OEE_MYSQL_TABLE_NAME}  ) - INTERVAL 7 DAY AND date <= ( SELECT MAX(date) FROM {OEE_MYSQL_TABLE_NAME}  )"
        where_condition = date_condition + " " + "GROUP BY Serial_Number ORDER BY downtime DESC LIMIT 5"
        query = f"SELECT Serial_Number, round(AVG(Planned_Downtime+Unplanned_Downtime), 2) as downtime, round(AVG(oee), 2) as oee FROM {OEE_MYSQL_TABLE_NAME}"
        results =  query_mysql_database(query+where_condition)
        transformed_data = []
        oee_list = []
        for row in results:
            transformed_data.append({
                "label" : row[0],
                "value" : row[1]
            })
            oee_list.append(row[2])
        del results
        return jsonify({'data': transformed_data, 'oee':oee_list, 'code': 0})
    except mysql.connector.Error as error:
        return jsonify({'error': str(error), 'code': 1})

@app.route('/sql/oee/top5/oee/lastWeek', methods=['GET'])
def get_top_5_oee_machines_of_cutting_area():
    try:
        date_condition = f" where Date >= ( SELECT MAX(Date) FROM {OEE_MYSQL_TABLE_NAME}  ) - INTERVAL 7 DAY AND date <= ( SELECT MAX(date) FROM {OEE_MYSQL_TABLE_NAME}  )"
        where_condition = date_condition + " " + "GROUP BY Serial_Number ORDER BY oee DESC LIMIT 5"
        query = f"SELECT Serial_Number, round(AVG(oee), 2) as oee FROM {OEE_MYSQL_TABLE_NAME}"
        results =  query_mysql_database(query+where_condition)
        transformed_data = []
        for row in results:
            transformed_data.append({
                "label" : row[0],
                "value" : row[1]
            })
        del results
        return jsonify({'data': transformed_data, 'code': 0})
    except mysql.connector.Error as error:
        return jsonify({'error': str(error), 'code': 1})








# ------------------------ KPI Efficience E1 : Urls ------------------------



@app.route('/sql/efficience/efficience_unique_dates', methods=['GET'])
def get_efficience_unique_dates():
    try :
        results =  query_mysql_database(f"SELECT distinct date FROM {EFFICIENCE_MYSQL_TABLE_NAME}")
        dates = []
        for row in results:
            dates.append(row[0])
        del results
        return dates
    except mysql.connector.Error as error:
        return jsonify({'error': str(error), 'code': 1})

@app.route('/sql/efficience', methods=['GET'])
@app.route('/sql/efficience/<project>', methods=['GET'])
def get_kpi_efficience_data_from_mysql(project = None):
    try:
        project_query = " where Project='"+ project +"'" if project is not None else ""
        query = f'SELECT * FROM {EFFICIENCE_MYSQL_TABLE_NAME}'
        efficience_data = query_mysql_database(query+project_query)
        transformed_data = []
        for row in efficience_data:
            transformed_row = {
                "Project": row[1],
                "Responsible": row[2],
                "Date": row[3],
                "Efficience": row[4],
                "Headcount": row[5],
                "PostedHours": row[6],
                "HoursOfProduction": row[7],
                "Absents": row[8],
                "RegistredHeadCount": row[9],
            }
            transformed_data.append(transformed_row)

        del efficience_data

        # Renvoyez les données au format JSON
        return jsonify({'data': transformed_data, 'code': 0})  # Custom backend code: data found

    except mysql.connector.Error as error:
        # En cas d'erreur, renvoyez une réponse d'erreur au format JSON
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error

@app.route('/sql/averages/efficience/lastWeek', methods=['GET'])
def get_averages_efficience_data_from_mysql():
    try:
        where_condition = f" where Date >= ( SELECT MAX(Date) FROM {EFFICIENCE_MYSQL_TABLE_NAME} ) - INTERVAL 7 DAY AND date <= ( SELECT MAX(date) FROM {EFFICIENCE_MYSQL_TABLE_NAME} ) GROUP BY Date ORDER BY Date ASC"
        query = f'SELECT Date, round(AVG(Efficience), 2) FROM {EFFICIENCE_MYSQL_TABLE_NAME} '
        efficience_data = query_mysql_database(query+where_condition)
        transformed_data = []
        for row in efficience_data:
            transformed_row = {
                "date": row[0],
                "value": row[1],
            }
            transformed_data.append(transformed_row)

        del efficience_data

        # Renvoyez les données au format JSON
        return jsonify({'data': transformed_data, 'code': 0})  # Custom backend code: data found

    except mysql.connector.Error as error:
        # En cas d'erreur, renvoyez une réponse d'erreur au format JSON
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error

@app.route('/sql/average/efficience/lastWeek', methods=['GET'])
def get_average_efficience_data_from_mysql():
    try:
        where_condition = f" where Date >= ( SELECT MAX(Date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) - INTERVAL 7 DAY AND date <= ( SELECT MAX(date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) "
        query = f'SELECT round(AVG(Efficience), 2) FROM {EFFICIENCE_MYSQL_TABLE_NAME} '
        efficience_data = query_mysql_database(query+where_condition)
        
        # Renvoyez les données au format JSON
        return jsonify({'data': efficience_data[0][0], 'code': 0})  # Custom backend code: data found

    except mysql.connector.Error as error:
        # En cas d'erreur, renvoyez une réponse d'erreur au format JSON
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error

@app.route('/sql/average/productionHours/lastWeek', methods=['GET'])
def get_average_production_hours_for_last_week_data_from_mysql():
    try:
        where_condition = f" where Date >= ( SELECT MAX(Date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) - INTERVAL 7 DAY AND date <= ( SELECT MAX(date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) "
        query = f'SELECT round(AVG(HoursOfProduction), 2) FROM {EFFICIENCE_MYSQL_TABLE_NAME} '
        efficience_data = query_mysql_database(query+where_condition)
        
        # Renvoyez les données au format JSON
        return jsonify({'data': efficience_data[0][0], 'code': 0})  # Custom backend code: data found

    except mysql.connector.Error as error:
        # En cas d'erreur, renvoyez une réponse d'erreur au format JSON
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error

@app.route('/sql/average/productionHours/weekBeforelastWeek', methods=['GET'])
def get_average_production_hours_for_week_before_last_week_data_from_mysql():
    try:
        where_condition = f" where Date >= ( SELECT MAX(Date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) - INTERVAL 21 DAY AND date <= ( SELECT MAX(date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) - INTERVAL 14 DAY AND date"
        query = f'SELECT round(AVG(HoursOfProduction), 2) FROM {EFFICIENCE_MYSQL_TABLE_NAME} '
        efficience_data = query_mysql_database(query+where_condition)
        
        # Renvoyez les données au format JSON
        return jsonify({'data': efficience_data[0][0], 'code': 0})  # Custom backend code: data found

    except mysql.connector.Error as error:
        # En cas d'erreur, renvoyez une réponse d'erreur au format JSON
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error

@app.route('/sql/average/efficience/weekbeforlastWeek', methods=['GET'])
def get_average_efficience_week_befor_last_week_data_from_mysql():
    try:
        where_condition = f" where Date >= ( SELECT MAX(Date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) - INTERVAL 21 DAY AND date <= ( SELECT MAX(date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) - INTERVAL 14 DAY AND date"
        query = f'SELECT round(AVG(Efficience),2) FROM {EFFICIENCE_MYSQL_TABLE_NAME} '
        efficience_data = query_mysql_database(query+where_condition)
        
        # Renvoyez les données au format JSON
        return jsonify({'data': efficience_data[0][0], 'code': 0})  # Custom backend code: data found

    except mysql.connector.Error as error:
        # En cas d'erreur, renvoyez une réponse d'erreur au format JSON
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error

@app.route('/sql/production_hours_per_project/efficience/lastWeek_and_beforlastWeek', methods=['GET'])
def get_production_hours_per_family_data_lastWeek_and_beforlastWeek_from_mysql():
    try:
        # ---------------------------------------------------------
        # First Query
        where_condition = f" where Date >= ( SELECT MAX(Date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) - INTERVAL 7 DAY AND date <= ( SELECT MAX(date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) GROUP BY Project ORDER BY average_hp DESC"
        query = f'SELECT Project , ROUND(SUM(HoursOfProduction), 2) as average_hp FROM {EFFICIENCE_MYSQL_TABLE_NAME} '
        efficience_data = query_mysql_database(query+where_condition)
        transformed_data_1 = []
        for row in efficience_data:
            transformed_row = {
                "label": row[0],
                "value": row[1],
            }
            transformed_data_1.append(transformed_row)
        del efficience_data
        # ---------------------------------------------------------
        # Second Query
        where_condition = f" where Date >= ( SELECT MAX(Date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) - INTERVAL 14 DAY AND date <= ( SELECT MAX(date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) - INTERVAL 7 DAY AND date GROUP BY Project ORDER BY average_hp DESC"
        query = f'SELECT Project , ROUND(SUM(HoursOfProduction), 2) as average_hp FROM {EFFICIENCE_MYSQL_TABLE_NAME} '
        efficience_data = query_mysql_database(query+where_condition)
        transformed_data_2 = []
        for row in efficience_data:
            transformed_row = {
                "label": row[0],
                "value": row[1],
            }
            transformed_data_2.append(transformed_row)

        del efficience_data
        # ---------------------------------------------------------
        # Combine and Re-sutructure the results
        # Traiter les résultats pour obtenir les listes de valeurs et de labels correspondantes
        labels = []
        values1 = []
        values2 = []
        
        for item in transformed_data_1:
            labels.append(item["label"])
            values1.append(item["value"])


        label_found = False
        for label in labels:
            label_found = False
            for item in transformed_data_2:
                if item["label"] == label:
                    values2.append(item["value"])
                    label_found = True
                    break
            if not label_found:
                values2.append(0)
            

        # Créer le nouvel objet JSON avec les propriétés "data" et "secondData"
        combined_result = {
            "code": 0,
            "firstData": [{"label": label, "value": value} for label, value in zip(labels, values1)],
            "secondData": values2
        }

        # ---------------------------------------------------------
        # Renvoyez les données au format JSON
        return jsonify(combined_result)  # Custom backend code: data found

    except mysql.connector.Error as error:
        # En cas d'erreur, renvoyez une réponse d'erreur au format JSON
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error

@app.route('/sql/production_hours_per_responsible/efficience/lastWeek_and_beforlastWeek', methods=['GET'])
def get_production_hours_per_responsible_data_lastWeek_and_beforlastWeek_from_mysql():
    try:
        # ---------------------------------------------------------
        # First Query
        where_condition = f" where Date >= ( SELECT MAX(Date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) - INTERVAL 7 DAY AND date <= ( SELECT MAX(date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) GROUP BY Responsible ORDER BY average_hp DESC"
        query = f'SELECT Responsible , ROUND(SUM(HoursOfProduction), 2) as average_hp FROM {EFFICIENCE_MYSQL_TABLE_NAME} '
        efficience_data = query_mysql_database(query+where_condition)
        transformed_data_1 = []
        for row in efficience_data:
            transformed_row = {
                "label": row[0],
                "value": row[1],
            }
            transformed_data_1.append(transformed_row)
        del efficience_data
        # ---------------------------------------------------------
        # Second Query
        where_condition = f" where Date >= ( SELECT MAX(Date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) - INTERVAL 14 DAY AND date <= ( SELECT MAX(date) FROM {EFFICIENCE_MYSQL_TABLE_NAME}  ) - INTERVAL 7 DAY AND date GROUP BY Responsible ORDER BY average_hp DESC"
        query = f'SELECT Responsible , ROUND(SUM(HoursOfProduction), 2) as average_hp FROM {EFFICIENCE_MYSQL_TABLE_NAME} '
        efficience_data = query_mysql_database(query+where_condition)
        transformed_data_2 = []
        for row in efficience_data:
            transformed_row = {
                "label": row[0],
                "value": row[1],
            }
            transformed_data_2.append(transformed_row)

        del efficience_data
        # ---------------------------------------------------------
        # Combine and Re-sutructure the results
        # Traiter les résultats pour obtenir les listes de valeurs et de labels correspondantes
        labels = []
        values1 = []
        values2 = []
        
        for item in transformed_data_1:
            labels.append(item["label"])
            values1.append(item["value"])


        label_found = False
        for label in labels:
            label_found = False
            for item in transformed_data_2:
                if item["label"] == label:
                    values2.append(item["value"])
                    label_found = True
                    break
            if not label_found:
                values2.append(0)
            

        # Créer le nouvel objet JSON avec les propriétés "data" et "secondData"
        combined_result = {
            "code": 0,
            "firstData": [{"label": label, "value": value} for label, value in zip(labels, values1)],
            "secondData": values2
        }

        # ---------------------------------------------------------
        # Renvoyez les données au format JSON
        return jsonify(combined_result)  # Custom backend code: data found

    except mysql.connector.Error as error:
        # En cas d'erreur, renvoyez une réponse d'erreur au format JSON
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error

@app.route('/sql/efficience_full_data', methods=['GET'])
@app.route('/sql/efficience_full_data/<description_filter>', methods=['GET'])
@app.route('/sql/efficience_full_data/<description_filter>/<project>', methods=['GET'])
def get_efficience_data_from_mysql(description_filter = None, project = None):
    try:

        if description_filter is None:
            description_filter = 'efficience'

        project_query = " where project='"+ project +"'" if project is not None else ""
        efficience_data = query_mysql_database(f'SELECT date, {description_filter}, project, responsible FROM {EFFICIENCE_MYSQL_TABLE_NAME} '+project_query)
        
        # Transformez les données récupérées dans le format souhaité
        transformed_data = []
        for row in efficience_data:
            transformed_row = {
                "date": row[0],
                "value": row[1],
                "project": row[2],
                "responsible": row[3]
            }
            transformed_data.append(transformed_row)

        del efficience_data

        # Renvoyez les données au format JSON
        return jsonify({'data': transformed_data, 'code': 0})  # Custom backend code: data found

    except mysql.connector.Error as error:
        # En cas d'erreur, renvoyez une réponse d'erreur au format JSON
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error



# def first_configuration_data_preprocessing(__X_dataset, encoder_object, scaler_object):

#     X_dataset = __X_dataset.copy()
#     X_dataset['Date'] = pd.to_datetime(X_dataset['Date'])
#     X_dataset['Week'] = X_dataset['Date'].isocalendar().week
#     X_dataset['Year'] = X_dataset['Date'].year
#     X_dataset['Month'] = X_dataset['Date'].month
#     X_dataset['Day'] = X_dataset['Date'].day
#     X_dataset['Week Day'] = X_dataset['Date'].day_of_week
#     X_dataset.drop(columns=['Date'], inplace=True)

#     columns = ['Project', 'Responsible', 'Date', 'Headcount', 'Posted Hours', 'Hours of production', 'Absents', 'Registred Headcount', 'Week', 'Year', 'Month', 'Day', 'Week Day']
#     X_dataset =  pd.DataFrame(X_dataset.to_numpy().reshape(1, -1), columns=columns)

#     columns_to_normalize = ['Headcount', 'Posted Hours', 'Hours of production', 'Absents', 'Registred Headcount', 'Week',
#                             'Year', 'Month', 'Day', 'Week Day']
#     categorical_columns = ['Project', 'Responsible']

#     X_dataset[columns_to_normalize] = scaler_object.transform(X_dataset[columns_to_normalize].to_numpy().reshape(1, -1))[0]

#     one_hot_encoder_columns = ['Project_AFTER MARKET', 'Project_C1', 'Project_C3 EQ1',
#        'Project_C3 EQ2', 'Project_C3 EQ3', 'Project_CNHI EQ1',
#        'Project_CNHI EQ2', 'Project_HDEP LAD', 'Project_JCB',
#        'Project_MAN', 'Project_MDEP', 'Project_PROTO',
#        'Project_SMALL EQ1', 'Project_SMALL EQ2', 'Project_STEP E',
#        'Project_VCE', 'Responsible_bartit anas', 'Responsible_bendriss',
#        'Responsible_chakir', 'Responsible_faicel',
#        'Responsible_marjany hayat', 'Responsible_rizki fatiha',
#        'Responsible_sabri', 'Responsible_sadik hicham',
#        'Responsible_sibari', 'Responsible_zaiti amine', 'Responsible_nan']

#     one_hot_encoder_df = pd.DataFrame(data=0, columns=one_hot_encoder_columns, index=X_dataset.index, dtype=int)

#     one_hot_encoder_df_2 = pd.concat([X_dataset[['Headcount', 'Posted Hours', 'Hours of production', 'Absents', 'Registred Headcount', 'Week', 'Year',
#           'Month', 'Day', 'Week Day']], one_hot_encoder_df], axis=1)

#     X_dataset.drop(columns=['Date'], inplace=True)
    
#     df_dummies = pd.get_dummies(X_dataset)
#     df_dummies.drop(columns=['Headcount',	'Posted Hours',	'Hours of production',	'Absents',	'Registred Headcount',	'Week',	'Year',	'Month',	'Day',	'Week Day'], inplace=True)
#     one_hot_encoder_df_2[df_dummies.columns[0]] = 1
#     one_hot_encoder_df_2[df_dummies.columns[1]] = 1    
#     return one_hot_encoder_df_2


# @app.route('/sql/predict_efficience', methods=['GET'])
# @app.route('/sql/predict_efficience/<project>', methods=['GET'])
# def get_efficience_predicted_data_from_mysql(project = None):
#     try:

#         project_query = " where project='"+ project +"'" if project is not None else ""
#         where_condition = project_query + " order by date DESC limit 1"
#         efficience_data = query_mysql_database(f'SELECT Project, Responsible, Date, headcount, postedHours, HoursOfProduction, Absents, RegistredHeadcount FROM {EFFICIENCE_MYSQL_TABLE_NAME} '+where_condition)
        
#         # Transformez les données récupérées dans le format souhaité
#         transformed_data = []
#         for row in efficience_data:
#             transformed_row = {
#                 "Project": row[0],
#                 "Responsible": row[1],
#                 "Date": row[2],
#                 "headcount": row[3],
#                 "postedHours": row[4],
#                 "HoursOfProduction": row[5],
#                 "Absents": row[6],
#                 "RegistredHeadcount": row[7]
#             }
#             transformed_data.append(transformed_row)

#         del efficience_data
#         new_row = transformed_data[0]
#         new_row["Hours of production"] = new_row["HoursOfProduction"]
#         new_row["Registred Headcount"] = new_row["RegistredHeadcount"]
#         new_row["Headcount"] = new_row["headcount"]
#         new_row["Posted Hours"] = new_row["postedHours"]
#         new_row.pop("HoursOfProduction")
#         new_row.pop("RegistredHeadcount")
#         new_row.pop("headcount")
#         new_row.pop("postedHours")

#         # Créez une série pandas à partir du dictionnaire
#         serie = pd.Series(new_row)
#         # Réorganisez la série selon l'ordre souhaité des colonnes
#         serie = serie[["Project", "Responsible", "Date", "Headcount", "Posted Hours", "Hours of production", "Absents", "Registred Headcount"]]
        
#         model_random_forest_regressor = pickle.load(open(random_forest_regressor_file_path, 'rb'))
#         loaded_scaler_first_config = joblib.load(scaler_file_path)
#         loaded_encoder_first_config = joblib.load(encoder_file_path)
#         y_train_labels_scaler = joblib.load(labels_scaler_file_path)

#         new_row_x_after_feature_enginnering = first_configuration_data_preprocessing(serie, loaded_encoder_first_config, loaded_scaler_first_config)
#         y_pred_FC_1_scaled = model_random_forest_regressor.predict(new_row_x_after_feature_enginnering)
#         y_pred_FC_1 = y_train_labels_scaler.inverse_transform(y_pred_FC_1_scaled.reshape(-1, 1)).reshape(y_pred_FC_1_scaled.shape[0],)

#         # Renvoyez les données au format JSON
#         return jsonify({'data': {
#             "value": str(y_pred_FC_1[0]),
#             "date": str(datetime.strptime(serie['Date'], '%Y-%m-%d') + timedelta(days=1)).split(' ')[0]
#         }, 'code': 0})  # Custom backend code: data found

#     except mysql.connector.Error as error:
#         # En cas d'erreur, renvoyez une réponse d'erreur au format JSON
#         return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error















@cross_origin
@app.route('/sql/uniqueEfficienceProject', methods=['GET'])
def get_uniqueFamilies():
    try:
        unique_families_data = query_mysql_database(f'select distinct Project from {EFFICIENCE_MYSQL_TABLE_NAME} ')
        res=[]
        for e in unique_families_data:
            res.append(e[0])

        return jsonify({'data': res, 'code': 0})  # Custom backend code: data found

    except mysql.connector.Error as error:
        # En cas d'erreur, renvoyez une réponse d'erreur au format JSON
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error

@cross_origin
@app.route('/@me')
def get_current_user():
    user_id = session.get("user_id")

    if not user_id:
        return jsonify({
            "error": "You're not logged in"
        }), 401
    
    user = User.query.filter_by(id=user_id).first()
    return jsonify({
        "id":user.id,
        "email":user.email
    })

@cross_origin
@app.route('/register', methods=['POST'])
def register_user():
    email = request.json['email']
    password = request.json['password']

    user_exists = User.query.filter_by(email=email).first() is not None

    if user_exists:
        return jsonify({"error": "User already exists"}), 409

    hashed_password = bcrypt.generate_password_hash(password)
    new_user = User(email=email, password=hashed_password)
    db.session.add(new_user)
    db.session.commit()
    
    return jsonify({
        "id":new_user.id,
        "email":new_user.email
    })

@cross_origin
@app.route("/login", methods=['POST'])
def login_user():
    email = request.json['email']
    password = request.json['password']

    user = User().query.filter_by(email=email).first()

    # Uilisateur non existé
    if user is None:
        return jsonify({"error":"Unauthorized : user not exist !"}) , 401 # netword code
    
    # Vérifier le password
    if not bcrypt.check_password_hash(user.password, password):
        return jsonify({"error":"Unauthorized : invalid Password !",
                        "code":100}) # custom backend code : invalid password
    

    # put data in session
    session['user_id'] = user.id


    return jsonify({
        "id":user.id,
        "email":user.email,
        "code":0 # custom backend code : data (user) found
    })

@cross_origin
@app.route("/logout", methods=['POST'])
def logout_user():
    session.pop('user_id')
    return "200"

@cross_origin
@app.route('/api/kpi/efficience/<description>', methods=['GET'])
@app.route('/api/kpi/efficience/<description>/<family>', methods=['GET'])
def get_kpi_data(description=None, family=None):
    e1_data = load_e1_data(e1_dataset_file_path,description=description, family=family)
    return jsonify({'data': e1_data, 
                    'code': 0}) # custom backend code : data found

@cross_origin
@app.route('/uniqueCuttingAreaSerialNumbers', methods=['GET'])
def get_uniqueCuttingAreaSerialNumbers():
    return get_unique_from_oee_dataset(oee_dataset_file_path)
    



"""
#################################       Real Time Data Handlers       #################################  
"""
def get_selectedBranch_and_selectedFamily():
    try:
        with open('branch_family_state.json', 'r') as file:
            data = json.load(file)
        return jsonify({'data': [data], 'code': 0})  # Custom backend code: data found
    except IOError as error:
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error

@socketio.on('update_description_family')
def update_branch_family_into_database(data):
    try:
        with open('branch_family_state.json', 'w') as file:
            json.dump({"selectedBranch":data['url_extension'],"selectedFamily":data['family'][0]}, file)
        print("Data written to branch_family_state.json successfully")
    except IOError as error:
        print("\n \n \n IOError at update_branch_family_into_file function:", str(error))

# @cross_origin
# @app.route('/last', methods=['GET'])
def get_infos_about_last_updates_in_database(__KPI="efficience"):
    select_database = {
        "efficience" : EFFICIENCE_MYSQL_TABLE_NAME
    }
    realtime_updates_metadata = None
    __is_KPIs_data_updated = False
    try:
        with open('realtime_updates_metadata.json', 'r') as file:
            data = json.load(file)
            realtime_updates_metadata = data
    except IOError as error:
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error
    

    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()
        cursor.execute(f"select count(*) from {select_database[__KPI]}")
        _last_update__max_row = cursor.fetchall()[0][0]
        cursor.close()
        connection.close()

        if realtime_updates_metadata[__KPI] != _last_update__max_row:
            realtime_updates_metadata[__KPI] = _last_update__max_row            
            with open('realtime_updates_metadata.json', 'w') as file:
                json.dump(realtime_updates_metadata, file)
                __is_KPIs_data_updated = True

        return jsonify({'data': __is_KPIs_data_updated, 'code': 0})
        
    except mysql.connector.Error as error:
        return jsonify({'error': str(error), 'code': 1})
    except IOError as error:
        return jsonify({'error': str(error), 'code': 1})  # Custom backend code: error

def is_KPIs_data_updated(__kpi='efficience'):
    database_last_updates_infos = get_infos_about_last_updates_in_database(__kpi)
    return json.loads(database_last_updates_infos.data.decode('utf-8'))['data']

def background_thread():
    with app.app_context():
        while True:
            # Check if data updates in database
            is_data_updated = is_KPIs_data_updated("efficience")
            if is_data_updated:
                b_f_data = get_selectedBranch_and_selectedFamily()       
                selected_branche = json.loads(b_f_data.data.decode('utf-8'))['data'][0]['selectedBranch']
                selected_family = json.loads(b_f_data.data.decode('utf-8'))['data'][0]['selectedFamily']
                efficience_data = get_efficience_for_test_data_from_mysql(description_filter=selected_branche, family=selected_family)
                socketio.emit('updateEfficienceData', {'data': efficience_data.data})
                socketio.sleep(2)
            sleep(5)

"""
Decorator for connect
"""
@socketio.on('connect')
def connect():
    global thread
    print('Client connected')

    global thread
    with thread_lock:
        if thread is None:
            thread = socketio.start_background_task(background_thread)

"""
Decorator for disconnect
"""
@socketio.on('disconnect')
def disconnect():
    print('Client disconnected',  request.sid)



if __name__ == '__main__':
    # app.run(debug=True)
    socketio.run(app)
