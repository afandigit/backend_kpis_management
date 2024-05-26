# Étape 1 : Préparation des données à partir du fichier CSV
import csv

oee_dataset_columns_name = ['BU', 'Machine_Type', 'Serial_number',
       'Unused_machines_J1', 'Unused_machines_UNP', 'Unused_machines_PL',
       'OEE', 'OEE-J1', 'OEE-J1-H1-G+G6', 'Availability', 'Performance',
       'Planned_Downtime%', 'Unplanned_Downtime%', 'Performance_losses',
       'Batch_size', 'Setuptime/min', 'Waitingtime/min', 'Quantity/h',
       'Quantity', 'Avg_length/mm', 'ShiftTime/h', 'Netto_StandardTime/h',
       'Downtime/h', 'Dw_J1', 'Planned_Downtime/h', 'Unplanned_Downtime/h',
       'Urgent_Orders(A16)%', 'Disciplinary_responsible_vs_Shifthours',
       'Not_disciplinary_responsible_vs_Shifthours', 'No_Orders', 'Scrap/m',
       'Scrap%', 'Avg_No_Machines', 'Somme de Dw_B', 'shift', 'Day', 'Month',
       'Year', 'Date']

reverseColumnName = {
    'BU' : 'BU' ,
    'Machine_Type' : 'Machine_Type' ,
    'Serial_number' : 'Serial_Number' ,
    'Unused_machines_J1' : 'Unused_Machines_J1' ,
    'Unused_machines_UNP' : 'Unused_Machines_UNP' ,
    'Unused_machines_PL' : 'Unused_Machines_PL' ,
    'OEE' : 'OEE' ,
    'OEE-J1' : 'OEE_J1' ,
    'OEE-J1-H1-G+G6' : 'OEE_J1_H1_G_G6' ,
    'Availability' : 'Availability' ,
    'Performance' : 'Performance' ,
    'Planned_Downtime%' : 'Planned_Downtime' ,
    'Unplanned_Downtime%' : 'Unplanned_Downtime' ,
    'Performance_losses' : 'Performance_losses' ,
    'Batch_size' : 'Batch_size' ,
    'Setuptime/min' : 'SetuptimePerMin' ,
    'Waitingtime/min' : 'WaitingtimePerMin' ,
    'Quantity/h' : 'QuantityPerH' ,
    'Quantity' : 'Quantity' ,
    'Avg_length/mm' : 'Avg_lengthPerMm' ,
    'ShiftTime/h' : 'ShiftTimePerH' ,
    'Netto_StandardTime/h' : 'Netto_StandardTimePerH' ,
    'Downtime/h' : 'DowntimePerH' ,
    'Dw_J1' : 'Dw_J1' ,
    'Planned_Downtime/h' : 'Planned_DowntimePerH' ,
    'Unplanned_Downtime/h' : 'Unplanned_DowntimePerH' ,
    'Urgent_Orders(A16)%' : 'Urgent_Orders_A16' ,
    'Disciplinary_responsible_vs_Shifthours' : 'Disciplinary_responsible_vs_Shifthours' ,
    'Not_disciplinary_responsible_vs_Shifthours' : 'Not_disciplinary_responsible_vs_Shifthours' ,
    'No_Orders' : 'No_Orders' ,
    'Scrap/m' : 'ScrapPerM' ,
    'Scrap%' : 'Scrap' ,
    'Avg_No_Machines' : 'Avg_No_Machines' ,
    'Somme de Dw_B' : 'Somme_de_Dw_B' ,
    'shift' : 'shift' ,
    'Day' : 'Day' ,
    'Month' : 'Month' ,
    'Year' : 'Year' ,
    'Date' : 'Date' }



def load_OEE_data(file_path, serial_number=None):
    data = []

    with open(file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        
        for row in reader:
            if serial_number != None:
                if serial_number != row['Serial_number']:
                    continue

            # Création des variables à partir du tableau et affectation des valeurs
            # for column in oee_dataset_columns_name:
            #     globals()[column] = row[column]
            variables_dict = {}

            for column in oee_dataset_columns_name:
                if row[column] == '#DIV/0!' or  row[column] == "nan":
                    row[column] = 0
                if column in ['BU', 'Machine_Type', 'Serial_number']:
                    variables_dict[reverseColumnName[column]] = row[column]
                elif column == 'Date':
                    variables_dict[reverseColumnName[column]] = row[column].split(" ")[0]
                else:
                    variables_dict[reverseColumnName[column]] = float(row[column])

            data.append(variables_dict)

    return data

def get_unique_from_oee_dataset(file_path, column='Serial_number'):
    data = []
    with open(file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for r in reader:
            data.append(r[column])
    return list(set(data))

