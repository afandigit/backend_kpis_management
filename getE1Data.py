# Étape 1 : Préparation des données à partir du fichier CSV
import csv

def load_e1_data(file_path, description='E1', family=None):
    data = []

    with open(file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            if row['Description'] == description:
                # Ajouter les propriétés spécifiques de la KPI
                _family = row['Family']
                if family is not None and _family != family:
                    continue
                date = row['Date']
                value = row['e1']
                responsable = row['Resp']
            
                # Ajouter les données à la liste
                data.append({'date': date,
                            'e1_value': value,
                            'family':_family,
                            'responsable':responsable})

    return data

def get_unique_from_e1_dataset(file_path, column='Family'):
    data = []
    with open(file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for r in reader:
            data.append(r[column])
    return list(set(data))


# # Utilisation de la fonction pour charger les données du CSV
# e1_data = load_e1_data('../DataSource/full_data_V2.csv')
# print(e1_data)


# Étape 2 : Création d'une API Flask pour récupérer les données

# from flask import Flask, jsonify

# app = Flask(__name__)

# @app.route('/api/kpi', methods=['GET'])
# def get_kpi_data():
#     # Retourner les données de la KPI sous forme de JSON
#     return jsonify(csv_data)

# if __name__ == '__main__':
#     app.run()
