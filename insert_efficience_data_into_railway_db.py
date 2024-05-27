import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import Error

# Lire le fichier CSV
file_path = './database/efficience.csv'
df = pd.read_csv(file_path, sep=';')

# Détails de connexion à la base de données
host = ''
database = ''
user = ''
password = ''

try:
    # Créer une connexion SQLAlchemy engine
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}', echo=False)

    # Insérer les données dans la table
    table_name = 'efficience'
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

    print("Données insérées avec succès dans la table")

except Error as e:
    print(f"Erreur : {e}")

finally:
    # Fermer la connexion
    if (engine):
        engine.dispose()
        print("Connexion fermée")
