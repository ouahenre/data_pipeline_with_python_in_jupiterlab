import glob as g # sert à gérer les fichiers
import pandas as pd
from datetime import datetime

#######extraction
def extract_from_csv(file):
    df=pd.read_csv(file)
    return df

def extract_from_json(file):
    df=pd.read_json(file, lines=true)
    return df

#initiation de dataframe vide qui va combiner les df csv et json

def extract():
    extracted_data=pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    for i in g.glob("raw_data/*.csv"):
        #ignore index pour continuer les index(lignes) et non écraser les index des suivants csv
        extracted_data=extracted_data._append(extract_from_csv(i),ignore_index=True)
    for i in g.glob("raw_data/*.json"):
        #ignore index pour continuer les index(lignes) et non écraser les index des suivants csv
        extracted_data=extracted_data._append(extract_from_json(i),ignore_index=True)
    return extracted_data
    
#######Transformation
def transform(data):
    data['price']=round(data['price'],2)
    data['car_age']=datetime.today().year - data['year_of_manufacture']
    return data
        
#######Load 
def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)

#######Execution processlus ETL(Data Pipeline)
extracted_df=extract()
transformed_df=transform(data=extracted_df)
load_df=load(targetfile="transformed_data/transformed_cars_data.csv",data_to_load=transformed_df)
    