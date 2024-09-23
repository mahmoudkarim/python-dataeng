import json
import csv
import os
import re
import io
import shutil
import ftfy

import apache_beam as beam

class CleanData(beam.DoFn):
    def process(self, file_path):
        # On recupère le nom du ficher avec et sans extension
        file_name = os.path.basename(file_path)
        file_name_without_ext = os.path.splitext(file_name)[0]

        # Creation du dossier pour les fichiers nettoyés
        output_dir = os.path.join('data', 'pre-processing', file_name_without_ext) 
        os.makedirs(output_dir, exist_ok=True)
       
        # Chemin du fichier de sortie
        output_path = os.path.join(output_dir, file_name)

        # Dossier pour les fichiers traités
        old_dir = os.path.join('data','raw_data', 'old', file_name_without_ext)
        os.makedirs(old_dir, exist_ok=True)

        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        if file_name.endswith('.csv'):
            cleaned_content = self.clean_csv(content)
        elif file_name.endswith('.json'):
            cleaned_content = self.clean_json(content)
        else:
            cleaned_content = content
        
        # Écrire le contenu nettoyé dans le fichier de sortie
        with open(output_path, 'w', encoding='utf-8') as out_file:
            out_file.write(cleaned_content)
        
        # Déplacer le fichier original dans le dossier 'old'
        old_file_path = os.path.join(old_dir, file_name)
        shutil.move(file_path, old_file_path)
        
        yield output_path

    def clean_csv(self, content):        
        # On utilise ftfy pour corriger les encodages incorrects
        content = ftfy.fix_text(content)
        
        output = io.StringIO()
        reader = csv.reader(io.StringIO(content))
        # On met tout en string dans un premier temps
        writer = csv.writer(output, quoting=csv.QUOTE_ALL)
        
        for row in reader:
            # zap des lignes vide
            if any(field.strip() for field in row):
                # Nettoyer chaque cellule de la ligne
                cleaned_row = [self.clean_cell(cell) for cell in row]
                writer.writerow(cleaned_row)
        
        return output.getvalue()

    def clean_cell(self, cell):
        # Expression régulière pour trouver les séquences du type \x 
        # suivies de deux caractères hexadécimaux non traiter par ftfy
        cell = re.sub(r'\\x[0-9A-Fa-f]{2}', '', cell)
        return cell


    def clean_json(self, content):
        # Supprimer les virgules finales non attendu
        content = re.sub(r',\s*}', '}', content)
        content = re.sub(r',\s*]', ']', content)
        
        # Analyser pour s'assurer que c'est un JSON valide
        try:
            # Décoder et ré-encoder pour gérer les caractères invalides
            content = content.encode('utf-8', errors='replace').decode('utf-8')
            data = json.loads(content)
            # Convertir toutes les valeurs en chaînes de caractères
            data_as_str = self.convert_all_values_to_string(data)
            return json.dumps(data_as_str, indent=2, ensure_ascii=False)
        except json.JSONDecodeError as e:
            print(f"Error in JSON file: {str(e)}")
            return content
        
    def convert_all_values_to_string(self, data):
        # Convertit récursivement toutes les valeurs du JSON en chaînes de caractères
        if isinstance(data, dict):
            return {key: self.convert_all_values_to_string(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.convert_all_values_to_string(item) for item in data]
        else:
            return str(data)

def run_cleaning(file_paths):
    return (
        file_paths
        | "Clean files" >> beam.ParDo(CleanData())
    )