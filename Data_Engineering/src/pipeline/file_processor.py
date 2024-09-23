import apache_beam as beam
import os
import json
import csv
import datetime
import uuid
import shutil

class FileProcessor(beam.DoFn):
    def process(self, file_path):
        # On recupere le nom du ficher avec sans extension
        file_name = os.path.basename(file_path)
        file_name_without_ext, file_extension = os.path.splitext(file_name)
        
        # on creer un ID pour l'ajouter au nom de notre fichier (certain on le meme nom)
        unique_id = str(uuid.uuid4())[:8]
        output_dir = self.create_output_path(file_name_without_ext)
        os.makedirs(output_dir, exist_ok=True)
        
        if file_extension.lower() == '.json':
            # si c'est un json on fait rien
            new_file_name = f"{file_name_without_ext}__{unique_id}{file_extension}"
            output_file = os.path.join(output_dir, new_file_name)
            shutil.copy2(file_path, output_file)
        else:
            # Convertion des csv en json
            data = self.transform_file(file_path)
            new_file_name = f"{file_name_without_ext}__{unique_id}.json"
            output_file = os.path.join(output_dir, new_file_name)
            
            with open(output_file, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, indent=2, ensure_ascii=False)
        
        yield output_file

    def transform_file(self, file_path):
        _, file_extension = os.path.splitext(file_path)
        
        if file_extension.lower() == '.csv':
            return self.csv_to_json(file_path)
        else:
            with open(file_path, 'r', encoding='utf-8') as text_file:
                return {"content": text_file.read()}

    def csv_to_json(self, csv_file_path):
        data = []
        with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                data.append({k: v for k, v in row.items()})
        return data

    def create_output_path(self, file_name):
        # creation de dossier Horodaté pour nos fichiers
        now = datetime.datetime.now()
        return os.path.join(
            'data',
            'tmp',
            file_name,
            str(now.year),
            f"{now.month:02d}",
            f"{now.day:02d}",
            f"{now.hour:02d}",
            f"{now.second:02d}"
        )

def run_file_processing(input_files):
    return input_files | "Process Files" >> beam.ParDo(FileProcessor())