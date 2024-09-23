import apache_beam as beam
import yaml
import json
import os
from dateutil import parser
from utils.logger_config import logger

class DataFormatter(beam.DoFn):
    def __init__(self, config_path):
        with open(config_path, 'r', encoding='utf-8') as config_file:
            # On recupère notre config sur les tables
            self.config = yaml.safe_load(config_file)['tables']

    def process(self, file_path):
        logger.info(f"Processing file: {file_path}")
        file_name = self.determine_file_name(file_path)

        if file_name in self.config:
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                data = json.loads(content)
                formatted_data = self.format_data(data, file_name)
                
                # Creer le chemin pour le fichier formaté
                formatted_dir = os.path.join('data', 'formatted_data', file_name)
                os.makedirs(formatted_dir, exist_ok=True)
                formatted_file_path = os.path.join(formatted_dir, os.path.basename(file_path))
                
                # Ecrire les données formatée
                with open(formatted_file_path, 'w', encoding='utf-8') as f:
                    json.dump(formatted_data, f, indent=2, ensure_ascii=False)
                
                # Supprimer le fichier de tmp
                os.remove(file_path)
                
                yield formatted_file_path
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in file: {file_path}")
                yield file_path
            except IOError:
                logger.error(f"Error reading file: {file_path}")
                yield file_path
        else:
            logger.warning(f"Unknown Table: {file_path}")
            yield file_path

    def determine_file_name(self, file_path):
        base_name = os.path.basename(file_path)
        file_name = os.path.splitext(base_name)[0]
        
        # Recuperer la partie avant le premier '__'
        if '__' in file_name:
            file_name = file_name.split('__')[0]
        
        return file_name

    def format_data(self, data, file_type):
        if isinstance(data, list):
            return [self.format_item(item, file_type) for item in data]
        else:
            return self.format_item(data, file_type)

    def format_item(self, item, file_type):
        formatted_item = {}
        for field, field_config in self.config[file_type]['schema'].items():
            if field in item:
                formatted_item[field] = self.format_field(item[field], field_config)
        return formatted_item

    def format_field(self, value, field_config):
        field_type = field_config
        if field_type == 'string':
            return str(value)
        elif field_type == 'integer':
            try:
                return int(value)
            except ValueError:
                return 0
        elif field_type == 'date':
            try:
                parsed_date = parser.parse(value)
                # Date au format YYYY-MM-DD
                return parsed_date.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                return None
        else:
            return value

def run_data_formatting(input_pcollection):
    return input_pcollection | "Format Data" >> beam.ParDo(DataFormatter("config.yaml"))
