import apache_beam as beam
import os
import yaml
import json
from utils.logger_config import logger
from datetime import datetime

class DataValidator(beam.DoFn):
    def __init__(self, config_path):
        with open(config_path, 'r') as config_file:
            self.config = yaml.safe_load(config_file)['tables']

    def process(self, file_path):
        file_name = self.determine_file_name(file_path)
        
        if file_name in self.config:
            try:
                with open(file_path, 'r') as file:
                    data = json.load(file)
                validated_data, rejected_data = self.validate_data(data, file_name)
                
                if validated_data:
                    # Écrire les données valides dans le fichier des données valide
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(validated_data, f, indent=2, ensure_ascii=False)
                    yield beam.pvalue.TaggedOutput('valid', file_path)
                
                if rejected_data:
                    rejected_file_path = self.save_rejected_data(file_path, rejected_data)
                    yield beam.pvalue.TaggedOutput('invalid', rejected_file_path)
            
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in file: {file_path}")
                self.save_rejected_data(file_path, data)
        else:
            logger.warning(f"Unknown table : {file_path}")
            self.save_rejected_data(file_path, data)

    def determine_file_name(self, file_path):
        base_name = os.path.basename(file_path)
        file_name = os.path.splitext(base_name)[0]
        
        if '__' in file_name:
            file_name = file_name.split('__')[0]
        
        return file_name

    def validate_data(self, data, file_type):
        if isinstance(data, list):
            validated = []
            rejected = []
            for item in data:
                if self.validate_item(item, file_type):
                    validated.append(item)
                else:
                    rejected.append(item)
            return validated, rejected
        else:
            if self.validate_item(data, file_type):
                return [data], []
            else:
                return [], [data]

    def validate_item(self, item, file_type):
        schema = self.config[file_type]['schema']
        validation_rules = self.config[file_type].get('validation', {})
        
        for field, field_type in schema.items():
            value = item.get(field)
            if not self.validate_field(value, field_type, validation_rules.get(field, {})):
                return False
        return True

    def validate_field(self, value, field_type, validation_rules):
        if validation_rules.get('required', False):
            if value is None or (isinstance(value, str) and value.strip() == ""):
                return False
            if field_type == 'integer' and value == 0:
                return False

        if value is not None:
            if field_type == 'string' and not isinstance(value, str):
                return False
            elif field_type == 'integer' and not isinstance(value, int):
                return False
            elif field_type == 'date':
                try:
                    datetime.strptime(value, '%Y-%m-%d')
                except ValueError:
                    return False

        return True

    def save_rejected_data(self, original_file_path, rejected_data):
        # Enrgistrer les données rejeté
        base_name = os.path.basename(original_file_path)
        rejected_dir = os.path.join(os.path.dirname(original_file_path), 'rejected')
        os.makedirs(rejected_dir, exist_ok=True)
        rejected_file_path = os.path.join(rejected_dir, f"rejected_{base_name}")
        
        with open(rejected_file_path, 'w', encoding='utf-8') as f:
            json.dump(rejected_data, f, indent=2,  ensure_ascii=False)
        
        return rejected_file_path

def run_data_validation(input_pcollection):
    return input_pcollection | "Validate Data" >> beam.ParDo(DataValidator("config.yaml")).with_outputs('valid', 'invalid')