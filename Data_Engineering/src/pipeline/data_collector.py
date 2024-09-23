import apache_beam as beam
import os
import json

class DataCollector(beam.DoFn):
    def process(self, file_path):
        # Ignorer les fichiers qui ne sont pas dans formatted_data
        if 'formatted_data' not in file_path:
            return

        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            
            # Extraire le nom de base de la table (avant le "__")
            base_name = os.path.basename(file_path).split('__')[0]
            
            yield (base_name, data)

class CombineAndDeduplicate(beam.DoFn):
    """ Dans cette classe on combine les fichier stocker dans formatted data et on les deduplique """
    def process(self, element):
        base_name, data_list = element
        combined_data = []
        seen = set()
        for data in data_list:
            if isinstance(data, list):
                items = data
            else:
                items = [data]
            for item in items:
                # Selection de la clé primaire
                if 'id' in item:
                    key = item['id']
                else :
                    key = item['atccode']
                
                if key not in seen:
                    seen.add(key)
                    combined_data.append(item)
        
        output_file = os.path.join('data', 'graph_data', f'{base_name}.json')
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(combined_data, f, indent=2, ensure_ascii=False)
        
def run_data_collection(valid_files):
    return (
        valid_files
        | "Read and group files" >> beam.ParDo(DataCollector())
        | "Group by base name" >> beam.GroupByKey()
        | "Combine and deduplicate" >> beam.ParDo(CombineAndDeduplicate())
    )