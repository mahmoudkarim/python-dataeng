import os
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from pipeline.data_cleaning import run_cleaning
from pipeline.file_processor import run_file_processing
from pipeline.data_collector import run_data_collection
from pipeline.graph_builder import run_graph_building
from pipeline.data_formatter import run_data_formatting
from pipeline.data_validation import run_data_validation
from utils.logger_config import logger

def run_data_processing_pipeline():
    options = PipelineOptions(runner='DirectRunner')
    
    with beam.Pipeline(options=options) as p:
        # Récupérer tous les fichiers dans le dossier data/raw_data (données brutes)
        raw_data = "data/raw_data"
        file_paths = p | "Get file paths" >> beam.Create([
            os.path.join(raw_data, f) for f in os.listdir(raw_data) 
            if os.path.isfile(os.path.join(raw_data, f))
        ])
        
        # Dans cette etape on nettoie les fichier pour qu'ils soit lisible
        # nettoyage simple (enlever le virgule en trop dans les json, les charactères hexadicimaux, ...)
        cleaned_files = run_cleaning(file_paths)
        
        # Traitement des fichiers nettoyés
        # on transform tout en json 
        processed_files = run_file_processing(cleaned_files)
        
        # Etape de formatage des données
        # en se basans sur le fichier config.yaml on convertie suivant le type de donnée
        formatted_files = run_data_formatting(processed_files)
        
        # Etape de Data validation
        # ici on valide la donnée et on rejette les données incompletes ou inexploitables
        validation_results = run_data_validation(formatted_files)
        
        # Collecte et deduplique la données pour facilement construire notre graphe
        run_data_collection(validation_results['valid'])

def run_graph_building_pipeline():
    options = PipelineOptions(runner='DirectRunner')
    
    with beam.Pipeline(options=options) as p:
        
        # Construction du graphe
        run_graph_building(p)

if __name__ == "__main__":
    logger.info("Starting data processing pipeline")
    run_data_processing_pipeline()
    logger.info("Data processing pipeline completed")
    
    logger.info("Starting graph building pipeline")
    run_graph_building_pipeline()
    logger.info("Graph building pipeline completed")