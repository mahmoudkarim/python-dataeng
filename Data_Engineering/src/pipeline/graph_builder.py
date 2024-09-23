import apache_beam as beam
import json
import os
import json
from typing import Dict, Any, List
from utils.logger_config import logger

class GraphBuilder(beam.DoFn):
    def __init__(self):
        self.graph = None

    def setup(self):
        self.graph = {"drugs": {}}

    def process(self, file_path: str):
        # Fonction qui nous permet de construire notre Graph
        logger.info(f"Processing file: {file_path} for graph building")
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                data = json.load(file)
                for item in data:
                    self.process_data(item, file_path)
            yield self.graph
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")

    def process_data(self, data: Dict[str, Any], file_path: str):
        if "clinical_trials" in file_path.lower():
            self.process_clinical_trial(data)
        elif "pubmed" in file_path.lower():
            self.process_pubmed(data)
        elif "drugs" in file_path.lower():
            self.process_drug(data)

    def process_clinical_trial(self, data: Dict[str, Any]):
        # Construction du graph a partir de  clinical_trial
        for drug in self.graph["drugs"]:
            if drug.lower() in data["scientific_title"].lower():
                journal = data["journal"].lower()
                date = data["date"]
                self.graph["drugs"][drug]["clinical_trials"].append({"journal": journal, "date": date})

    def process_pubmed(self, data: Dict[str, Any]):
        # Construction du graph a partir de pubmed
        for drug in self.graph["drugs"]:
            if drug.lower() in data["title"].lower():
                journal = data["journal"].lower()
                date = data["date"]
                self.graph["drugs"][drug]["pubmed"].append({"journal": journal, "date": date})

    def process_drug(self, data: Dict[str, Any]):
        # Construction du graph a partir de drugs
        drug = data["drug"].lower()
        if drug not in self.graph["drugs"]:
            self.graph["drugs"][drug] = {"pubmed": [], "clinical_trials": []}

def combine_graphs(graphs: List[Dict[str, Any]]):
    # combinaison des graphs
    combined = {"drugs": {}}
    for graph in graphs:
        for drug, drug_data in graph.get("drugs", {}).items():
            if drug not in combined["drugs"]:
                combined["drugs"][drug] = {"pubmed": [], "clinical_trials": []}
            for key in ["pubmed", "clinical_trials"]:
                if key in drug_data:
                    combined["drugs"][drug][key].extend(drug_data[key])
    
    for drug in combined["drugs"]:
        for key in ["pubmed", "clinical_trials"]:
            combined["drugs"][drug][key] = list({
                (entry["journal"], entry["date"]): entry 
                for entry in combined["drugs"][drug][key]
            }.values())
    
    return combined

def run_graph_building(p: beam.Pipeline) -> beam.PCollection:
    return (
        p
        | "List graph data files" >> beam.Create([
            os.path.join("data/graph_data", f) for f in os.listdir("data/graph_data") 
            if os.path.isfile(os.path.join("data/graph_data", f))
        ])
        | "Build Graph" >> beam.ParDo(GraphBuilder())
        | "Combine Graphs" >> beam.CombineGlobally(combine_graphs)
        | "Convert to JSON" >> beam.Map(lambda x: json.dumps(x, ensure_ascii=False, indent=2))
        | "Write Final JSON" >> beam.io.WriteToText(
            'data/output/graph.json',
            shard_name_template='',
            append_trailing_newlines=False
        )
    )