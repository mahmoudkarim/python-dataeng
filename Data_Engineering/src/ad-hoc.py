import json
from collections import defaultdict

def load_data(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def journal_with_most_drugs(data):
    journal_drug_count = defaultdict(set)
    for drug, info in data['drugs'].items():
        for source in ['pubmed', 'clinical_trials']:
            for entry in info[source]:
                journal_drug_count[entry['journal']].add(drug)
    
    max_journal = max(journal_drug_count, key=lambda x: len(journal_drug_count[x]))
    return max_journal, len(journal_drug_count[max_journal])

def find_related_drugs(data, target_drug):
    target_journals = set()
    related_drugs = set()

    # Trouver les journaux qui mentionnent le medicament cible dans pubmed
    for entry in data['drugs'][target_drug]['pubmed']:
        target_journals.add(entry['journal'])

    # Trouver les médicaments mentionnés par les mêmes journaux dans pubmed
    for drug, info in data['drugs'].items():
        if drug != target_drug:
            for entry in info['pubmed']:
                if entry['journal'] in target_journals:
                    related_drugs.add(drug)
                    break

    # Retirer les médicaments qui sont aussi mentionnés dans les essais cliniques
    for drug in list(related_drugs):
        for entry in data['drugs'][drug]['clinical_trials']:
            if entry['journal'] in target_journals:
                related_drugs.remove(drug)
                break

    return related_drugs

def main(mode='journal_with_most_drugs', target_drug='tetracycline'):
    data = load_data('data/output/graph.json')

    if mode == 'journal_with_most_drugs':
        # Journal mentionnant le plus de médicaments différents
        top_journal, drug_count = journal_with_most_drugs(data)
        print(f"Le journal mentionnant le plus de médicaments différents est '{top_journal}' avec {drug_count} médicaments.")
    elif mode == 'related':
        # Trouver les médicaments liés pour un médicament donné
        related_drugs = find_related_drugs(data, target_drug.lower())
        print(f"\nMédicaments liés à {target_drug}:")
        for drug in related_drugs:
            print(f"- {drug}")
    else:
        print("Mode non reconnu. Utilisez 'journal_with_most_drugs' ou 'related' avec le nom du medicament recherché")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else 'journal_with_most_drugs'
    target_drug = sys.argv[2] if len(sys.argv) > 2 else 'tetracycline'
    main(mode=mode, target_drug=target_drug)
