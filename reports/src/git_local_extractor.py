import requests
import os
import argparse
import zipfile
from logDataclasses import TestData
from logExtractor import PytestArtifactLogExtractor
import argparse
import inspect
import csv
import itertools  


def download_file(url, filename, save_dir, token):
    """
    Faz o download de um arquivo de uma URL e salva localmente na pasta especificada.
    """
    filepath = os.path.join(save_dir, filename)
    #print(f"Baixando {filename} para {filepath}...")
    headers = {
        'Authorization': f'token {token}',
    }
    response = requests.get(url, headers=headers, stream=True)
    
    if response.status_code == 200:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)  # Cria o diretório se necessário
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Arquivo {filename} baixado com sucesso!")
    else:
        print(f"Erro ao baixar o arquivo {filename}: {response.status_code}")

def process_and_save_artifact(artifact:list[str], token:str, save_dir, processed_path: str):
    artifact_name = artifact['name']
    artifact_url = artifact['archive_download_url']
    artifact_zip = f"{artifact_name}.zip"  # Nome do arquivo zip para o artefato

    # Faz o download do artefato
    download_file(artifact_url, artifact_zip, save_dir, token)
    zip_path = os.path.join(save_dir, artifact_zip)
    with zipfile.ZipFile(zip_path, 'r') as zip:
        print(f"Unzipping {zip_path}")
        zip.extractall(save_dir)

def get_action_artifacts(repo_owner: str, repo_name: str, n: int, token: str, save_dir: str, processed_path: str, **kwargs):
    """
    Obtém os artefatos de execução dos workflows do GitHub Actions e faz o download dos arquivos.
    """
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/runs"
    headers = {
        'Authorization': f'token {token}',
    }
    
    params = {
        'per_page': n  # Número de execuções de workflows a retornar
    }
    
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        try:
            with open(processed_path, 'r') as r:
                processed = list(itertools.chain.from_iterable(list(csv.reader(r))))
        except:
            processed = []

        runs = list(map(lambda id: str(id['id']), response.json()['workflow_runs']))
        # Extraindo dados e achando os workflows que ainda nao foram processados
        processed_workflow = list(set(runs).difference(set(processed)))

        for run_id in processed_workflow:
            print(f"Obtendo artefatos da execução do workflow: {run_id}")
            
            # URL para pegar os artefatos da execução do workflow
            artifacts_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/runs/{run_id}/artifacts"
            artifacts_response = requests.get(artifacts_url, headers=headers)
            
            if artifacts_response.status_code == 200:
                artifacts = artifacts_response.json()['artifacts']

                # Recuperando todos os artifatos presentes em um workflow
                for artifact in artifacts:
                    process_and_save_artifact(artifact, token, save_dir, processed_path)
            else:
                print(f"Erro ao obter artefatos da execução {run_id}: {artifacts_response.status_code}")

            # Salvando para comparacoes posteriores

            with open(processed_path, 'a+') as c:
                writer = csv.writer(c)
                writer.writerow([run_id])
                print(f"Salvando id dos workflow: {run_id}")
    else:
        print(f"Erro ao acessar a API do GitHub: {response.status_code}")
        print(response.json())

if __name__ == "__main__":
    # Define os argumentos para a linha de comando
    parser = argparse.ArgumentParser(description="Baixar artefatos de workflows do GitHub Actions")
    
    # Adiciona os parâmetros que o usuário deve passar ao chamar o script
    parser.add_argument('repo_owner', type=str, help='Proprietário do repositório no GitHub')
    parser.add_argument('repo_name', type=str, help='Nome do repositório no GitHub')
    parser.add_argument('n', type=int, help='Número de execuções de workflows que você quer pegar')
    parser.add_argument('token', type=str, help='Seu token de autenticação do GitHub')
    args = parser.parse_args()
    
    args.save_dir = './reports/output/downloaded_artifact/'
    args.processed_path = './output/processed.csv'

    # Chama a função para obter os artefatos das execuções dos workflows
    get_action_artifacts(**vars(args))

    # Everything depends on the files present on the output
    assert os.path.exists(args.save_dir), f"{args.save_dir} does not exist"

    artifacts_paths = list(filter(lambda log: log.endswith('.log'), os.listdir(args.save_dir)))

    test_data_arguments = list(inspect.signature(TestData).parameters.keys())
    test_data = {args: [] for args in test_data_arguments}

    for path in artifacts_paths:
        logs = PytestArtifactLogExtractor(args.save_dir + path).log_to_df()
        # Adding the new tuple to the dict
        if test_data:
            list(map(lambda key, log: test_data[key].append(log), test_data_arguments, logs))

    test_data = TestData(**test_data)
