import requests
import os
import argparse

def download_file(url, filename, save_dir):
    """
    Faz o download de um arquivo de uma URL e salva localmente na pasta especificada.
    """
    filepath = os.path.join(save_dir, filename)
    print(f"Baixando {filename} para {filepath}...")
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)  # Cria o diretório se necessário
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Arquivo {filename} baixado com sucesso!")
    else:
        print(f"Erro ao baixar o arquivo {filename}: {response.status_code}")

def get_action_artifacts(repo_owner, repo_name, n, token, save_dir):
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
        runs = response.json()['workflow_runs']
        
        for run in runs:
            run_id = run['id']
            print(f"\nObtendo artefatos da execução do workflow: {run_id}")
            
            # URL para pegar os artefatos da execução do workflow
            artifacts_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/runs/{run_id}/artifacts"
            artifacts_response = requests.get(artifacts_url, headers=headers)
            
            if artifacts_response.status_code == 200:
                artifacts = artifacts_response.json()['artifacts']
                
                for artifact in artifacts:
                    artifact_name = artifact['name']
                    artifact_url = artifact['archive_download_url']
                    artifact_filename = f"{artifact_name}.zip"  # Nome do arquivo zip para o artefato
                    
                    # Faz o download do artefato
                    download_file(artifact_url, artifact_filename, save_dir)
            else:
                print(f"Erro ao obter artefatos da execução {run_id}: {artifacts_response.status_code}")
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
    
    # Diretório onde os artefatos serão salvos
    save_dir ='./output/downloaded_artifact/'

    # Faz o parsing dos argumentos passados
    args = parser.parse_args()
    
    # Chama a função para obter os artefatos das execuções dos workflows
    get_action_artifacts(args.repo_owner, args.repo_name, args.n, args.token, save_dir)


    ## todo: função que trata os parquets e gera os artefatos corretamente para o exporter.
