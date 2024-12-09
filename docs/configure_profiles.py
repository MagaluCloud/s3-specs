import os
import yaml
import subprocess
import sys

def configure_aws_profiles_from_env(profiles):
    try:
        for profile_name, profile_data in profiles.items():
            # Obter informações do perfil
            endpoint = profile_data.get("endpoint")
            access_key = profile_data.get("access_key")
            secret_key = profile_data.get("secret_key")
            region = profile_data.get("region")

            if not (endpoint and access_key and secret_key and region):
                print(f"Perfil {profile_name} está incompleto. Ignorando...")
                continue

            # Configurar o perfil usando AWS CLI
            subprocess.run(
                ["aws", "configure", "set", f"profile.{profile_name}.region", region],
                check=True,
            )
            subprocess.run(
                ["aws", "configure", "set", f"profile.{profile_name}.aws_access_key_id", access_key],
                check=True,
            )
            subprocess.run(
                ["aws", "configure", "set", f"profile.{profile_name}.aws_secret_access_key", secret_key],
                check=True,
            )
            subprocess.run(
                ["aws", "configure", "set", f"profile.{profile_name}.endpoint_url", endpoint],
                check=True,
            )

    except yaml.YAMLError as e:
        print(f"Erro ao processar os dados YAML: {e}")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar comando AWS CLI: {e}")

# Exemplo de uso
if __name__ == "__main__":
    # Ler o conteúdo da variável de ambiente
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as file:
            profiles = yaml.safe_load(file)
    else:
        profiles_data = os.getenv("PROFILES")
        if not profiles_data:
            print(f"Variável de ambiente '{"PROFILES"}' não encontrada ou vazia.")

            # Carregar o conteúdo como YAML
        profiles = yaml.safe_load(profiles_data)

    configure_aws_profiles_from_env(profiles)
