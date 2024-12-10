import os
import yaml
import subprocess
import sys

def set_aws_profiles(profile_name, data):
    subprocess.run(
        ["aws", "configure", "set", f"profile.{profile_name}.region", data.get("region")],
        check=True,
    )
    subprocess.run(
        ["aws", "configure", "set", f"profile.{profile_name}.aws_access_key_id", data.get("access_key")],
        check=True,
    )
    subprocess.run(
        ["aws", "configure", "set", f"profile.{profile_name}.aws_secret_access_key", data.get("secret_key")],
        check=True,
    )
    subprocess.run(
        ["aws", "configure", "set", f"profile.{profile_name}.endpoint_url", data.get("endpoint")],
        check=True,
    )

def set_rclone_profiles(profile_name, data):
    """
    Configura o rclone com base nos dados fornecidos.
    
    :param profile_name: Nome do perfil a ser configurado.
    :param data: Dicionário contendo as informações de configuração.
    """
    subprocess.run(
        ["rclone", "config", "create", profile_name, data.get("type", "s3")],
        check=True,
    )
    subprocess.run(
        ["rclone", "config", "update", profile_name, "region", data.get("region")],
        check=True,
    )
    subprocess.run(
        ["rclone", "config", "update", profile_name, "access_key_id", data.get("access_key")],
        check=True,
    )
    subprocess.run(
        ["rclone", "config", "update", profile_name, "secret_access_key", data.get("secret_key")],
        check=True,
    )
    if "endpoint" in data:
        subprocess.run(
            ["rclone", "config", "update", profile_name, "endpoint", data.get("endpoint")],
            check=True,
        )

def configure_profiles(profiles):
    try:
        for profile_name, profile_data in profiles.items():
            endpoint = profile_data.get("endpoint")
            access_key = profile_data.get("access_key")
            secret_key = profile_data.get("secret_key")
            region = profile_data.get("region")

            if not (endpoint and access_key and secret_key and region):
                print(f"Perfil {profile_name} está incompleto. Ignorando...")
                continue

            set_aws_profiles(profile_name=profile_name, data=profile_data)
            set_rclone_profiles(profile_name=profile_name, data=profile_data)
            print(f"Perfil {profile_name} configurado com sucesso.")

    except yaml.YAMLError as e:
        print(f"Erro ao processar os dados YAML: {e}")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar comando AWS CLI: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as file:
            profiles = yaml.safe_load(file)
    else:
        profiles_data = os.getenv("PROFILES")
        if not profiles_data:
            print(f"Variável de ambiente '{"PROFILES"}' não encontrada ou vazia.")

        profiles = yaml.safe_load(profiles_data)

    configure_profiles(profiles)
