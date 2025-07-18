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
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True,
    )
    subprocess.run(
        ["rclone", "config", "update", profile_name, "region", data.get("region")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True,
    )
    subprocess.run(
        ["rclone", "config", "update", profile_name, "access_key_id", data.get("access_key")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True,
    )
    subprocess.run(
        ["rclone", "config", "update", profile_name, "secret_access_key", data.get("secret_key")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True,
    )
    if "endpoint" in data:
        subprocess.run(
            ["rclone", "config", "update", profile_name, "endpoint", data.get("endpoint")],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True,
        )

def set_mgc_profiles(profile_name, data):
    profile_dir = os.path.expanduser(f"~/.config/mgc/{profile_name}")
    os.makedirs(profile_dir, exist_ok=True)
    
    # Create auth.yaml
    auth_file_path = os.path.join(profile_dir, "auth.yaml")
    with open(auth_file_path, "w") as auth_file:
        auth_file.write(
            f"access_key_id: {data.get('access_key')}\n"
            f"secret_access_key: {data.get('secret_key')}\n"
        )
    
    # Create cli.yaml
    cli_file_path = os.path.join(profile_dir, "cli.yaml")
    with open(cli_file_path, "w") as cli_file:
        cli_file.write(f"region: {data.get('region')}\n")
        cli_file.write(f"serverurl:: {data.get('endpoint')}\n")

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
            
            if (access_key == "YOUR-KEY-ID-HERE" or secret_key == "YOUR-SECRET-KEY-HERE" ):
                print(f"Perfil {profile_name} está incompleto. Ignorando...")
                continue

            set_aws_profiles(profile_name=profile_name, data=profile_data)
            set_rclone_profiles(profile_name=profile_name, data=profile_data)
            set_mgc_profiles(profile_name=profile_name, data=profile_data)
            print(f"Configuration of {profile_name} done!")
    except yaml.YAMLError as e:
        print(f"Erro ao processar os dados YAML: {e}")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar comando AWS CLI: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as file:
            profiles_list = yaml.safe_load(file)
            profiles = {}
            for profile in list(profiles_list.items())[2][1]:
                if not isinstance(profile, dict):
                    profiles = profiles_list
                    break
                profiles[profile.get("profile_name")] = {}
                profiles[profile.get("profile_name")]["endpoint"] = profile.get("endpoint_url")
                profiles[profile.get("profile_name")]["access_key"] = profile.get("aws_access_key_id")
                profiles[profile.get("profile_name")]["secret_key"] = profile.get("aws_secret_access_key")
                profiles[profile.get("profile_name")]["region"] = profile.get("region_name")
                
    else:
        profiles_data = os.getenv("PROFILES")
        if not profiles_data:
            print(f"Variável de ambiente '{"PROFILES"}' não encontrada ou vazia.")

        profiles = yaml.safe_load(profiles_data)

    print(f"Number of profiles - {len(profiles)}")
    configure_profiles(profiles)
    print("Profile Configurations Done!")
