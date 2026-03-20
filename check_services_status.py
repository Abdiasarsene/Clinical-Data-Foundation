import subprocess
import json
import yaml

# Chemin vers ton fichier docker-compose
DOCKER_COMPOSE_FILE = "./docker-compose.stack.yaml"

def get_services_from_compose(file_path):
    with open(file_path, "r") as f:
        compose = yaml.safe_load(f)
    return list(compose.get("services", {}).keys())

def check_service_status(service):
    try:
        # docker inspect pour vérifier si le container est en cours
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={service}", "--format", "{{.Status}}"],
            capture_output=True, text=True
        )
        status = result.stdout.strip()
        if status.startswith("Up"):
            return "up"
        else:
            return "down"
    except Exception:
        return "error"

def main():
    services = get_services_from_compose(DOCKER_COMPOSE_FILE)
    status_dict = {}
    for service in services:
        status_dict[service] = check_service_status(service)
    
    print(json.dumps(status_dict, indent=2))

if __name__ == "__main__":
    main()