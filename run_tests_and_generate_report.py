import subprocess
import argparse
import sys
from datetime import datetime
from pathlib import Path
import json
from report_generators import generate_pdf_report, create_index_html

CATEGORY_MAPPING = {
    'full': '',
    'acl': 'acl',
    'locking': 'locking',
    'policy': 'policy',
    'cold_storage': 'cold storage',
    'basic': 'basic',
    'presign': 'presign',
    'versioning': 'bucket versioning',
    'multiple_objects': 'multiple objects',
    'big_objects': 'big objects',
    'consistency': 'consistency',
    'benchmark': 'benchmark',
}

REPORTS_DIR = Path("reportspdfs")
HTML_REPORTS_DIR = Path("reports_html")
REPORTS_DIR.mkdir(exist_ok=True)
HTML_REPORTS_DIR.mkdir(exist_ok=True)

def parse_args():
    parser = argparse.ArgumentParser(description="Executar testes e gerar relatório PDF e HTML.")
    parser.add_argument("category", choices=CATEGORY_MAPPING.keys(), help="Categoria de testes a executar")
    parser.add_argument("--mark", help="Marcação adicional do pytest", default="")
    return parser.parse_args()

def run_tests(args):
    category_name = args.category
    html_output = HTML_REPORTS_DIR / f"{category_name}.html"
    json_output = HTML_REPORTS_DIR / f"{category_name}_report.json"
    
    command = [
        "pytest",
        "--config", "./params.example.yaml",
        "./src/s3_specs/docs/",
        "--tb=line",
        "--json-report",
        f"--json-report-file={json_output}",
        f"--html={html_output}",
        "--self-contained-html",
    ]

    if args.category != 'full':
        mark_expr = CATEGORY_MAPPING[args.category]
        if args.mark:
            mark_expr += f" and {args.mark}"
        command.extend(["-m", mark_expr])
    elif args.mark:
        command.extend(["-m", args.mark])

    try:
        return subprocess.run(command).returncode
    except KeyboardInterrupt:
        print("\nTestes interrompidos. Gerando relatório parcial...")
        return 1

def generate_pdf(category=None):
    json_output = HTML_REPORTS_DIR / f"{category}_report.json" if category else Path("report.json")
    if not json_output.exists():
        print(f"Arquivo de relatório JSON não encontrado: {json_output}")
        return None

    try:
        with json_output.open() as f:
            data = json.load(f)
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON do arquivo: {json_output}")
        return None
    except Exception as e:
        print(f"Erro ao ler o arquivo JSON: {json_output}, {e}")
        return None

    return generate_pdf_report(data, REPORTS_DIR, category)

def clean_old_reports(max_files=5):
    reports = sorted(REPORTS_DIR.glob("test_report_*.pdf"))
    if len(reports) > max_files:
        for old_report in reports[:-max_files]:
            old_report.unlink()
            print(f"Removido relatório antigo: {old_report}")

if __name__ == "__main__":
    args = parse_args()
    print(f"Executando testes da categoria: {args.category}")
    if args.mark:
        print(f"Com marcação adicional: {args.mark}")

    test_result = run_tests(args)
    generate_pdf(args.category)
    clean_old_reports()
    create_index_html(HTML_REPORTS_DIR, list(CATEGORY_MAPPING.keys()), CATEGORY_MAPPING)

    sys.exit(test_result)