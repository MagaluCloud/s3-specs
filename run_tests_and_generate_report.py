import subprocess
import argparse
import sys
from datetime import datetime
from pathlib import Path
import json
from fpdf import FPDF, XPos, YPos

CATEGORY_MAPPING = {
    'full': '',
    'versioning': 'versioning',
    'basic': 'basic',
    'policy': 'policy',
    'cold': 'cold',
    'locking': 'locking',
    'big-objects': 'big_objects'
}

REPORTS_DIR = Path("reportspdfs")
REPORTS_DIR.mkdir(exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")


def parse_args():
    parser = argparse.ArgumentParser(description="Executar testes e gerar relatório PDF.")
    parser.add_argument("category", choices=CATEGORY_MAPPING.keys(), help="Categoria de testes a executar")
    parser.add_argument("--mark", help="Marcação adicional do pytest", default="")
    return parser.parse_args()


def run_tests(args):
    command = [
        "pytest",
        "--config", "./params.example.yaml",
        "./src/s3_specs/docs/",
        "--tb=line",
        "--json-report",
        "--json-report-file=report.json",
        "--html=report.html",
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


class PDFReport(FPDF):
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=15)
        self.page_width = 210  # A4 width in mm
        self.content_width = self.page_width - 20
        
    def header(self):
        self.set_font("Helvetica", 'B', 12)
        self.cell(0, 10, f"Relatório de Testes - {timestamp}", 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
        self.ln(5)
        
    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", 'I', 8)
        self.cell(0, 10, f'Página {self.page_no()}', 0, new_x=XPos.RIGHT, new_y=YPos.TOP, align='C')
    
    def section_header(self, title, color=(230, 230, 230)):
        self.set_font("Helvetica", 'B', 12)
        self.set_fill_color(*color)
        self.cell(0, 10, title, 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='L', fill=True)
        self.ln(2)
    
    def add_test_entry(self, name, duration, message=None, status="passed"):
        status_colors = {
            "passed": (220, 255, 220),  
            "failed": (255, 220, 220),  
            "skipped": (240, 240, 255),  
            "error": (255, 200, 200)  
        }
        
        self.set_fill_color(*status_colors.get(status, (255, 255, 255)))
        self.set_font("Helvetica", 'B', 10)
        
        test_name = name.split("::")[0].split("/")[-1].replace(".py", "") if "::" in name else name
        test_case = name.split("::")[-1] if "::" in name else ""
        self.cell(self.content_width - 20, 8, test_name, 1, new_x=XPos.RIGHT, new_y=YPos.TOP, align='L', fill=True)
        self.cell(20, 8, f"{duration:.2f}s", 1, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='R', fill=True)
        
        if test_case:
            self.set_font("Helvetica", '', 9)
            self.cell(self.content_width, 7, f"Caso: {test_case}", 1, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='L', fill=True)
        
        if message:
            self.set_font("Helvetica", '', 9)
            max_chars_per_line = 100
            formatted_message = ""
            lines = message.split('\n')
            for line in lines:
                if len(line) > max_chars_per_line:
                    for i in range(0, len(line), max_chars_per_line):
                        formatted_message += line[i:i+max_chars_per_line] + "\n"
                else:
                    formatted_message += line + "\n"
            
            self.multi_cell(self.content_width, 5, formatted_message, 1, 'L')
        
        self.ln(2)

    def add_summary(self, passed, failed, skipped, total):
        self.ln(5)
        self.set_font("Helvetica", 'B', 11)
        
        self.set_fill_color(240, 240, 240)
        self.cell(self.content_width/4, 8, f"Passaram: {passed}", 1, new_x=XPos.RIGHT, new_y=YPos.TOP, align='C', fill=True)
        
        fill_color = (255, 200, 200) if failed > 0 else (240, 240, 240)
        self.set_fill_color(*fill_color)
        self.cell(self.content_width/4, 8, f"Falharam: {failed}", 1, new_x=XPos.RIGHT, new_y=YPos.TOP, align='C', fill=True)
        
        self.set_fill_color(240, 240, 240)
        self.cell(self.content_width/4, 8, f"Pulados: {skipped}", 1, new_x=XPos.RIGHT, new_y=YPos.TOP, align='C', fill=True)
        self.cell(self.content_width/4, 8, f"Total: {total}", 1, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C', fill=True)


def generate_pdf():
    report_path = Path("report.json")
    if not report_path.exists():
        print(f"Arquivo de relatório JSON não encontrado: {report_path}")
        return None

    try:
        with report_path.open() as f:
            data = json.load(f)
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON do arquivo: {report_path}")
        return None
    except Exception as e:
        print(f"Erro ao ler o arquivo JSON: {report_path}, {e}")
        return None

    pdf_file = REPORTS_DIR / f"test_report_{timestamp}.pdf"

    passed_tests = []
    failed_tests = []
    skipped_tests = []
    error_tests = []

    for test in data.get("tests", []):
        name = test.get("nodeid", "N/A")
        outcome = test.get("outcome", "N/A")
        duration = test.get("call", {}).get("duration", 0.0) or test.get("setup", {}).get("duration", 0.0)
        
        if outcome == "passed":
            passed_tests.append((name, duration))
        elif outcome == "failed":
            error = test.get("call", {}).get("longrepr", "").strip()
            failed_tests.append((name, duration, error))
        elif outcome == "skipped":
            skip_reason = test.get("setup", {}).get("longrepr", "").strip()
            if "Skipped: " in skip_reason:
                skip_reason = skip_reason.split("Skipped: ", 1)[1].strip()
            skipped_tests.append((name, duration, skip_reason))
        else: 
            for phase in ["setup", "call", "teardown"]:
                if phase in test and "longrepr" in test[phase]:
                    error = test[phase]["longrepr"].strip()
                    error_tests.append((name, duration, error))
                    break
            else:
                error_tests.append((name, duration, "Erro não especificado"))

    pdf = PDFReport()
    pdf.add_page()
    
    total = len(passed_tests) + len(failed_tests) + len(skipped_tests) + len(error_tests)
    pdf.add_summary(len(passed_tests), len(failed_tests) + len(error_tests), len(skipped_tests), total)
    pdf.ln(10)
    
    if error_tests:
        pdf.section_header("Erros em Testes", color=(255, 180, 180))
        for name, duration, error in error_tests:
            pdf.add_test_entry(name, duration, error, "error")
    
    if failed_tests:
        pdf.section_header("Testes com Falha", color=(255, 200, 200))
        for name, duration, error in failed_tests:
            pdf.add_test_entry(name, duration, error, "failed")
    
    if skipped_tests:
        pdf.section_header("Testes Pulados", color=(220, 220, 255))
        for name, duration, reason in skipped_tests:
            pdf.add_test_entry(name, duration, reason, "skipped")
    
    if passed_tests:
        pdf.section_header("Testes que Passaram", color=(220, 255, 220))
        for name, duration in passed_tests:
            pdf.add_test_entry(name, duration, status="passed")

    try:
        pdf.output(str(pdf_file))
        print(f"Relatório gerado: {pdf_file}")
        return pdf_file
    except Exception as e:
        return None


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
    generate_pdf()
    clean_old_reports()
    sys.exit(test_result)