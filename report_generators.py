from datetime import datetime
from pathlib import Path
import json
from fpdf import FPDF, XPos, YPos

class PDFReport(FPDF):
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=15)
        self.page_width = 210  # A4 width in mm
        self.content_width = self.page_width - 20
        
    def header(self):
        self.set_font("Helvetica", 'B', 12)
        self.cell(0, 10, f"Relatório de Testes - {datetime.now().strftime('%Y%m%d_%H%M%S')}", 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
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

def generate_pdf_report(json_data, output_dir, category=None):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_file = output_dir / f"test_report_{category}_{timestamp}.pdf" if category else output_dir / f"test_report_{timestamp}.pdf"

    passed_tests = []
    failed_tests = []
    skipped_tests = []
    error_tests = []

    for test in json_data.get("tests", []):
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
        print(f"Erro ao gerar o arquivo PDF: {e}")
        return None

def create_index_html(reports_dir, categories, category_mapping):
    category_descriptions = {
        'full': 'Todos os testes',
        'versioning': 'Testes de versionamento',
        'basic': 'Testes básicos',
        'policy': 'Testes de policy',
        'cold storage': 'Testes de cold storage',
        'locking': 'Testes de locking',
        'big objects': 'Testes de big objects',
        'presign': 'Testes de presign',
        'multiple objects': 'Testes de múltiplos objetos',
        'consistency': 'Testes de consistência',
        'benchmark': 'Testes de benchmark',
        'acl': 'Testes de ACL',
    }

    def get_test_stats(json_file):
        if not json_file.exists():
            return 0, 0, 0, 0
        
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            passed = 0
            failed = 0
            skipped = 0
            error = 0
            
            for test in data.get("tests", []):
                outcome = test.get("outcome", "")
                if outcome == "passed":
                    passed += 1
                elif outcome == "failed":
                    failed += 1
                elif outcome == "skipped":
                    skipped += 1
                elif outcome == "error":
                    error += 1
            
            return passed, failed, skipped, error
        except:
            return 0, 0, 0, 0

    html_content = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Relatórios de Testes</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            color: #333;
            background-color: #f5f5f5;
        }
        h1 {
            color: #2c3e50;
            border-bottom: 2px solid #eee;
            padding-bottom: 10px;
            text-align: center;
        }
        .report-container {
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            margin-top: 20px;
            justify-content: center;
        }
        .report-card {
            background-color: #fff;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            width: 280px;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        .report-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 20px rgba(0,0,0,0.1);
        }
        .report-card h2 {
            margin-top: 0;
            color: #3498db;
            border-bottom: 1px solid #eee;
            padding-bottom: 10px;
            text-align: center;
        }
        .report-card p {
            color: #7f8c8d;
        }
        .report-link {
            display: block;
            margin-top: 15px;
            background-color: #3498db;
            color: white;
            padding: 10px 15px;
            text-decoration: none;
            border-radius: 4px;
            transition: background-color 0.3s;
            text-align: center;
            font-weight: bold;
        }
        .report-link:hover {
            background-color: #2980b9;
        }
        .updated-time {
            font-size: 0.85em;
            color: #95a5a6;
            margin-top: 15px;
            text-align: right;
        }
        .category-description {
            margin-bottom: 15px;
            text-align: center;
            height: 40px;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .stats {
            display: flex;
            justify-content: space-between;
            margin: 15px 0;
            font-size: 0.9em;
        }
        .stat-item {
            text-align: center;
            padding: 5px;
            border-radius: 4px;
            flex: 1;
            margin: 0 3px;
        }
        .passed {
            background-color: #e8f8f5;
            color: #27ae60;
        }
        .failed {
            background-color: #fdedec;
            color: #e74c3c;
        }
        .skipped {
            background-color: #eaf2f8;
            color: #3498db;
        }
        .error {
            background-color: #fce4ec;
            color: #c0392b;
        }
        footer {
            margin-top: 40px;
            text-align: center;
            color: #7f8c8d;
            font-size: 0.9em;
        }
    </style>
</head>
<body>
    <h1>Relatórios de Testes</h1>
    <p style="text-align: center;">Selecione uma categoria de testes para visualizar o relatório:</p>

    <div style="text-align: center; margin-top: 10px;">
        <a href="https://github.com/MagaluCloud/s3-specs/tree/temp-action-test/reports_pdf" target="_blank" class="report-link" style="background-color: #2ecc71; display: inline-block; width: auto; padding: 10px 20px;">
            📄 Ver últimas runs (PDF)
        </a>
    </div>

    <div class="report-container">
"""

    for category in categories:
        html_file = reports_dir / f"{category}.html"
        json_file = reports_dir / f"{category}_report.json"
        
        exists = html_file.exists()
        modified_time = datetime.fromtimestamp(html_file.stat().st_mtime).strftime("%d/%m/%Y %H:%M") if exists else "N/A"
        description = category_descriptions.get(category, 'Testes específicos')
        
        passed, failed, skipped, error = get_test_stats(json_file) if json_file.exists() else (0, 0, 0, 0)
        total = passed + failed + skipped + error
        
        html_content += f"""
        <div class="report-card">
            <h2>{category.capitalize()}</h2>
            <p class="category-description">{description}</p>
            
            <div class="stats">
                <div class="stat-item passed">{passed} ✓</div>
                <div class="stat-item failed">{failed} ✗</div>
                <div class="stat-item skipped">{skipped} ⚠</div>
                <div class="stat-item error">{error} ⚠</div>
            </div>
            
            <a href="{category}.html" class="report-link">{"Ver relatório" if exists else "Não disponível"}</a>
            <p class="updated-time">{"Atualizado em: " + modified_time if exists else "Ainda não gerado"}</p>
        </div>
"""

    html_content += """
    </div>
    
    <footer>
        <p>Gerado em: """ + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + """</p>
    </footer>
</body>
</html>
"""

    index_path = reports_dir / "index.html"
    with open(index_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Arquivo index.html criado em {index_path}")
    return index_path