import os, re
from reader import read_csv_with_encodings, read_excel
from analyzer import (
    find_primary_keys,
    find_foreign_keys,
    find_business_fields,
    column_stats,
    detect_temporal_rules,
    infer_implicit_fks,
    detect_multivariate_rules,
    detect_derived_fields,
    detect_dimension_candidates,
    detect_workflow,
    detect_dead_column
)
from report_generator import (
    generate_markdown_report,
    generate_rag_report,
    generate_curadoria_report,
)
import openai
from openai import OpenAI
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env se existir
load_dotenv()

# ---------------------------------------------------------------------------
# Descoberta automática de arquivos na pasta /data
# ---------------------------------------------------------------------------

def discover_input_files(data_dir: str = "data") -> dict:
    """Varre recursivamente `data_dir` e devolve dict path -> tipo (csv/excel).

    Arquivos suportados:
        • .csv
        • .xlsx / .xlsm / .xls
    Demais extensões são ignoradas pelo pipeline atual.
    """
    import glob
    input_files = {}
    patterns = ["**/*.csv", "**/*.xlsx", "**/*.xlsm", "**/*.xls", "**/*.sql"]
    for pattern in patterns:
        for path in glob.glob(os.path.join(data_dir, pattern), recursive=True):
            if os.path.basename(path).startswith(".~lock"):
                continue  # ignora artefatos de lock do LibreOffice
            ext = os.path.splitext(path)[1].lower()
            if ext == ".csv":
                input_files[path.replace("\\", "/")] = "csv"
            elif ext in {".xlsx", ".xlsm", ".xls"}:
                input_files[path.replace("\\", "/")] = "excel"
            elif ext == ".sql":
                input_files[path.replace("\\", "/")] = "sql"
    return input_files


INPUT_FILES = discover_input_files()
if not INPUT_FILES:
    print("Aviso: Nenhum arquivo CSV/XLSX encontrado na pasta 'data'.")

def run_analysis():
    # Estrutura de análise será organizada por tabela
    analysis = {
        'tables': {},  # fname -> {...informações...}
        'relationships': []  # lista de dicionários: {from_table, fk, to_table, pk}
    }
    for fname, ftype in INPUT_FILES.items():
        try:
            if ftype == 'csv':
                df = read_csv_with_encodings(fname)
            elif ftype == 'excel':
                df = read_excel(fname)
            else:
                df = None
        except Exception as e:
            continue
        if ftype in {'csv', 'excel'} and df is not None:
            pks = find_primary_keys(df)
            fks = find_foreign_keys(df)
        elif ftype == 'sql':
            # parse SQL file for PK/FK
            with open(fname, 'r', encoding='utf-8', errors='ignore') as sqlf:
                sql_text = sqlf.read()
            pks = re.findall(r"PRIMARY KEY \(([^)]+)\)", sql_text, flags=re.IGNORECASE)
            pks = [pk.strip().strip('`"') for group in pks for pk in group.split(',')]

            fk_matches = re.findall(r"FOREIGN KEY \(([^)]+)\).*?REFERENCES\s+\w+\s*\(([^)]+)\)", sql_text, flags=re.IGNORECASE|re.DOTALL)
            fks = []
            for fk_cols, ref_cols in fk_matches:
                for col in fk_cols.split(','):
                    fks.append(col.strip().strip('`"'))
        else:
            pks, fks = [], []
        if df is not None:
            business = find_business_fields(df)
            stats = column_stats(df)
            temporal_rules = detect_temporal_rules(df)
            multirules = detect_multivariate_rules(df)
            derived = detect_derived_fields(df)
            dead_cols = [c for c in df.columns if detect_dead_column(df[c])]
            dimensions = detect_dimension_candidates(df)
            workflow = detect_workflow(df)
        else:
            business = {}
            stats = {}
            temporal_rules = []
            multirules = []
            derived = []
            dead_cols = []
            dimensions = []
            workflow = {}

        analysis['tables'][fname] = {
            'pks': pks,
            'fks': fks,
            'business': business,
            'stats': stats,
            'temporal_rules': temporal_rules,
            'df': df if df is not None else None  # guardar para análises cruzadas
            , 'multivariate_rules': multirules
            , 'derived_fields': derived
            , 'dead_columns': dead_cols
            , 'dimension_candidates': dimensions
            , 'workflow': workflow
        }

    # -------------------- Descobrir relacionamentos PK/FK -------------------
    # Heurística simples: FK com mesmo nome de PK em outra tabela
    for tbl_from, tbl_info in analysis['tables'].items():
        for fk in tbl_info['fks']:
            # procurar tabela onde esse campo é PK
            for tbl_to, info_to in analysis['tables'].items():
                if fk in info_to['pks'] and tbl_from != tbl_to:
                    analysis['relationships'].append({
                        'from_table': tbl_from,
                        'fk': fk,
                        'to_table': tbl_to,
                        'pk': fk
                    })

    # -------------------- Inferir FKs implícitas ----------------------------
    # Considerar apenas tabelas que possuem DataFrame carregado
    tables_dfs = {tbl: info['df'] for tbl, info in analysis['tables'].items() if info['df'] is not None}
    implicit = infer_implicit_fks(tables_dfs, analysis['relationships'])
    analysis['implicit_relationships'] = implicit

    return analysis

def refine_with_gpt(markdown):
    api_key = os.getenv("OPENAI_API_KEY")
    
    # Se a chave não estiver definida, solicitar ao usuário
    if not api_key:
        api_key = input("Por favor, insira sua chave da API OpenAI: ")
        if not api_key:
            print("Chave da API não fornecida. Pulando refinamento com GPT.")
            return markdown
    
    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "Você é um assistente que refina relatórios de análise estrutural."},
            {"role": "user", "content": markdown}
        ],
        temperature=0.2
    )
    return response.choices[0].message.content

if __name__ == '__main__':
    analysis = run_analysis()

    # ------------- Diretório de saída ---------------
    REPORT_DIR = 'reports'
    os.makedirs(REPORT_DIR, exist_ok=True)

    report_md = generate_markdown_report(analysis)
    with open(os.path.join(REPORT_DIR, 'relatorio_sym_supply.md'), 'w', encoding='utf-8') as f:
        f.write(report_md)

    # Versão adaptada para RAG
    rag_md = generate_rag_report(analysis)
    with open(os.path.join(REPORT_DIR, 'relatorio_sym_supply_rag.md'), 'w', encoding='utf-8') as f:
        f.write(rag_md)

    # Curadoria
    cur_md = generate_curadoria_report(analysis)
    with open(os.path.join(REPORT_DIR, 'relatorio_curadoria.md'), 'w', encoding='utf-8') as f:
        f.write(cur_md)
    
    try:
        refined = refine_with_gpt(report_md)
        with open(os.path.join(REPORT_DIR, 'relatorio_sym_supply_gpt.md'), 'w', encoding='utf-8') as f:
            f.write(refined)
        print("Pipeline executado com sucesso. Relatórios gerados.")
    except Exception as e:
        print(f"Erro ao refinar com GPT: {e}")
        print("Relatório básico gerado em 'relatorio_sym_supply.md'.")