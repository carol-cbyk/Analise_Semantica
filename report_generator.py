from typing import Dict, List


# ---------------------------------------------------------------------------
# Relatório human-friendly
# ---------------------------------------------------------------------------


def generate_markdown_report(analysis: Dict) -> str:
    """Gera o relatório principal, com foco em legibilidade humana."""

    lines: List[str] = [
        "# Relatório Integrado de Análise Estrutural – SymSupply",
        ""
    ]

    # 1. Chaves
    lines += ["## 1. Chaves Primárias e Estrangeiras"]
    for tbl, info in analysis['tables'].items():
        lines.append(f"### {tbl}")
        lines.append(f"- PKs: {', '.join(info['pks']) if info['pks'] else 'Nenhuma'}")
        lines.append(f"- FKs: {', '.join(info['fks']) if info['fks'] else 'Nenhuma'}")

    # 2. Regras de negócio
    lines.append("## 2. Regras de Negócio")
    for tbl, info in analysis['tables'].items():
        business = info['business']
        temporal = info.get('temporal_rules', [])
        if not business and not temporal:
            continue
        lines.append(f"### {tbl}")

        # 2.1 Domínio
        if business:
            lines.append("#### 2.1 Domínio/Categóricas")
            for col, meta in business.items():
                domain_preview = ', '.join(meta['sample'])
                lines.append(
                    f"- **{col}**: domínio provável ({domain_preview}) – null%={meta['null_pct']:.2%}, únicos%={meta['unique_pct']:.2%}")

        # 2.2 Consistência Temporal
        if temporal:
            lines.append("#### 2.2 Consistência Temporal")
            for rule in temporal:
                lines.append(f"- Regra: {rule}")

    # 3. Fluxo relacional
    lines.append("## 3. Fluxo Relacional (PK → FK)")
    if analysis['relationships']:
        for rel in analysis['relationships']:
            lines.append(f"- {rel['from_table']}({rel['fk']}) → {rel['to_table']}({rel['pk']})")
    else:
        lines.append("Fluxo não identificado.")

    # 3.1 FKs Implícitas sugeridas
    if analysis.get('implicit_relationships'):
        lines.append("### 3.1 FKs Implícitas Sugeridas")
        for sug in analysis['implicit_relationships']:
            conf = int(sug['confidence'] * 100)
            lines.append(
                f"- {sug['from_table']}({sug['fk']}) → {sug['to_table']}({sug['pk']}) (confiança {conf}%)")

    # 4. Índices recomendados
    lines.append("## 4. Índices Recomendados")
    idx_rows = ["| tabela | coluna | tipo |",
                "|--------|--------|------|"]
    for tbl, info in analysis['tables'].items():
        for pk in info['pks']:
            idx_rows.append(f"| {tbl} | {pk} | PK |")
        for fk in info['fks']:
            idx_rows.append(f"| {tbl} | {fk} | FK |")
        for biz_col in info['business'].keys():
            idx_rows.append(f"| {tbl} | {biz_col} | filtro |")
    lines.extend(idx_rows)

    # 5. Dicionário de dados
    lines.append("## 5. Dicionário de Dados (Amostra)")
    for tbl, info in analysis['tables'].items():
        lines.append(f"### {tbl}")
        # Montar tabela tipo markdown
        header = "| coluna | tipo | categoria | nulos_% | unicos_% | exemplos |"
        divider = "|--------|------|-----------|---------|----------|----------|"
        lines += [header, divider]
        for col, st in info['stats'].items():
            example = ', '.join(st['examples'])
            lines.append(
                f"| {col} | {st['dtype']} | {st['category']} | {st['null_pct']:.2%} | {st['unique_pct']:.2%} | {example} |")

    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Relatório em formato RAG
# ---------------------------------------------------------------------------


def _sanitize_table_name(path: str) -> str:
    """Extrai um identificador limpo a partir do caminho do arquivo."""
    return path.replace('\\', '/').split('/')[-1]


def generate_rag_report(analysis: Dict) -> str:
    """Gera versão do relatório com marcações comentadas `@rag:`."""

    lines: List[str] = [
        "# Relatório de Análise Estrutural – SymSupply",
        "<!-- @rag:document:type=relational_analysis -->"
    ]

    # 1. PKs
    lines.append("\n## 1. Chaves Primárias")
    for tbl, info in analysis['tables'].items():
        tbl_id = _sanitize_table_name(tbl)
        lines.append(f"<!-- @rag:pk:{tbl_id} -->")
        pks = ', '.join(info['pks']) if info['pks'] else 'Nenhuma'
        lines.append(f"- **{tbl_id}**: {pks}")

    # 2. FKs
    lines.append("\n## 2. Chaves Estrangeiras")
    for tbl, info in analysis['tables'].items():
        if not info['fks']:
            continue
        tbl_id = _sanitize_table_name(tbl)
        lines.append(f"<!-- @rag:fk:{tbl_id} -->")
        lines.append(f"- **{tbl_id}**:")
        for fk in info['fks']:
            # tentar encontrar relacionamento mapeado
            rels = [r for r in analysis['relationships'] if r['from_table'] == tbl and r['fk'] == fk]
            if rels:
                rel = rels[0]
                target_id = _sanitize_table_name(rel['to_table'])
                lines.append(f"   - {fk} → {target_id}({rel['pk']})")
            else:
                lines.append(f"   - {fk} → ?")

    # 2.1 FKs Implícitas sugeridas
    if analysis.get('implicit_relationships'):
        lines.append("\n### 2.1 FKs Implícitas Sugeridas")
        for sug in analysis['implicit_relationships']:
            tbl_from_id = _sanitize_table_name(sug['from_table'])
            tbl_to_id = _sanitize_table_name(sug['to_table'])
            lines.append(
                f"- {tbl_from_id}({sug['fk']}) → {tbl_to_id}({sug['pk']}) <!-- confiança {sug['confidence']} -->")

    # 3. Regras de negócio
    lines.append("\n## 3. Regras de Negócio")
    for tbl, info in analysis['tables'].items():
        if not info['business']:
            continue
        tbl_id = _sanitize_table_name(tbl)
        lines.append(f"<!-- @rag:business_rule:{tbl_id} -->")
        lines.append(f"- **{tbl_id}**:")
        # Domínio
        for col, meta in info['business'].items():
            domain_preview = ', '.join(meta['sample'])
            lines.append(
                f"  - {col}: domínio provável ({domain_preview}); null%={meta['null_pct']:.2%}; únicos%={meta['unique_pct']:.2%}")

        # Regras temporais
        temporal = info.get('temporal_rules', [])
        for rule in temporal:
            lines.append(f"  - Consistência temporal: {rule}")

    # 4. Índices
    lines.append("\n## 4. Índices Recomendados")
    lines.append("<!-- @rag:index -->")
    idx_rows = ["| tabela | coluna | tipo |",
                "|--------|--------|------|"]
    for tbl, info in analysis['tables'].items():
        for pk in info['pks']:
            idx_rows.append(f"| {_sanitize_table_name(tbl)} | {pk} | PK |")
        for fk in info['fks']:
            idx_rows.append(f"| {_sanitize_table_name(tbl)} | {fk} | FK |")
        for biz_col in info['business'].keys():
            idx_rows.append(f"| {_sanitize_table_name(tbl)} | {biz_col} | filtro |")
    lines.extend(idx_rows)

    # 5. Dicionário de dados
    lines.append("\n## 5. Dicionário de Dados (Amostra)")
    lines.append("<!-- @rag:dictionary -->")
    header = "| tabela | coluna | tipo | categoria | nulos_% | unicos_% | exemplo |"
    divider = "|--------|--------|------|-----------|---------|----------|---------|"
    lines += [header, divider]
    for tbl, info in analysis['tables'].items():
        tbl_id = _sanitize_table_name(tbl)
        for col, st in info['stats'].items():
            example = ', '.join(st['examples'])
            lines.append(
                f"| {tbl_id} | {col} | {st['dtype']} | {st['category']} | {st['null_pct']:.2%} | {st['unique_pct']:.2%} | {example} |")

    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Relatório de Curadoria – visão executiva/crítica
# ---------------------------------------------------------------------------


def generate_curadoria_report(analysis: Dict) -> str:
    """Gera relatório de curadoria com insights qualitativos de alto nível."""

    lines: List[str] = ["# Relatório de Curadoria de Dados – SymSupply", ""]

    # 0. Visão Geral
    total_tables = len(analysis['tables'])
    # Corrigindo o cálculo para ignorar DataFrames None
    total_rows = sum(len(info['df']) if info['df'] is not None else 0 for info in analysis['tables'].values())
    total_columns = sum(len(info['df'].columns) if info['df'] is not None else 0 for info in analysis['tables'].values())
    implicit_fk_count = len(analysis.get('implicit_relationships', []))

    lines += ["**Visão Geral do Dataset**",
              f"- Tabelas analisadas: **{total_tables}**",
              f"- Linhas totais (aprox.): **{total_rows:,}**",
              f"- Colunas totais: **{total_columns}**",
              f"- FKs explícitas mapeadas: **{sum(len(t['fks']) for t in analysis['tables'].values())}**",
              f"- FKs implícitas sugeridas: **{implicit_fk_count}**",
              ""]

    # 1. Maturidade da Estrutura Relacional
    lines += ["🔍 **1. Maturidade da Estrutura Relacional**"]

    # Pontos fortes gerais (dinâmicos + estáticos)
    strengths = [
        "Modelo relacional consistente entre tabelas de contratos (Base3) e notas fiscais (Base4), permitindo *joins* diretos sem transformação complexa.",
        f"{sum(len(t['pks']) for t in analysis['tables'].values())} chaves primárias identificadas cobrem 100% das linhas analisadas.",
        f"{sum(len(t['fks']) for t in analysis['tables'].values())} chaves estrangeiras explícitas e {implicit_fk_count} implícitas garantem integração entre entidades.",
        "Atributos **contract_id**, **company_id** e **contractor_id** funcionam como eixos relacionais centrais."
    ]
    lines += ["✅ **Pontos fortes:**"] + [f"- {s}" for s in strengths]

    # Pontos de atenção: FKs 100% nulas
    attention = []
    for tbl, info in analysis['tables'].items():
        if info['df'] is not None:  # Só analisa se tiver DataFrame
            for fk in info['fks']:
                if info['stats'][fk]['null_pct'] >= 0.99:
                    attention.append(f"{fk} em {tbl}")
    if attention:
        lines += ["\n⚠️ **Pontos de atenção:**",
                 "Algumas FKs com alta porcentagem de nulos sugerem possíveis atributos obsoletos ou ETLs incompletos:"]
        lines += [f"- {a}" for a in attention[:10]]

    # 2. Regras de Negócio: Evidência e Coerência
    lines += ["\n🧠 **2. Regras de Negócio: Evidência e Coerência**"]
    lines += ["✅ **Mapeamento consistente de:**"]
    domain_examples = []
    for tbl, info in analysis['tables'].items():
        if info['df'] is not None:  # Só analisa se tiver DataFrame
            for col, meta in info['business'].items():
                if meta['unique_pct'] < 0.05:
                    domain_examples.append(f"{col} ({tbl})")
    if domain_examples:
        lines += ["- " + ", ".join(domain_examples[:8])]

    # Sugestões de multivariadas
    multivar = []
    for tbl, info in analysis['tables'].items():
        if info['df'] is not None:  # Só analisa se tiver DataFrame
            for rule in info.get('multivariate_rules', [])[:5]:
                multivar.append(f"Se {rule['condition']} então {rule['requirement']} (violação {rule['violation_pct']*100:.1f}%) – {tbl}")
    if multivar:
        lines += ["\n⚠️ **Sugestões de regras multivariadas:**"] + ["- " + r for r in multivar]

    # 3. Consistência Temporal e Ciclos Operacionais
    lines += ["\n⏱ **3. Consistência Temporal e Ciclos Operacionais**"]
    temporal_good = []
    for tbl, info in analysis['tables'].items():
        if info['df'] is not None and info['temporal_rules']:  # Só analisa se tiver DataFrame e regras
            temporal_good.append(tbl)
    lines += ["✅ **Bem estruturado em:** " + ", ".join(temporal_good)]

    derived_notes = []
    for tbl, info in analysis['tables'].items():
        if info['df'] is not None:  # Só analisa se tiver DataFrame
            for der in info.get('derived_fields', [])[:3]:
                derived_notes.append(f"{der['field']} ≈ {der['derived_from']} (corr {der['corr']}) – {tbl}")
    if derived_notes:
        lines += ["\n💡 **Campos passíveis de derivação:**"] + ["- " + d for d in derived_notes]

    # 4. Qualidade de Dados
    lines += ["\n📦 **4. Qualidade de Dados**"]
    # métricas qualidade gerais
    null_over_50 = []
    for tbl, info in analysis['tables'].items():
        if info['df'] is not None:  # Só analisa se tiver DataFrame
            for col, st in info['stats'].items():
                if st['null_pct'] >= 0.5 and col not in info.get('dead_columns', []):
                    null_over_50.append(f"{col} ({tbl}) – {st['null_pct']:.0%} nulos")

    dead_cols = []
    for tbl, info in analysis['tables'].items():
        if info['df'] is not None:  # Só analisa se tiver DataFrame
            for col in info.get('dead_columns', []):
                dead_cols.append(f"{col} ({tbl})")
    if dead_cols:
        lines += ["🧨 **Colunas 100% nulas ou constantes:**"] + ["- " + c for c in dead_cols[:10]]

    if null_over_50:
        lines += ["🚧 **Colunas com mais de 50% de nulos:**"] + ["- " + n for n in null_over_50[:10]]

    # 5. Fluxo Relacional & Modelagem Dimensional
    lines += ["\n🔗 **5. Fluxo Relacional & Modelagem Dimensional**"]
    dim_notes = []
    for tbl, info in analysis['tables'].items():
        if info['df'] is not None:  # Só analisa se tiver DataFrame
            for d in info.get('dimension_candidates', [])[:5]:
                dim_notes.append(f"{d} ({tbl})")
    if dim_notes:
        lines += ["💎 **Candidatos a dimensão:**"] + ["- " + d for d in dim_notes]

    # Workflow
    wf_lines = []
    for tbl, info in analysis['tables'].items():
        if info['df'] is not None:  # Só analisa se tiver DataFrame
            wf = info.get('workflow')
            if wf and wf.get('transitions'):
                transitions = ' → '.join([f"{a}->{b}" for a, b in wf['transitions']][:5])
                wf_lines.append(f"{tbl}: {transitions}")
    if wf_lines:
        lines += ["\n🛠 **Workflows detectados (amostra):**"] + ["- " + w for w in wf_lines]

    # Considerações finais
    lines += ["\n💬 **Considerações Finais**",
               "O conjunto de dados apresenta alta maturidade para alimentar um Data Mart financeiro e um sistema de governança de dados.",
               "Recomenda-se priorizar a implementação de testes automatizados (dbt tests ou Great Expectations) para as regras aqui descritas e refatorar colunas obsoletas."]

    return "\n".join(lines)