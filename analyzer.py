import pandas as pd

def find_primary_keys(df):
    pks = []
    for col in df.columns:
        if df[col].is_unique and not df[col].isnull().any():
            pks.append(col)
    return pks

def find_foreign_keys(df):
    return [col for col in df.columns if col.endswith('_id')]

def find_business_fields(df, threshold_unique=0.1, threshold_null=0.1):
    """Detecta colunas candidatas a regras de negócio.

    Uma *business field* aqui é entendida como uma coluna com baixa cardinalidade
    (indicando domínio fechado / enum) e baixa proporção de nulos.
    A função devolve um dicionário onde a chave é o nome da coluna e o valor
    contém as métricas de suporte, inclusive uma amostra de valores possíveis.
    """
    candidates = {}
    n = len(df)
    for col in df.columns:
        nunique_ratio = df[col].nunique() / n if n else 0
        null_ratio = df[col].isnull().sum() / n if n else 0
        if nunique_ratio < threshold_unique and null_ratio < threshold_null:
            # Amostra de até 5 valores não nulos exclusivos
            sample_values = list(df[col].dropna().astype(str).unique()[:5])
            candidates[col] = {
                'unique_pct': nunique_ratio,
                'null_pct': null_ratio,
                'sample': sample_values
            }
    return candidates


# ----------------------- Estatísticas de coluna ---------------------------

def column_stats(df):
    """Computa estatísticas básicas para cada coluna de *DataFrame*.

    Retorna um dicionário no formato {col: {...stats...}} onde:
      • dtype: tipo pandas
      • null_pct: porcentagem de nulos
      • unique_pct: cardinalidade relativa
      • examples: até 3 valores de exemplo não nulos
    """
    stats = {}
    n = len(df)
    for col in df.columns:
        series = df[col]
        stats[col] = {
            'dtype': str(series.dtype),
            'null_pct': series.isnull().mean() if n else 0,
            'unique_pct': series.nunique() / n if n else 0,
            'examples': list(series.dropna().astype(str).unique()[:3]),
            'category': categorize_column(col, series.dtype)
        }
    return stats


# ----------------------- Categorização ---------------------------

import re


def categorize_column(col_name: str, dtype) -> str:
    """Retorna categoria semântica aproximada para a coluna."""
    name = col_name.lower()
    str_dtype = str(dtype)

    if name.endswith('_id'):
        return 'relacional'
    if name.endswith('_at') or any(tok in name for tok in ['date', 'data', 'time', 'inicio', 'inicio_', 'fim', 'end', 'updated', 'created']):
        return 'temporal'
    if name in {'ativo', 'active'} or categorize_boolean(series=None, sample_name=name):
        return 'boolean'
    if any(token in name for token in ['valor', 'value', 'amount', 'preco', 'price', '_total']):
        return 'monetaria'
    if 'status' in name or 'validation' in name or 'check' in name:
        return 'status'
    if str_dtype.startswith('int') or str_dtype.startswith('float'):
        # possível numérica mas não monetária
        return 'numerica'
    return 'categorica'


def categorize_boolean(series, sample_name: str = '') -> bool:
    """Retorna True se amostra ou nome sugerirem campo booleano."""
    if sample_name:
        name = sample_name.lower()
        if name.startswith('is_') or name.endswith('_flag') or name in {'ativo', 'active', 'valid'}:
            return True
    if series is not None:
        uniq = set(series.dropna().unique())
        bool_like = {0, 1, True, False, '0', '1', 'true', 'false', 'True', 'False'}
        if uniq.issubset(bool_like):
            return True
    return False


# --------------------- Regras temporais --------------------------


EARLY_TOKENS = ['inicio', 'start', 'issue', 'created', 'emissao', 'emit', 'kick_off']
LATE_TOKENS = ['fim', 'end', 'due', 'payment', 'venc', 'updated', 'update']


def detect_temporal_rules(df):
    """Gera regras temporais simples (col_a <= col_b) quando nomes sugerem ordem."""
    date_cols = [c for c in df.columns if categorize_column(c, df[c].dtype) == 'temporal']
    rules = []
    for col_a in date_cols:
        for col_b in date_cols:
            if col_a == col_b:
                continue
            a_low = col_a.lower()
            b_low = col_b.lower()
            if any(tok in a_low for tok in EARLY_TOKENS) and any(tok in b_low for tok in LATE_TOKENS):
                rules.append(f"{col_a} ≤ {col_b}")
    return list(set(rules))

# --------------------- Qualidade de Dados ------------------------

import math
import numpy as np


def column_entropy(series):
    counts = series.value_counts(dropna=True)
    total = counts.sum()
    if total == 0:
        return 0
    probs = counts / total
    return float(-(probs * np.log2(probs)).sum())


def detect_dead_column(series) -> bool:
    return series.isnull().all() or series.nunique() <= 1


# --------------------- Inferência de FK implícitas ---------------


def infer_implicit_fks(tables_dfs: dict, existing_rels: list, min_overlap_ratio: float = 0.5):
    """Tenta sugerir FKs implícitas baseando-se em interseção de valores."""
    suggestions = []
    pk_lookup = {
        tbl: find_primary_keys(df) for tbl, df in tables_dfs.items()
    }
    for tbl_from, df_from in tables_dfs.items():
        for col in [c for c in df_from.columns if c.endswith('_id')]:
            # já mapeada?
            if any(rel['from_table'] == tbl_from and rel['fk'] == col for rel in existing_rels):
                continue
            fk_values = set(df_from[col].dropna().unique())
            if len(fk_values) == 0:
                continue
            best_match = None
            best_ratio = 0
            for tbl_to, df_to in tables_dfs.items():
                if tbl_from == tbl_to:
                    continue
                for pk in pk_lookup[tbl_to]:
                    pk_values = set(df_to[pk].dropna().unique())
                    if len(pk_values) == 0:
                        continue
                    overlap = fk_values & pk_values
                    ratio = len(overlap) / min(len(fk_values), len(pk_values))
                    if ratio > best_ratio:
                        best_ratio = ratio
                        best_match = (tbl_to, pk)
            if best_match and best_ratio >= min_overlap_ratio:
                suggestions.append({
                    'from_table': tbl_from,
                    'fk': col,
                    'to_table': best_match[0],
                    'pk': best_match[1],
                    'confidence': round(best_ratio, 2)
                })
    return suggestions


# ------------------ Regras de Negócio Multivariadas ---------------


def detect_multivariate_rules(df, max_cardinality=10, max_violation=0.05):
    """Detecta regras condicionais simples: se col_status=valor então col_target não pode ser nulo.

    Retorna lista de dicionários com: condition, requirement, violation_pct.
    """
    rules = []
    n = len(df)
    if n == 0:
        return rules

    # escolher colunas candidatas para condição (baixa cardinalidade)
    cond_cols = [c for c in df.columns if df[c].nunique() <= max_cardinality]
    target_cols = [c for c in df.columns if c not in cond_cols]

    for cond in cond_cols:
        for target in target_cols:
            subset = df[[cond, target]].dropna(subset=[cond])
            if subset.empty:
                continue
            for val in subset[cond].unique():
                cond_rows = subset[subset[cond] == val]
                if cond_rows.empty:
                    continue
                null_pct = cond_rows[target].isnull().mean()
                if null_pct <= max_violation:  # regra quase sempre verdadeira
                    rules.append({
                        'condition': f"{cond} = {val}",
                        'requirement': f"{target} != null",
                        'violation_pct': round(null_pct, 3)
                    })
    return rules


# ------------------ Campos Deriváveis -----------------------------


def detect_derived_fields(df, corr_threshold=0.95):
    """Sugere campos numéricos deriváveis da diferença entre datas."""
    suggestions = []
    date_cols = [c for c in df.columns if categorize_column(c, df[c].dtype) == 'temporal']
    numeric_cols = [c for c in df.columns if df[c].dtype.kind in {'i', 'f'}]
    if len(date_cols) < 2:
        return suggestions

    # pré-calcular dias entre pares de datas
    import pandas as pd

    for num in numeric_cols:
        if df[num].isnull().all():
            continue
        y = df[num].dropna()
        for i in range(len(date_cols)):
            for j in range(i + 1, len(date_cols)):
                a = date_cols[i]
                b = date_cols[j]
                # alinhar índices
                if not pd.api.types.is_datetime64_any_dtype(df[a]) or not pd.api.types.is_datetime64_any_dtype(df[b]):
                    continue
                diff = (df[b] - df[a]).dt.days
                aligned = pd.concat([y, diff], axis=1).dropna()
                if len(aligned) < 10:
                    continue
                corr = aligned.corr().iloc[0, 1]
                if pd.notnull(corr) and abs(corr) >= corr_threshold:
                    suggestions.append({
                        'field': num,
                        'derived_from': f"{b} - {a}",
                        'corr': round(float(corr), 2)
                    })
    return suggestions


# ------------------ Workflow Detection ---------------------------


def detect_workflow(df, status_col_candidates=None, timestamp_col='updated_at'):
    """Detecta possíveis estados e transições com base em uma coluna de status.

    Retorna dict com 'states' e 'transitions'.
    """
    if status_col_candidates is None:
        status_col_candidates = [c for c in df.columns if 'status' in c.lower() or 'state' in c.lower()]
    for status_col in status_col_candidates:
        if status_col in df.columns and df[status_col].nunique() > 1:
            break
    else:
        return {}

    if timestamp_col not in df.columns or df[timestamp_col].isnull().all():
        timestamp_col = None

    states = sorted(df[status_col].dropna().unique())

    transitions = set()
    if timestamp_col:
        df_sorted = df.dropna(subset=[status_col, timestamp_col]).sort_values(timestamp_col)
        grouped = df_sorted.groupby('invoice_id' if 'invoice_id' in df.columns else df.index)
        for _, grp in grouped:
            prev = None
            for st in grp[status_col]:
                if prev is not None and prev != st:
                    transitions.add((prev, st))
                prev = st

    return {
        'status_col': status_col,
        'states': list(states),
        'transitions': list(transitions)
    }


# ------------------ Dimension Candidates -------------------------


def detect_dimension_candidates(df, max_cardinality_pct=0.02):
    n = len(df)
    candidates = []
    if n == 0:
        return candidates
    for col in df.columns:
        card = df[col].nunique() / n if n else 1
        if 0 < card <= max_cardinality_pct and not col.endswith('_id'):
            candidates.append(col)
    return candidates