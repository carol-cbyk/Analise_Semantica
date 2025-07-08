import pandas as pd
from chardet import detect

def read_csv_with_encodings(path, encodings=('utf-8','latin-1','cp1252')):
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    with open(path, 'rb') as f:
        result = detect(f.read())
    return pd.read_csv(path, encoding=result['encoding'])

def read_excel(path):
    return pd.read_excel(path, engine='openpyxl')