# Pipeline de Análise Estrutural de Dados

Este projeto é uma ferramenta automatizada para análise estrutural de bases de dados, gerando relatórios detalhados sobre a estrutura, relacionamentos e características dos dados.

## Funcionalidades Principais

- **Análise Automática de Arquivos**:
  - Suporte para arquivos CSV, Excel (xlsx, xlsm, xls) e SQL
  - Descoberta automática de arquivos na pasta `/data`

- **Análise Estrutural**:
  - Identificação de chaves primárias (PKs)
  - Detecção de chaves estrangeiras (FKs)
  - Descoberta de campos de negócio
  - Análise estatística de colunas
  - Detecção de regras temporais
  - Inferência de FKs implícitas
  - Identificação de regras multivariadas
  - Detecção de campos derivados
  - Identificação de candidatos a dimensões
  - Análise de fluxos de trabalho
  - Detecção de colunas sem uso (dead columns)

- **Geração de Relatórios**:
  - Relatório em formato Markdown
  - Relatório adaptado para RAG (Retrieval-Augmented Generation)
  - Relatório de curadoria
  - Versão refinada usando GPT (requer API key da OpenAI)

## Estrutura do Projeto

- `main.py`: Arquivo principal que coordena o pipeline de análise
- `reader.py`: Módulo para leitura de diferentes formatos de arquivo
- `analyzer.py`: Contém as funções de análise estrutural
- `report_generator.py`: Geração dos diferentes tipos de relatório
- `data/`: Diretório onde devem ser colocados os arquivos a serem analisados
- `requirements.txt`: Dependências do projeto

## Como Usar

1. Coloque seus arquivos de dados (CSV, Excel ou SQL) na pasta `data/`
2. Instale as dependências do projeto:
   ```
   pip install -r requirements.txt
   ```
3. Execute o script principal:
   ```
   python main.py
   ```
4. Os relatórios serão gerados na pasta `reports/`

## Saídas Geradas

O pipeline gera quatro tipos de relatórios:
- `relatorio_sym_supply.md`: Relatório base em Markdown
- `relatorio_sym_supply_rag.md`: Versão adaptada para RAG
- `relatorio_curadoria.md`: Relatório de curadoria
- `relatorio_sym_supply_gpt.md`: Versão refinada pelo GPT (se API key disponível)

## Requisitos

- Python 3.x
- Dependências listadas em `requirements.txt`
- Chave da API OpenAI (opcional, para refinamento com GPT) 