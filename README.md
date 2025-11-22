#Desafio - Pipeline de dados

Este repositório contém um pipeline completo de ETL (Extract, Transform, Load) projetado para reprodutibilidade máxima. A execução é feita através de um comando único (`make run`) em um ambiente de terminal (headless).

## ⚙️ Setup e Pré-requisitos

Para executar o pipeline, você precisa ter instalados o **Python 3** e o **`make`**. O `make run` instala automaticamente todas as dependências Python necessárias.

## 🚀 Como Rodar o Projeto

### Passo 1: Preparação dos Dados

1.  Coloque seu arquivo de dados brutos CSV dentro da pasta `dados/`.
2.  O programa possibilita a navagação direto para o arquivo CSV.

### Passo 2: Execução do ETL Completo

Execute o comando a seguir na pasta raiz do projeto:

```bash
make run
```

Este comando executa a sequência completa: Instalação → Bronze → Silver → Gold → Métricas → Testes Automatizados.

## 📊 Artefatos e Resultados

Os resultados da execução são salvos na pasta **`results/`**:
* **`metricas.json`**: Dados de tempo e contagem de linhas (Q3).
* **`throughput_tempo.png`**: Gráfico da performance por etapa.
* **`dedup_effect.png`**: Gráfico que mostra a redução de linhas (deduplicação).

## 🗑️ Limpeza do Projeto

Para remover o banco de dados DuckDB e os arquivos de cache:

```bash
make clean
```
# Pipeline
# Pipeline
# Pipeline
