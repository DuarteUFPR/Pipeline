# Makefile
# Execução não interativa para o terminal (make run)
# Inclui o passo 'install' para garantir a reprodutibilidade em novos ambientes.

# O caminho padrão de arquivo de entrada.
INPUT_FILE ?= dados/input.csv 
PIPELINE_SCRIPT = pipeline.py
DB_FILE = bronze_duck.db
# Lista de dependências Python necessárias para o seu pipeline
PYTHON_DEPS = pandas duckdb chardet tqdm
PYTHON_DEPS = pandas duckdb chardet tqdm matplotlib
.PHONY: run clean install

# Target 'install': Garante que as dependências estejam instaladas.
install:
	@echo "==============================================="
	@echo "    🛠️ INSTALANDO DEPENDÊNCIAS PYTHON...     "
	@echo "==============================================="
	# Instala todas as bibliotecas necessárias.
	pip install $(PYTHON_DEPS)

# make run: Executa o pipeline ETL completo, garantindo a instalação primeiro.
run: install 
	@echo "=========================================================================="
	@echo "             EXECUTANDO MAKE RUN (ETL COMPLETO E TESTES)                "
	@echo "=========================================================================="
	
	# Executa o script principal, passando o caminho do arquivo como argumento.
	python3 $(PIPELINE_SCRIPT) $(INPUT_FILE)

# make clean: Remove o banco de dados e caches gerados.
clean:
	@echo "Limpando artefatos gerados..."
	rm -f $(DB_FILE)
	rm -rf __pycache__ # O -rf remove pastas de cache de forma segura
	@echo "Limpeza concluída."
