# ==============================================================================
#  CHAT4ALL V2 - MAKEFILE
# ==============================================================================

# --- VARIÁVEIS DE DIRETÓRIOS ---
PROTO_DIR := common/proto
GEN_DIR := common/generated
SERVICES_DIR := services

# --- CONFIGURAÇÃO DO DOCKER ---
DOCKER_CMD := docker
# ifneq ($(shell id -u), 0)
# 	DOCKER_CMD := sudo docker
# endif
ENV_FILE := --env-file .env

# ==============================================================================
#  COMANDOS PRINCIPAIS
# ==============================================================================

.PHONY: help
help: ## Mostra este menu de ajuda
	@echo "----------------------------------------------------------------------"
	@echo "Chat4All v2 - Ferramentas de Desenvolvimento"
	@echo "----------------------------------------------------------------------"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo "----------------------------------------------------------------------"

# ==============================================================================
#  GRPC & PROTOBUF
# ==============================================================================

.PHONY: install
install: ## Instala dependências do projeto usando Poetry
	@echo "📦 Instalando dependências..."
	@poetry install
	@echo "✅ Instalação concluída."

.PHONY: test
test: ## Executa os testes automatizados via Docker
	@echo "🧪 Rodando testes via Docker..."
	@$(DOCKER_CMD) build -t chat4all-tests -f tests/Dockerfile.test .
	@$(DOCKER_CMD) run --rm \
		--network local_net \
		-e GATEWAY_URL=http://gateway:80 \
		-e WS_URL=ws://gateway:80/ws \
		-e S3_TEST_URL=http://minio:9000 \
		chat4all-tests -v

.PHONY: proto
proto: clean-proto ## Compila os arquivos .proto via Docker (com correção de imports)
	@echo "🚀 Gerando códigos gRPC..."
	@mkdir -p $(GEN_DIR)
	@touch $(GEN_DIR)/__init__.py
	
	@$(DOCKER_CMD) run --rm \
		-u $$(id -u):$$(id -g) \
		-v "$${PWD}:/app" \
		-w /app \
		-e HOME=/tmp \
		python:3.11-slim \
		/bin/bash -c " \
			python -m venv /tmp/venv && \
			. /tmp/venv/bin/activate && \
			pip install --upgrade --quiet pip grpcio-tools && \
			python -m grpc_tools.protoc \
			-I $(PROTO_DIR) \
			--python_out=$(GEN_DIR) \
			--grpc_python_out=$(GEN_DIR) \
			\$$(find $(PROTO_DIR) -name '*.proto') && \
			\
			echo '🔧 Corrigindo imports do Python gRPC...' && \
			sed -i 's/^import .*_pb2 as/from . \0/' /app/$(GEN_DIR)/*_pb2_grpc.py \
		"
	
	@echo "✅ Códigos gerados e corrigidos em: $(GEN_DIR)"

.PHONY: clean-proto
clean-proto: ## Remove os arquivos gRPC gerados anteriormente
	@echo "🧹 Limpando códigos gerados..."
	@rm -rf $(GEN_DIR)/*
	@echo "✨ Limpeza concluída."

# ==============================================================================
#  DOCKER & INFRA (Atalhos)
# ==============================================================================

.PHONY: up
up: ## Sobe toda a infraestrutura (infra/local/docker-compose.yml)
	@echo "🐳 Subindo containers..."
	@$(DOCKER_CMD) network prune -f
	@$(DOCKER_CMD) compose $(ENV_FILE) -f infra/local/docker-compose.yml up -d

.PHONY: down
down: ## Derruba a infraestrutura
	@echo "🛑 Parando containers..."
	@$(DOCKER_CMD) compose $(ENV_FILE) -f infra/local/docker-compose.yml down -v --remove-orphans
	@echo "🛑 Removendo networks..."
	@$(DOCKER_CMD) network prune -f

.PHONY: logs
logs: ## Mostra logs de todos os containers
	@$(DOCKER_CMD) compose $(ENV_FILE) -f infra/local/docker-compose.yml logs -f

.PHONY: config
config: ## Debug: Mostra como o docker-compose está enxergando as variáveis
	@$(DOCKER_CMD) compose $(ENV_FILE) -f infra/local/docker-compose.yml config

.PHONY: build
build: proto ## Gera os protos e depois Rebuilda as imagens Docker (Força atualização)
	@echo "🏗️  Buildando imagens..."
	@$(DOCKER_CMD) compose $(ENV_FILE) -f infra/local/docker-compose.yml build

.PHONY: ps
ps: ## Lista os containers rodando
	@$(DOCKER_CMD) compose $(ENV_FILE) -f infra/local/docker-compose.yml ps

# ==============================================================================
#  UTILITÁRIOS
# ==============================================================================

.PHONY: clean-pyc
clean-pyc: ## Remove arquivos temporários do Python (.pyc, __pycache__)
	find . -name "*.pyc" -exec rm -f {} +
	find . -name "*.pyo" -exec rm -f {} +
	find . -name "*~" -exec rm -f {} +
	find . -name "__pycache__" -exec rm -fr {} +

.PHONY: reboot
reboot: down build up