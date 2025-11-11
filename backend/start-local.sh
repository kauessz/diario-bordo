#!/bin/bash

# =============================================================================
# SCRIPT DE INICIALIZAÇÃO - DIÁRIO OPERACIONAL (BACKEND LOCAL)
# =============================================================================
# 
# Este script inicia o backend localmente
# 
# Uso:
#   chmod +x start-local.sh
#   ./start-local.sh
# 
# =============================================================================

echo "🚀 Iniciando Diário Operacional - Backend Local"
echo "================================================"
echo ""

# Verificar se o .env existe
if [ ! -f ".env" ]; then
    echo "❌ Arquivo .env não encontrado!"
    echo ""
    echo "📝 Crie o arquivo .env a partir do .env.backend.example:"
    echo "   cp .env.backend.example .env"
    echo ""
    echo "Depois edite o .env e configure:"
    echo "   - SUPABASE_DB_URL (obrigatório)"
    echo "   - GEMINI_API_KEY (opcional, para geração de emails com IA)"
    echo ""
    exit 1
fi

# Verificar se o Python está instalado
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 não está instalado!"
    echo "   Instale o Python 3.8+ e tente novamente."
    exit 1
fi

# Verificar se as dependências estão instaladas
echo "📦 Verificando dependências..."
python3 -c "import fastapi" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "⚠️  Dependências não instaladas."
    echo ""
    read -p "Deseja instalar agora? (s/N) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Ss]$ ]]; then
        echo "📥 Instalando dependências..."
        pip install -r requirements.txt --break-system-packages
        if [ $? -ne 0 ]; then
            echo "❌ Erro ao instalar dependências!"
            exit 1
        fi
        echo "✅ Dependências instaladas com sucesso!"
    else
        echo "❌ Instale as dependências manualmente:"
        echo "   pip install -r requirements.txt --break-system-packages"
        exit 1
    fi
fi

echo ""
echo "✅ Ambiente configurado!"
echo ""
echo "🌐 O backend estará disponível em: http://127.0.0.1:8000"
echo "📚 Documentação da API: http://127.0.0.1:8000/docs"
echo ""
echo "⚡ Iniciando servidor..."
echo "================================================"
echo ""

# Carregar variáveis de ambiente
export $(cat .env | xargs)

# Iniciar servidor
python3 -m uvicorn app:app --host 0.0.0.0 --port 8000 --reload
