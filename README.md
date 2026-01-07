# BrazilGrid

Análise de dados do setor elétrico brasileiro.

## Estrutura do Projeto

```
brazilgrid/
├── products/           # Produtos e análises
│   └── historico/      # Análises históricas
│       ├── pipelines/  # Pipelines de processamento de dados
│       │   └── curtailment/
│       └── dashboard/  # Dashboards e visualizações
├── shared/             # Código compartilhado
│   └── handlers/       # Handlers reutilizáveis
├── infra/              # Infraestrutura e configurações
├── pyproject.toml      # Configuração do projeto
└── README.md           # Este arquivo
```

## Desenvolvimento

Este projeto usa [uv](https://github.com/astral-sh/uv) para gerenciamento de dependências.

### Instalação

```bash
# Instalar dependências
uv sync
```

## Licença

TBD
