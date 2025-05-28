# Projeto: Pipeline de Processamento de Corridas - Databricks & PySpark

Este projeto contém um pipeline de processamento de dados de corridas, desenvolvido em Databricks utilizando PySpark e Delta Lake. O objetivo é ler, tratar, agregar e persistir informações de corridas, facilitando análises e relatórios.

---

## Índice

1. [Estrutura do Projeto](#-estrutura-do-projeto)
2. [Fluxo do Pipeline](#fluxo-do-pipeline)
3. [Como Executar](#-como-executar)
4. [Testes](#testes)
5. [Observações](#observações)
6. [Autor](#autor)

---

## 📁 Estrutura do Projeto

- **diario_de_bordo**: Pipeline ETL completo, do bronze ao silver.
- **utils**: Funções utilitárias e helpers para o pipeline.
- **tests_app**: Testes unitários para garantir a qualidade das funções.

---

## Fluxo do Pipeline

1. **Leitura**  
   Lê dados da tabela `bronze_layer.tb_info_transportes` (arquivo `info_transportes` no Databricks).

2. **Tratamento**  
   Ajusta tipos, remove espaços e cria colunas derivadas.

3. **Agregação**  
   Gera estatísticas diárias das corridas (quantidades, distâncias, propósitos).

4. **Conversão de Tipos**  
   Garante que cada coluna tenha o tipo correto para análise e persistência.

5. **Persistência**  
   Cria e salva a tabela `silver_layer.info_corridas_do_dia` em formato Delta.

---

## 🚀 Como Executar

1. Faça upload dos arquivos `diario_de_bordo` `utils` e `tests_app` e importe-os para o `Workspace` dentro dos diretórios `Users/<seu-user>/diario_de_bordo`, `/utils/utils` e `tests/tests_app`
2. Acesse o notebook `diario_de_bordo` e crie os databases `bronze_layer` e `silver_layer` e faça o upload do arquivo `info_transportes` para o Databricks, nomeie a tabela para `tb_info_transportes`.
3. Execute o notebook `diario_de_bordo` para rodar o pipeline.
4. Consulte os resultados na tabela `silver_layer.info_corridas_do_dia`.

---

## Testes

Os testes unitários estão em `tests_app` e cobrem:
- Leitura de dados
- Tratamento de campos
- Agregações
- Conversão de tipos
- Criação e persistência de tabelas

Recomenda-se rodar os testes para garantir a integridade do pipeline.

---

## Observações

- Projeto desenvolvido e testado no Databricks Community Edition.
- Utiliza PySpark, Delta Lake e SQL do Databricks.
- Estrutura modular para facilitar manutenção e evolução.

---

## Autor

Gabriel Soares Evangelista - [@gabrielsoaresevt](https://www.linkedin.com/in/gabriel-soares-evangelista)

## 📄 Licença
### The MIT License
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)