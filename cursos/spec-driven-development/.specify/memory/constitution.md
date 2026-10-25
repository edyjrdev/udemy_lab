<!--
SYNC IMPACT REPORT
Mudança de versão: 1.0.0 → 1.0.1
Princípios modificados: nenhum (apenas esclarecimento de placeholder)
Princípios adicionados: nenhum
Seções adicionadas: nenhuma
Seções removidas: nenhuma
Alterações: PROJECT_NAME preenchido com "SDD na Engenharia de Dados", removendo o
  TODO pendente da ratificação inicial.
Templates dependentes: não modificados neste comando (fora de escopo).
Itens pendentes (TODO): nenhum
-->

# SDD na Engenharia de Dados Constitution

## Core Principles

### I. Clean Code e SOLID Aplicados à Engenharia de Dados (NON-NEGOTIABLE)
Todo código de ingestão, transformação e disponibilização de dados DEVE seguir os
princípios de Clean Code (nomes significativos, funções pequenas com responsabilidade
única, eliminação de duplicação, ausência de efeitos colaterais ocultos) e os
princípios SOLID adaptados à engenharia de dados: cada job/transformação DEVE ter uma
única responsabilidade (SRP); pipelines DEVEM ser abertos para extensão (novas fontes
ou destinos) e fechados para modificação de contratos já publicados (OCP); dependências
entre camadas DEVEM ocorrer por meio de contratos de schema estáveis, não por acoplamento
direto a implementações (DIP). Código que viole esses princípios DEVE ser rejeitado em
revisão.
**Racional**: pipelines de dados crescem rapidamente em complexidade; disciplina de
Clean Code e SOLID é o que garante manutenibilidade, testabilidade e evolução segura do
Data Warehouse ao longo do tempo.

### II. Arquitetura em Camadas Medallion (Bronze, Silver, Gold)
O projeto DEVE organizar todo o processamento de dados em três camadas distintas e
desacopladas, cada uma com responsabilidade exclusiva:
- **Bronze**: DEVE concentrar todos os recursos de ingestão das fontes de dados,
  preservando os dados brutos com fidelidade ao formato de origem, sem aplicar regras
  de negócio ou transformações.
- **Silver**: DEVE concentrar todas as transformações, limpeza, padronização e
  enriquecimento aplicadas sobre os dados oriundos da camada Bronze.
- **Gold**: DEVE disponibilizar os dados já modelados para o usuário final, seja por
  meio de API, seja por meio de MCP (Model Context Protocol), viabilizando a
  configuração de painéis (dashboards/BI) e a integração com outros sistemas.
Nenhuma camada pode pular etapas (por exemplo, a camada Gold não pode ler diretamente
da camada Bronze) e cada camada DEVE poder ser testada e evoluída de forma independente.
**Racional**: a separação em camadas isola responsabilidades, reduz o raio de impacto
de mudanças e permite escalar ingestão, transformação e consumo de forma independente.

### III. Modelagem Dimensional para o Data Warehouse
A camada Gold DEVE ser fundamentada em modelagem de dados analítica (esquemas do tipo
estrela e/ou floco de neve, com fatos e dimensões claramente definidos), garantindo que
o Data Warehouse resultante seja otimizado para consultas analíticas, ferramentas de BI
e integrações externas. Toda alteração estrutural nos modelos dimensionais (novas
dimensões, novos fatos, mudança de grão) DEVE ser versionada e documentada antes de ser
promovida para consumo.
**Racional**: modelagem dimensional consistente é o que torna os dados da camada Gold
compreensíveis, performáticos e reutilizáveis por múltiplos consumidores (API, MCP,
painéis).

### IV. Idempotência e Consistência de Dados (NON-NEGOTIABLE)
Toda operação de ingestão, transformação ou carga, em qualquer camada (Bronze, Silver
ou Gold), DEVE ser idempotente: reexecuções (reprocessamentos, reruns, backfills) com os
mesmos parâmetros de entrada NÃO PODEM gerar duplicidade, corrupção ou inconsistência de
dados no Data Warehouse. Estratégias como upsert/merge por chave de negócio,
particionamento por janela temporal e checkpoints de processamento DEVEM ser adotadas
para garantir esse comportamento.
**Racional**: pipelines de dados falham e precisam ser reexecutados; sem idempotência,
cada reprocessamento se torna um risco de corrupção do Data Warehouse.

### V. Documentação em Português do Brasil (pt-BR)
Toda a documentação do projeto (especificações, planos, tarefas, documentação de
arquitetura, comentários de decisão e relatórios) DEVE ser escrita em Português do
Brasil. Termos técnicos consolidados internacionalmente (por exemplo, os nomes das
camadas Bronze/Silver/Gold, "pipeline", "upsert", "schema") podem ser mantidos em
inglês, mas a redação corrida e as explicações DEVEM estar em pt-BR.
**Racional**: garantir acessibilidade e compreensão uniforme da documentação por toda a
equipe do projeto.

## Padrões de Qualidade e Confiabilidade de Dados

Toda transformação realizada na camada Silver DEVE incluir validações de qualidade de
dados (nulidade, tipos, duplicidade e integridade referencial) antes de qualquer dado
ser promovido para a camada Gold. Falhas de ingestão ou transformação DEVEM ser
registradas por meio de logging estruturado e NÃO PODEM corromper silenciosamente o
estado do Data Warehouse. Os contratos de schema entre camadas (Bronze → Silver → Gold)
DEVEM ser explícitos e versionados, permitindo evolução compatível sem quebrar
consumidores já existentes na camada Gold (API, MCP ou painéis).

## Fluxo de Desenvolvimento e Revisão

Toda mudança em pipelines ou modelos de dados DEVE ser revisada quanto à aderência às
camadas Medallion, aos princípios de Clean Code/SOLID e à idempotência antes do merge.
Novas fontes de dados DEVEM ser adicionadas exclusivamente na camada Bronze, sem impactar
contratos já publicados nas camadas Silver ou Gold. Pipelines críticos DEVEM possuir
testes automatizados que cubram cenários de reprocessamento, validando o comportamento
idempotente exigido pelo Princípio IV.

## Governance

Esta constituição prevalece sobre quaisquer outras práticas ou convenções documentadas
no projeto. Emendas a esta constituição requerem: (1) documentação da mudança proposta e
sua justificativa; (2) aprovação explícita do responsável técnico do projeto; e (3) um
plano de migração quando a mudança impactar contratos já publicados entre camadas.
O versionamento desta constituição segue semântica MAJOR.MINOR.PATCH: MAJOR para remoção
ou redefinição incompatível de princípios; MINOR para adição de novos princípios ou
seções, ou expansão material de diretrizes existentes; PATCH para esclarecimentos,
correções de redação e ajustes não semânticos. Toda revisão de código (pull request)
DEVE verificar conformidade com esta constituição, e qualquer complexidade adicional não
justificada DEVE ser rejeitada.

**Version**: 1.0.1 | **Ratified**: 2026-09-29 | **Last Amended**: 2026-09-29
