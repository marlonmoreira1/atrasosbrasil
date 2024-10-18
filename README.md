# Monitor de Voos Brasil

## Introdução ao Projeto

### Contextualização do Problema
Atrasos e cancelamentos de voos são problemas recorrentes em muitos aeroportos do Brasil, impactando diretamente passageiros, especialmente profissionais de negócios que dependem de horários rigorosos. Esses atrasos podem causar sérios inconvenientes, afetando compromissos e conexões com outros voos.

### Objetivo do Projeto
O objetivo deste projeto é criar uma solução de dados de ponta a ponta para monitorar os atrasos nos principais aeroportos do Brasil. Com isso, os usuários podem acessar informações sobre a pontualidade de diferentes companhias aéreas, aeroportos e horários de voo, ajudando-os a planejar de maneira mais eficiente.

### Público-Alvo
- **Profissionais de Negócios**: Viajantes frequentes que necessitam de pontualidade.
- **Viajantes com Conexões**: Passageiros com margens limitadas para imprevistos.
- **Empresas e Agências de Viagem**: Profissionais que podem usar os dados para melhorar recomendações a clientes.

## Entendimento dos Dados

### Fontes de Dados
Os dados são coletados do [FlightRadar24](https://www.flightradar24.com), uma plataforma que fornece informações em tempo real sobre voos em todo o mundo.

### Estrutura dos Dados
## Estrutura de Dados Coletados

| Coluna             | Descrição                                                       |
|--------------------|-----------------------------------------------------------------|
| **Data do Voo**     | A data de cada voo monitorado                                   |
| **Horário Previsto**| O horário originalmente programado para a partida ou chegada    |
| **Horário Realizado**| O horário real em que o voo partiu ou chegou                   |
| **Número do Voo**   | Identificação única do voo (código da companhia + número)       |
| **Companhia Aérea** | Nome da companhia aérea responsável pelo voo                    |
| **Aeroporto de Origem** | O aeroporto de onde o voo partiu                          |
| **Aeroporto de Destino**| O aeroporto para onde o voo foi destinado                 |
| **Cidade de Origem**| A cidade de onde o voo partiu                                   |
| **Cidade de Destino**| A cidade para onde o voo foi destinado                        |
| **Status do Voo**   | Indicação se o voo estava no horário, atrasado ou cancelado     |
| **Tipo de Aeronave**| Modelo da aeronave utilizada no voo                             |


## Coleta de Dados

### Ferramentas e Técnicas
A coleta é feita por meio de web scraping, armazenando os dados em formato Parquet no Azure Blob Storage. O processo é automatizado com GitHub Actions para garantir a execução diária da coleta.

### Armazenamento e Processamento
Utilizamos uma arquitetura de medallion:
- **Camada Bronze**: Dados brutos coletados.
- **Camada Prata**: Enriquecimento com informações demográficas.
- **Camada Ouro**: Limpeza e organização dos dados.

## Automação do Processo
O pipeline de coleta e transformação é gerenciado por GitHub Actions e Azure Data Factory, garantindo atualização automática e precisão dos dados.
![Frame 10](https://github.com/user-attachments/assets/5f6a813c-5207-4faa-9b2d-76cf650daa37)


## Análise e Visualização dos Dados

[Link para o App Monitor de Voos](https://bit.ly/VoosMonitorBrasil)

### Estrutura do App no Power BI
O aplicativo possui quatro páginas principais:
1. **Página de Pesquisa**: Permite busca personalizada de voos.
2. **Página de Monitoramento de Atrasos**: Exibe percentuais de atrasos por aeroporto e companhia.
3. **Página de Embarque x Desembarque**: Analisa a relação entre atrasos no embarque e desembarque.
4. **Página de Análise Histórica**: Mostra a evolução dos dados ao longo do tempo.


![Captura de tela 2024-10-18 192456](https://github.com/user-attachments/assets/8f7404ca-2ddd-4913-8ed5-77bbb59b9e3d)
Página de pesquisa


### Principais Métricas
- % de Atraso
- % de Cancelamento
- % On-Time
- Tempo de Espera


## Próximos Passos e Melhorias
Futuras iterações do projeto podem incluir a integração de dados de preços de passagens aéreas, enriquecendo a experiência do usuário e tornando o aplicativo uma ferramenta ainda mais valiosa.


