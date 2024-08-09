# Desafio de Solução Tech - BHub

Este projeto consiste no desenvolvimento de um sistema de processamento de pedidos para uma grande empresa, enfrentando o desafio de centralizar e automatizar um conjunto complexo e em constante evolução de regras de negócio. O sistema visa consolidar as diversas práticas comerciais que, até então, eram gerenciadas de maneira dispersa, com uma combinação de processos manuais e automatizados. Pela descrição do documento dar a entender que é utilizado uma mistura de práticas comerciais manuais e automatizadas, dependendo muitas vezes conhecimento de funcionários, que conhecem regras específicas não documentadas.

## Objetivo

O principal objetivo deste projeto é criar um sistema robusto e flexível que automatize o processamento de pedidos, incluindo a geração de guias de remessa, ativações de associações, e pagamentos de comissões, entre outros. O sistema deve ser capaz de gerenciar de forma eficiente as regras de negócio complexas e arbitrárias, permitindo ajustes rápidos para acomodar mudanças e exceções. Para abordar esses desafios, foi desenvolvido um sistema que integra todas as regras de negócio identificadas, automatizando processos anteriormente geridos de forma ad-hoc. A solução inclui a capacidade de gerar guias de remessa, processar associações e upgrades, enviar notificações por e-mail, e calcular comissões automaticamente, conforme ilustrado no fluxograma abaixo:

<img src="/adds/Imagens/fluxograma.jpg">

Com o objetivo de unificar essas práticas em uma única aplicação, foi necessário projetar um sistema que pudesse acomodar a diversidade das regras de negócio, sem sacrificar a flexibilidade ou a eficiência.

## Tecnologias Usadas

| Tecnologia     | Descrição                                                                              |
| -------------- | -------------------------------------------------------------------------------------- |
| Python         | [pLinguagem de programação utilizada para implementar o serviço                        |
| FastAPI        | Framework para criar APIs web assíncronas e de alto desempenho                         |
| Redis          | Banco de dados em memória para gerenciamento de mensagens e streams                    |
| Pydantic       | Biblioteca para validação de dados e criação de modelos de dados                       |
| Docker         | Plataforma que permite a criação, implantação e execução de aplicativos em contêineres |
| Docker Compose | Ferramenta que facilita a definição e execução de aplicativos multi-contêineres Docker |
| PostgreSQL     | Sistema de gerenciamento de banco de dados relacional robusto e open-source            |
| SQLAlchemy     | Biblioteca ORM para interagir com o banco de dados PostgreSQL                          |
| Pandas         | Biblioteca para manipulação e análise de dados em formato de DataFrame                 |
| Mailhog        | Ferramenta para envio de e-mails em ambientes de desenvolvimento                       |
| Asyncio        | Biblioteca para programação assíncrona em Python                                       |

## Instalação

Com o seu sistema precisa ter o git e docker já instalado. Com os mesmo configurados e rodando faça o clone do projeto:

```sh
git clone git@github.com:limawill/backed_loja.git
```

Dentro do projeto crie seu Virtual Environment (venv) e os ative (comandos abaixo para linux/mac)

```sh
python -m venv .venv
source .venv/bin/activate
```

Após isso na raiz do projeto:

```sh
docker-compose down --remove-orphans; docker-compose up --build
```

Aguarde a mensagem apararecer no terminal:

```sh
app1                      | INFO:     Started server process [1]
app1                      | INFO:     Waiting for application startup.
app1                      | INFO:     Application startup complete.
app1                      | INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
```

## Documentação

### Uso

O sistema utiliza o FastAPI para criar endpoints que recebem URLs com parâmetros específicos. Na pasta ADDs, você encontrará uma subpasta chamada Postman, que contém todas as chamadas de API (URLs) e exemplos detalhados de como os dados devem ser enviados. Ao todo, o sistema oferece 8 endpoints, que seguem a lógica apresentada no fluxograma acima. O coração do projeto é o arquivo 'app.py', ele é o responsável pela configuração e implementação para um serviço de processamento de transações baseado em FastAPI e Redis. Este serviço lida com diferentes tipos de transações, incluindo compras, associações, solicitações de streaming, comissões e remessas. A arquitetura usa o Redis para a comunicação entre diferentes partes do sistema, enviando e recebendo dados através de streams.
Abaixo, você encontrará exemplos de chamadas e uma breve descrição de cada endpoint.

### Método POST

#### Produto Fisico

http://localhost:8001/processar_compra

> Responsável por processar vendas de livros, inserir dados em tabelas relacionadas no banco de dados e gerenciar a comunicação com o Redis. A classe inclui métodos para inserir dados de vendas de livros, comissões e royalties/remessas, além de processar mensagens recebidas de um stream Redis.

#### Assinatura/Associação

http://localhost:8001/processar_associacao

> Responsável pelo processamento de associações de clientes e pelo envio de e-mails de confirmação. Ela lida com diferentes tipos de operações relacionadas a associações de clientes, incluindo a criação, atualização e ativação de associações. A classe utiliza um fluxo de mensagens assíncrono para processar as solicitações recebidas e interage com serviços externos para completar as operações.

#### Streaming

http://localhost:8001/streaming

> Responsável por processar solicitações de vídeos, enviar e-mails aos clientes com os detalhes dos vídeos disponíveis e gerenciar a comunicação com o Redis e o banco de dados. A classe inclui métodos para enviar e-mails aos clientes, processar solicitações de vídeo e processar mensagens recebidas.

### Método GET

#### Comissão Vendedores

http://localhost:8001/calcular_comissao

> Responsável por calcular as comissões de vendedores com base nas vendas registradas e enviar os resultados através de um stream Redis. O processo inclui a conexão com o banco de dados PostgreSQL para realizar cálculos e interagir com o Redis para receber dados e enviar respostas.

#### Gera Remessa e Royalties

http://localhost:8001/gera_remessa

> Responsável por gerar guias de remessa com base nos dados fornecidos e enviar os resultados através de um stream Redis. A classe inclui métodos para converter dados em JSON, adicionar campos faltantes ao DataFrame, gerar a guia de remessa e processar mensagens recebidas.

## Funcionalidades

É possivel validar o envio de email pelo 'Mailhog' após o disparo do endpoint de streaming por exemplo, acessando o endereço pelo navegador:

```sh
http://localhost:8025/#
```

Você tem acesso aos emails enviado pela API

<img src="/adds/Imagens/mailhog.png">
