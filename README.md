# Desafio de Solução Tech - BHub

Este projeto consiste no desenvolvimento de um sistema de processamento de pedidos para uma grande empresa, enfrentando o desafio de centralizar e automatizar um conjunto complexo e em constante evolução de regras de negócio. O sistema visa consolidar as diversas práticas comerciais que, até então, eram gerenciadas de maneira dispersa, com uma combinação de processos manuais e automatizados. Pela descrição do documento dar a entender que é utilizado uma mistura de práticas comerciais manuais e automatizadas, dependendo muitas vezes conhecimento de funcionários, que conhecem regras específicas não documentadas.

## Objetivo

O principal objetivo deste projeto é criar um sistema robusto e flexível que automatize o processamento de pedidos, incluindo a geração de guias de remessa, ativações de associações, e pagamentos de comissões, entre outros. O sistema deve ser capaz de gerenciar de forma eficiente as regras de negócio complexas e arbitrárias, permitindo ajustes rápidos para acomodar mudanças e exceções. Para abordar esses desafios, foi desenvolvido um sistema que integra todas as regras de negócio identificadas, automatizando processos anteriormente geridos de forma ad-hoc. A solução inclui a capacidade de gerar guias de remessa, processar associações e upgrades, enviar notificações por e-mail, e calcular comissões automaticamente, conforme ilustrado no fluxograma abaixo:

<img src="/imagens/fluxograma.jpg">

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
