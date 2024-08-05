import json
import redis
import asyncio
import pandas as pd
from sqlalchemy import text
from config import settings, logger
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError
from tools.db_connection import PostgreSQLConnection


class VendaProcessor:
    """
    Classe para processar vendas de livros e gerenciar a inserção de dados em várias tabelas no banco de dados.
    """

    def __init__(self):
        """
        Inicializa a instância do VendaProcessor com conexão Redis e conexão com banco de dados.
        """
        self.r = redis.Redis(host=settings.redis.host,
                             port=settings.redis.port)
        self.last_id = '0-0'
        self.db_connection = PostgreSQLConnection()

    async def connect_db(self):
        """
        Estabelece a conexão com o banco de dados.

        Returns:
            None
        """
        await self.db_connection.connect()

    async def close_db(self):
        """
        Fecha a conexão com o banco de dados.

        Returns:
            None
        """
        await self.db_connection.close()

    async def insere_venda_livro(self, df: pd.DataFrame) -> int:
        """
        Insere dados de venda de livros no banco de dados e atualiza tabelas relacionadas.

        Args:
            df (pd.DataFrame): DataFrame contendo os dados da venda.

        Returns:
            int: ID da venda inserida, ou None se a inserção falhar.
        """
        await self.connect_db()
        session = self.db_connection.session

        if not all(col in df.columns for col in settings.colunas_obrigatorias.required_columns):
            logger.error("DataFrame não contém todas as colunas necessárias")
            await self.close_db()
            return None

        try:
            for _, row in df.iterrows():

                logger.info("Inserir na tabela Vendas")
                venda_id = await self.db_connection.executa_insercao_retorna_id(
                    session,
                    settings.queries.insert_livros,
                    df,
                    {
                        "detalhes_compra.tipo_pagamento": "tipo_pagamento",
                        "detalhes_compra.preco": "preco",
                        "detalhes_compra.quantidade": "quantidade",
                        "detalhes_compra.produto_id": "produto_id",
                        "tipo_compra": "tipo_compra",
                        "vendedor_id": "vendedor_id",
                        "cliente_id": "cliente_id",
                        'data': 'data'
                    }
                )

                if venda_id is not None:
                    logger.info(f"Venda ID: {venda_id}")
                    logger.info("Inserir na tabela comissoes")

                    df_temp = df.copy()
                    df_temp['venda_id'] = venda_id
                    df_temp['status'] = "Fechado"

                    await self.db_connection.executa_insercao(
                        session,
                        settings.queries.insert_comissoes,
                        df_temp,
                        {
                            "venda_id": "venda_id",
                            "vendedor_id": "vendedor_id",
                            "data": "data_pagamento",
                            "detalhes_compra.preco": "valor",
                            "status": "status"
                        }
                    )

                    logger.info("Inserir na tabela Royalty ou Remessa")
                    if df.iloc[0]['detalhes_compra.tipo_produto'] == 'livro':
                        await self.db_connection.executa_insercao(
                            session,
                            settings.queries.insert_guias_royalty,
                            df_temp,
                            {
                                "venda_id": "venda_id",
                                'data': 'data_geracao',
                                'status': 'status',
                                'detalhes_compra.valor_royalty': 'valor'
                            }
                        )
                    else:
                        df_temp['data_prevista_entrega'] = datetime.now() + \
                            timedelta(days=15)

                        await self.db_connection.executa_insercao(
                            session,
                            settings.queries.insert_guias_remessa,
                            df_temp,
                            {
                                'venda_id': 'venda_id',
                                'cliente_id': 'cliente_id',
                                'data': 'data_geracao',
                                'status': 'status',
                                'data_prevista_entrega': 'data_prevista_entrega'
                            }
                        )

                    return venda_id
                else:
                    logger.error("Falha ao inserir venda e obter ID")
                    return None

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Erro ao processar compra: {e}")
            return None

        finally:
            await self.close_db()

    async def process_message(self, message):
        """
        Processa mensagens recebidas do Redis, insere dados da venda e atualiza o status no Redis.

        Args:
            message: Mensagem recebida do Redis.

        Returns:
            None
        """
        stream, message_data = message

        for msg_id, msg in message_data:
            json_data = msg[b'data'].decode('utf-8')
            json_dict = json.loads(json_data)
            df = pd.json_normalize(json_dict)

            venda_id = await self.insere_venda_livro(df)
            if venda_id is not None:
                logger.info(
                    "Venda de livro inserida com sucesso no banco de dados")
                self.r.xadd('stream_app3_app1', {
                    'status': 'true', 'venda_id': str(venda_id)})
                logger.info(f"Confirmação com venda_id {
                            venda_id} enviada para app1.")
            else:
                logger.info(
                    "Erro ao inserir a venda de livro no banco de dados.")
                self.r.xadd('stream_app3_app1', {'status': 'false'})
                logger.info("Confirmação enviada para app1.")

            self.last_id = msg_id

    async def main(self):
        """
        Loop principal que lê mensagens do Redis e processa as vendas.

        Returns:
            None
        """
        while True:
            messages = self.r.xread(
                {'stream_app1_app3': self.last_id}, block=1000)
            if messages:
                for message in messages:
                    await self.process_message(message)
            await asyncio.sleep(1)


if __name__ == "__main__":
    venda_processor = VendaProcessor()
    asyncio.run(venda_processor.main())
