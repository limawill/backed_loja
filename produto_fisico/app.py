import json
import redis
import asyncio
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
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

    async def insere_dados_banco(self, df: pd.DataFrame, queries: str, parameters: dict) -> int:

        await self.connect_db()
        session = self.db_connection.session

        try:
            for _, row in df.iterrows():

                logger.info(f"Inserir dados na tabela: {queries}")
                id_retorno = await self.db_connection.executa_insercao_retorna_id(
                    session,
                    queries,
                    df,
                    parameters
                )

                if id_retorno is not None:
                    logger.info(f"Venda ID: {id_retorno}")
                    return id_retorno

                else:
                    logger.error("Falha ao inserir e obter ID")
                    return None

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Erro ao processar dados: {e}")
            return None

        finally:
            await self.close_db()

    async def insere_venda_livro(self, df: pd.DataFrame) -> int:
        """
        Insere dados de venda de livros no banco de dados e atualiza tabelas relacionadas.

        Args:
            df (pd.DataFrame): DataFrame contendo os dados da venda.

        Returns:
            int: ID da venda inserida, ou None se a inserção falhar.
        """

        if not all(col in df.columns for col in settings.colunas_obrigatorias.colunas_exigidas_vendas):
            logger.error("DataFrame não contém todas as colunas necessárias")
            await self.close_db()
            return None

        venda_id = await self.insere_dados_banco(df,
                                                 settings.queries.insert_livros,
                                                 {
                                                     "detalhes_compra.tipo_pagamento": "tipo_pagamento",
                                                     "detalhes_compra.preco": "preco",
                                                     "detalhes_compra.quantidade": "quantidade",
                                                     "detalhes_compra.produto_id": "produto_id",
                                                     "tipo_compra": "tipo_compra",
                                                     "vendedor_id": "vendedor_id",
                                                     "cliente_id": "cliente_id",
                                                     'data': 'data'
                                                 })
        if venda_id is not None:
            logger.info(f"Venda ID: {venda_id}")
            return venda_id

        else:
            logger.error("Falha ao inserir venda e obter ID")
            return None

    async def insere_venda_comissao(self, df: pd.DataFrame, venda_id: int) -> bool:
        logger.info("Inserir na tabela comissoes")
        df_temp = df.copy()
        df_temp['venda_id'] = venda_id
        df_temp['status'] = "Fechado"

        id_comissao = await self.insere_dados_banco(df_temp,
                                                    settings.queries.insert_comissoes,
                                                    {
                                                        "venda_id": "venda_id",
                                                        "vendedor_id": "vendedor_id",
                                                        "data": "data_pagamento",
                                                        "detalhes_compra.preco": "valor",
                                                        "status": "status"
                                                    })
        if id_comissao is not None:
            logger.info(f"Comissao ID: {id_comissao}")
            return id_comissao

        else:
            logger.error("Falha ao inserir comissão e obter ID")
            return None

    async def insere_venda_royalty_remessa(self, df: pd.DataFrame, venda_id: int) -> int:
        """
        Insere dados de royalty ou remessa no banco de dados e atualiza tabelas relacionadas.

        Args:
            df (pd.DataFrame): DataFrame contendo os dados da venda.
            venda_id (int): ID da venda inserida, ou None se a inserção falhar.

        Returns:
            int: ID da royalty ou remessa inserida, ou None se a inserção falhar.
        """
        df_temp = df.copy()
        df_temp['venda_id'] = venda_id
        df_temp['status'] = "Fechado"

        logger.info("Inserir na tabela Royalty ou Remessa")

        if df.iloc[0]['detalhes_compra.tipo_produto'] == 'livro':
            id_comissao = await self.insere_dados_banco(df_temp,
                                                        settings.queries.insert_guias_royalty,
                                                        {
                                                            "venda_id": "venda_id",
                                                            'data': 'data_geracao',
                                                            'status': 'status',
                                                            'detalhes_compra.valor_royalty': 'valor'
                                                        })
            if id_comissao is not None:
                logger.info(f"Royalty ID: {id_comissao}")
                return id_comissao

            else:
                logger.error("Falha ao inserir Royalty e obter ID")
                return None

        else:

            df_temp['data_prevista_entrega'] = datetime.now() + \
                timedelta(days=15)

            id_comissao = await self.insere_dados_banco(df_temp,
                                                        settings.queries.insert_guias_remessa,
                                                        {
                                                            'venda_id': 'venda_id',
                                                            'cliente_id': 'cliente_id',
                                                            'data': 'data_geracao',
                                                            'status': 'status',
                                                            'data_prevista_entrega': 'data_prevista_entrega'
                                                        })
            if id_comissao is not None:
                logger.info(f"Remessa ID: {id_comissao}")
                return id_comissao

            else:
                logger.error("Falha ao inserir Remessa e obter ID")
                return None

            return venda_id

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
                retorno_id = await self.insere_venda_comissao(df, venda_id)
                if venda_id is not None:
                    retorno_id = await self.insere_venda_royalty_remessa(df, venda_id)

            if venda_id and retorno_id:
                logger.info(
                    "Venda de Produto fisico inserida com sucesso no banco de dados")
                self.r.xadd('stream_app3_app1', {
                    'status': 'true', 'venda_id': str(venda_id)})
                logger.info(f"Confirmação id venda: {
                            venda_id} enviada para app1.")
            else:
                logger.info(
                    "Erro ao inserir a venda de Produto fisico no banco de dados.")
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
