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
        self.r = redis.Redis(host=settings.redis.host, port=settings.redis.port)
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

        required_columns = [
            'data', 'cliente_id', 'vendedor_id', 'tipo_compra',
            'detalhes_compra.produto_id', 'detalhes_compra.quantidade',
            'detalhes_compra.preco', 'detalhes_compra.tipo_pagamento'
        ]
        if not all(col in df.columns for col in required_columns):
            logger.error("DataFrame não contém todas as colunas necessárias")
            await self.close_db()
            return None

        try:
            for _, row in df.iterrows():
                query = text(settings.queries.insert_livros)
                logger.info("Inserir na tabela Vendas")
                result = session.execute(query, {
                    'data': row['data'],
                    'cliente_id': row['cliente_id'],
                    'vendedor_id': row['vendedor_id'],
                    'tipo_compra': row['tipo_compra'],
                    'produto_id': row['detalhes_compra.produto_id'],
                    'quantidade': row['detalhes_compra.quantidade'],
                    'preco': row['detalhes_compra.preco'],
                    'tipo_pagamento': row['detalhes_compra.tipo_pagamento']
                })
                venda_id = result.fetchone()[0]
                logger.info(f"Venda ID: {venda_id}")

                logger.info("Inserir na tabela comissoes")
                query_comissoes = text(settings.queries.insert_comissoes)
                session.execute(query_comissoes, {
                    'venda_id': venda_id,
                    'vendedor_id': row['vendedor_id'],
                    'data_pagamento': row['data'],
                    'valor': row['detalhes_compra.preco'],
                    'status': 'fechado'
                })

                logger.info("Inserir na tabela Royalty ou Remessa")
                if row['detalhes_compra.tipo_produto'] == 'livro':
                    query_guias_royalty = text(settings.queries.insert_guias_royalty)
                    session.execute(query_guias_royalty, {
                        'venda_id': venda_id,
                        'data_geracao': row['data'],
                        'status': 'enviado',
                        'valor': row['detalhes_compra.valor_royalty']
                    })
                else:
                    query_guias_remessa = text(settings.queries.insert_guias_remessa)
                    session.execute(query_guias_remessa, {
                        'venda_id': venda_id,
                        'cliente_id': row['cliente_id'],
                        'data_geracao': row['data'],
                        'status': 'pendente',
                        'data_prevista_entrega': datetime.now() + timedelta(days=15)
                    })

                session.commit()
            return venda_id

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
                logger.info("Venda de livro inserida com sucesso no banco de dados")
                self.r.xadd('stream_app3_app1', {
                    'status': 'true', 'venda_id': str(venda_id)})
                logger.info(f"Confirmação com venda_id {venda_id} enviada para app1.")
            else:
                logger.info("Erro ao inserir a venda de livro no banco de dados.")
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
            messages = self.r.xread({'stream_app1_app3': self.last_id}, block=1000)
            if messages:
                for message in messages:
                    await self.process_message(message)
            await asyncio.sleep(1)

if __name__ == "__main__":
    venda_processor = VendaProcessor()
    asyncio.run(venda_processor.main())

