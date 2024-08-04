import json
import asyncio
import redis
import pandas as pd
from tools.mailhog import Mailhog
from config import settings, logger
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from tools.db_connection import PostgreSQLConnection
from typing import Optional


class AssocProcess:
    def __init__(self):
        self.r = redis.Redis(host=settings.redis.host,
                             port=settings.redis.port)
        self.last_id = '0-0'
        self.db_connection = PostgreSQLConnection()
        self.mailhog = Mailhog()

    async def envia_email_cliente(self, df: pd.DataFrame, tipo_servico: str) -> bool:
        nome = df['nome'].values[0]
        email = df['email'].values[0]
        titulo = "Sua associação foi atualizada com sucesso"

        if tipo_servico != 'Assinatura':
            corpo = f"""Olá {nome},
                Recebemos a sua solicitação e gostaríamos de informar que
                seu/sua {tipo_servico} da assinatura foi realizado
                com sucesso.

                Atenciosamente,
                Equipe backend
                """
        else:
            corpo = f"""Olá {nome},
                Recebemos a sua solicitação e gostaríamos de informar que
                sua {tipo_servico} foi criada com sucesso.

                Atenciosamente,
                Equipe backend
                """

        try:
            mensagem = self.mailhog.send_email(nome, email, titulo, corpo)
            if mensagem:
                return True
            else:
                logger.error("Erro ao enviar email")
                return False
        except Exception as e:
            logger.error(f"Erro ao enviar email: {e}")
            return False

    async def processar_associacao(self, df: pd.DataFrame) -> bool:
        await self.db_connection.connect()
        session = self.db_connection.session

        required_columns = [
            'data', 'cliente_id', 'vendedor_id',
            'detalhes_compra.nome_plano', 'detalhes_compra.ativo'
        ]
        if not all(col in df.columns for col in required_columns):
            logger.error("DataFrame não contém todas as colunas necessárias")
            await self.db_connection.close()
            return False

        try:
            for _, row in df.iterrows():
                query = text(settings.queries.nova_associacao)
                session.execute(query, {
                    'data_geracao': row['data'],
                    'cliente_id': row['cliente_id'],
                    'vendedor_id': row['vendedor_id'],
                    'plano': row['detalhes_compra.nome_plano'],
                    'ativo': row['detalhes_compra.ativo']
                })
                session.commit()

                query = text(settings.queries.select_email_cliente)
                result = session.execute(query, {'cpf': row['cliente_id']})
                rows = result.fetchall()
                dados_cliente = pd.DataFrame(rows, columns=result.keys())

                if await self.envia_email_cliente(dados_cliente, 'Assinatura'):
                    logger.info("Email enviado com sucesso")
                    return True
                else:
                    return False

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Erro ao processar compra: {e}")
            return False

        finally:
            await self.db_connection.close()

    async def upgrade_associacao(self, df: pd.DataFrame) -> bool:
        await self.db_connection.connect()
        session = self.db_connection.session

        required_columns = [
            'data', 'cliente_id', 'vendedor_id',
            'detalhes_compra.nome_plano', 'detalhes_compra.ativo'
        ]
        if not all(col in df.columns for col in required_columns):
            logger.error("DataFrame não contém todas as colunas necessárias")
            await self.db_connection.close()
            return False

        try:
            for _, row in df.iterrows():
                logger.info(f"Processando upgrade de associação para o cliente {
                            row['cliente_id']}")
                query = text(settings.queries.select_cliente)
                result = session.execute(query, {'cpf': row['cliente_id']})
                rows = result.fetchall()
                dados_cliente = pd.DataFrame(rows, columns=result.keys())

                if not dados_cliente.empty:
                    logger.info(f"Cliente encontrado: {row['cliente_id']}")
                    if dados_cliente['ativo'].str.contains('true', case=False, na=False).any():
                        query = text(settings.queries.update_associacao)
                        session.execute(query, {
                            'novo_plano': row['detalhes_compra.nome_plano'],
                            'cliente_id': row['cliente_id']
                        })
                        session.commit()
                        if await self.envia_email_cliente(dados_cliente, 'Upgrade'):
                            logger.info("Email enviado com sucesso")
                            return True
                        else:
                            return False

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Erro ao processar compra: {e}")
            return False

        finally:
            await self.db_connection.close()

    async def ativacao_associacao(self, df: pd.DataFrame) -> bool:
        await self.db_connection.connect()
        session = self.db_connection.session

        required_columns = [
            'data', 'cliente_id', 'vendedor_id',
            'detalhes_compra.nome_plano', 'detalhes_compra.ativo'
        ]
        if not all(col in df.columns for col in required_columns):
            logger.error("DataFrame não contém todas as colunas necessárias")
            await self.db_connection.close()
            return False

        try:
            for _, row in df.iterrows():
                logger.info(f"Processando ativação associação {
                            row['cliente_id']}")
                query = text(settings.queries.select_cliente)
                result = session.execute(query, {'cpf': row['cliente_id']})
                rows = result.fetchall()
                dados_cliente = pd.DataFrame(rows, columns=result.keys())

                if not dados_cliente.empty:
                    logger.info(f"Cliente encontrado: {row['cliente_id']}")
                    if not dados_cliente['ativo'].str.contains('sim', case=False, na=False).any():
                        query = text(settings.queries.ativacao_associacao)
                        session.execute(query, {
                            'ativo': row['detalhes_compra.ativo'],
                            'cliente_id': row['cliente_id']
                        })
                        session.commit()
                        if await self.envia_email_cliente(dados_cliente, 'Ativação'):
                            logger.info("Email enviado com sucesso")
                            return True
                        else:
                            return False

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Erro ao processar compra: {e}")
            return False

        finally:
            await self.db_connection.close()

    async def process_message(self, message):
        stream, message_data = message

        for msg_id, msg in message_data:
            json_data = msg[b'data'].decode('utf-8')
            json_dict = json.loads(json_data)
            df = pd.json_normalize(json_dict)

            if (df['tipo_assinatura'] == 'nova_associacao').all():
                if await self.processar_associacao(df):
                    logger.info("Associação criada com sucesso")
                    self.r.xadd('stream_app2_app1', {'status': 'true'})
                    logger.info("Confirmação enviada para app1.")
                else:
                    logger.info("Problemas na associação - Verifique o log")
                    self.r.xadd('stream_app2_app1', {'status': 'false'})
                    logger.info("Erro enviada para app1.")
            elif (df['tipo_assinatura'] == 'upgrade_associacao').all():
                if await self.upgrade_associacao(df):
                    logger.info("Associação criada com sucesso")
                    self.r.xadd('stream_app2_app1', {'status': 'true'})
                    logger.info("Confirmação enviada para app1.")
                else:
                    logger.info("Problemas na associação - Verifique o log")
                    self.r.xadd('stream_app2_app1', {'status': 'false'})
                    logger.info("Erro enviada para app1.")
            else:
                if await self.ativacao_associacao(df):
                    logger.info("Associação criada com sucesso")
                    self.r.xadd('stream_app2_app1', {'status': 'true'})
                    logger.info("Confirmação enviada para app1.")
                else:
                    logger.info("Problemas na associação - Verifique o log")
                    self.r.xadd('stream_app2_app1', {'status': 'false'})
                    logger.info("Erro enviada para app1.")
            self.last_id = msg_id

    async def main(self):
        while True:
            messages = self.r.xread(
                {'stream_app1_app2': self.last_id}, block=1000)
            if messages:
                for message in messages:
                    await self.process_message(message)
            await asyncio.sleep(1)


if __name__ == '__main__':
    assoc_process = AssocProcess()
    asyncio.run(assoc_process.main())
