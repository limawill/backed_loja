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
    """
    Classe para processar associações de clientes e enviar e-mails de confirmação.
    """

    def __init__(self):
        """
        Inicializa o objeto AssocProcess, configurando a conexão Redis, PostgreSQL e Mailhog.
        """
        self.r = redis.Redis(host=settings.redis.host,
                             port=settings.redis.port)
        self.last_id = '0-0'
        self.db_connection = PostgreSQLConnection()
        self.mailhog = Mailhog()

    async def envia_email_cliente(self, df: pd.DataFrame, tipo_servico: str) -> bool:
        """
        Envia um e-mail de confirmação para o cliente após a atualização da associação.

        Args:
            df (pd.DataFrame): DataFrame contendo os dados do cliente.
            tipo_servico (str): O tipo de serviço associado à atualização.

        Returns:
            bool: True se o e-mail foi enviado com sucesso, False caso contrário.
        """
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
        """
        Processa uma nova associação de cliente e envia um e-mail de confirmação.

        Args:
            df (pd.DataFrame): DataFrame contendo os detalhes da associação.

        Returns:
            bool: True se a associação foi processada com sucesso, False caso contrário.
        """
        await self.db_connection.connect()
        session = self.db_connection.session

        if not all(col in df.columns for col in settings.colunas_obrigatorias.insert_colunas):
            logger.error("DataFrame não contém todas as colunas necessárias")
            await self.db_connection.close()
            return False

        try:
            for _, row in df.iterrows():

                logger.info("Inserindo cliente no banco")

                await self.db_connection.executa_insercao(
                    session,
                    settings.queries.nova_associacao,
                    df,
                    {"data": "data_geracao",
                     "cliente_id": "cliente_id",
                     "vendedor_id": "vendedor_id",
                     "detalhes_compra.nome_plano": "plano",
                     "detalhes_compra.ativo": "ativo"},
                )

                logger.info("Localizando dados do cliente e enviado email")
                dados_cliente = await self.db_connection.executa_busca_retorna_df(
                    session,
                    settings.queries.select_associacao_cliente,
                    df,
                    {"cliente_id": "cpf"},
                )

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
        """
        Processa o upgrade de uma associação de cliente e envia um e-mail de confirmação.

        Args:
            df (pd.DataFrame): DataFrame contendo os detalhes do upgrade de associação.

        Returns:
            bool: True se o upgrade foi processado com sucesso, False caso contrário.
        """
        await self.db_connection.connect()
        session = self.db_connection.session

        if not all(col in df.columns for col in settings.colunas_obrigatorias.upgrade_colunas):
            logger.error("DataFrame não contém todas as colunas necessárias")
            await self.db_connection.close()
            return False

        try:
            for _, row in df.iterrows():
                logger.info(f"Processando upgrade de associação para o cliente {
                            row['cliente_id']}")
                dados_cliente = await self.db_connection.executa_busca_retorna_df(
                    session,
                    settings.queries.select_cliente,
                    df,
                    {"cliente_id": "cpf"},
                )

                if not dados_cliente.empty:
                    logger.info(f"Cliente encontrado: {row['cliente_id']}")

                    if dados_cliente['ativo'].str.contains('true', case=False, na=False).any():

                        await self.db_connection.executa_insercao(
                            session,
                            settings.queries.update_associacao,
                            df,
                            {"cliente_id": "cliente_id",
                             "detalhes_compra.nome_plano": "plano"
                             },
                        )

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
        """
        Processa a ativação de uma associação de cliente e envia um e-mail de confirmação.

        Args:
            df (pd.DataFrame): DataFrame contendo os detalhes da ativação de associação.

        Returns:
            bool: True se a ativação foi processada com sucesso, False caso contrário.
        """
        await self.db_connection.connect()
        session = self.db_connection.session

        missing_cols = [
            col for col in settings.colunas_obrigatorias.reativacao_colunas if col not in df.columns]
        if missing_cols:
            logger.error(f"DataFrame não contém as seguintes colunas necessárias: {
                         ', '.join(missing_cols)}")
            await self.db_connection.close()
            return False

        try:
            for _, row in df.iterrows():
                logger.info(f"Processando ativação associação {
                            row['cliente_id']}")

                dados_cliente = await self.db_connection.executa_busca_retorna_df(
                    session,
                    settings.queries.select_cliente,
                    df,
                    {"cliente_id": "cpf"},
                )

                if not dados_cliente.empty:
                    logger.info(f"Cliente encontrado: {row['cliente_id']}")
                    if not dados_cliente['ativo'].str.contains('sim', case=False, na=False).any():

                        await self.db_connection.executa_insercao(
                            session,
                            settings.queries.ativacao_associacao,
                            df,
                            {"cliente_id": "cliente_id",
                             "detalhes_compra.ativo": "ativo"
                             },
                        )

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
        """
        Processa uma mensagem recebida do stream Redis.

        Args:
            message: Mensagem recebida do stream Redis.
        """
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
        """
        Método principal que lê mensagens do stream Redis e processa as associações.
        """
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
