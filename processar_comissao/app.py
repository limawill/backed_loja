import json
import time
import redis
import asyncio
import numpy as np
import pandas as pd
from sqlalchemy import text
from typing import Optional, Dict
from config import settings, logger
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError
from tools.db_connection import PostgreSQLConnection

# Configurar Redis
r = redis.Redis(host=settings.redis.host, port=settings.redis.port)


class CalculoComissaoVendas():
    """
    Classe para calcular comissões de vendedores com base em dados fornecidos e enviar resultados através de Redis.
    """

    def __init__(self):
        """
        Inicializa o objeto CalculoComissaoVendas, configurando a conexão Redis e PostgreSQL.
        """
        self.last_id = '0-0'
        self.db_connection = PostgreSQLConnection()

    async def comissao_vendedores(self, df: pd.DataFrame) -> Optional[dict]:
        """
        Calcula a comissão dos vendedores com base nos dados fornecidos.

        Args:
            df (pd.DataFrame): DataFrame contendo os detalhes das vendas.

        Returns:
            Optional[dict]: Uma lista de dicionários contendo as comissões dos vendedores ou None se houver um erro.
        """
        await self.db_connection.connect()
        session = self.db_connection.session
        try:
            resultados = []
            for _, row in df.iterrows():
                query = text(settings.queries.calcular_comissao_geral)
                result = session.execute(query, {
                    'ano': int(row['ano']),
                    'mes': int(row['mes']),
                    'vendedor_id': int(row['vendedor_id'])
                })
                rows = result.fetchall()
                colunas = result.keys()

                # Converte as linhas em uma lista de dicionários
                for row in rows:
                    resultado_dict = {
                        coluna: valor for coluna, valor in zip(colunas, row)}
                    resultados.append(resultado_dict)

            return resultados if resultados else None

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Erro ao calcular comissões: {e}")
            return None

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

            # logger.info(f"Recebemos a mensagem: {list(df.columns)}")

            vendedores = await self.comissao_vendedores(df)
            if vendedores is not None:
                logger.info(
                    "Comissão calculada com sucesso")
                # Enviar confirmação para app1
                vendedores_json = json.dumps(vendedores)
                r.xadd('stream_app5_app1', {
                       'status': 'true', 'vendedores': vendedores_json})
                logger.info("Confirmação enviada para app1.")
            else:
                logger.info(
                    "Erro ao calcular comissões.")
                # Enviar confirmação para app1
                r.xadd('stream_app5_app1', {'status': 'false'})
                logger.info("Confirmação enviada para app1.")

            # Atualizar o ID da última mensagem processada
            self.last_id = msg_id

    async def main(self):
        """
        Método principal que lê mensagens do stream Redis e processa as comissões dos vendedores.
        """
        while True:
            messages = r.xread({'stream_app1_app5': self.last_id}, block=1000)
            if messages:
                for message in messages:
                    await self.process_message(message)
            # Pequena pausa para evitar loop de CPU intensa
            await asyncio.sleep(1)


if __name__ == '__main__':
    calculo_comissao_vendas = CalculoComissaoVendas()
    asyncio.run(calculo_comissao_vendas.main())
