import json
import redis
import asyncio
import pandas as pd
from sqlalchemy import text
from typing import Optional, Dict
from config import settings, logger
from datetime import datetime, timedelta
from json_converter import JsonConverter
from sqlalchemy.exc import SQLAlchemyError
from tools.db_connection import PostgreSQLConnection


class GerarGuiaRemessa:
    def __init__(self):
        self.r = redis.Redis(host=settings.redis.host,
                             port=settings.redis.port)
        self.last_id = '0-0'
        self.db_connection = PostgreSQLConnection()

    async def gera_guira_remessa(self, df: pd.DataFrame) -> json:
        await self.db_connection.connect()
        session = self.db_connection.session
        try:
            for _, row in df.iterrows():
                query = text(settings.queries.gera_guia_remessa)
                result = session.execute(query, {
                    'codigo_venda': int(row['codigo_venda'])})
                rows = result.fetchall()
                guia_remessa = pd.DataFrame(rows, columns=result.keys())
                
                if not guia_remessa.empty:
                    novo_guia_remessa = JsonConverter().adiciona_to_json(guia_remessa)
                    logger.info("Gerando a guia ...")
                    json_data = JsonConverter().convert_to_json(novo_guia_remessa)
                    logger.info("Guia remessa criada!")
                    return json_data
                else:
                    return None
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Erro ao criar guia remessa: {e}")
            return None
        finally:
            await self.db_connection.close()

    async def process_message(self, message):
        stream, message_data = message

        for msg_id, msg in message_data:
            json_data = msg[b'data'].decode('utf-8')
            json_dict = json.loads(json_data)
            df = pd.json_normalize(json_dict)
            remesa = await self.gera_guira_remessa(df)
            if remesa is not None:
                logger.info("A guia de remessa gerada com sucesso")
                self.r.xadd('stream_app6_app1', {
                    'status': 'true', 'remessa': remesa})
                logger.info("Confirmação enviada para app1.")
            else:
                logger.info("Erro ao calcular comissões.")
                self.r.xadd('stream_app6_app1', {'status': 'false'})
                logger.info("Confirmação enviada para app1.")
            self.last_id = msg_id

    async def main(self):
        while True:
            messages = self.r.xread(
                {'stream_app1_app6': self.last_id}, block=1000)
            if messages:
                for message in messages:
                    await self.process_message(message)
            await asyncio.sleep(1)


if __name__ == '__main__':
    gerar_guia_remessa = GerarGuiaRemessa()
    asyncio.run(gerar_guia_remessa.main())
