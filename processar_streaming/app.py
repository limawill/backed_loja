import json
import redis
import asyncio
import pandas as pd
from sqlalchemy import text
from typing import Optional, Dict
from tools.mailhog import Mailhog
from config import settings, logger
from sqlalchemy.exc import SQLAlchemyError
from tools.db_connection import PostgreSQLConnection


class VideoProcessor:
    def __init__(self):
        self.r = redis.Redis(host=settings.redis.host,
                             port=settings.redis.port)
        self.last_id = '0-0'
        self.db_connection = PostgreSQLConnection()
        self.mailhog = Mailhog()

    async def envia_email_cliente(self, df_cliente: pd.DataFrame, df_video: pd.DataFrame) -> Optional[dict]:
        nome = df_cliente['nome'].values[0]
        email = df_cliente['email'].values[0]
        titulo = "Seu vídeo está disponível!"

        logger.info("Montagem do corpo do e-mail")
        corpo = f"Olá {nome},\n\n"
        corpo += "Recebemos a sua solicitação e gostaríamos de informar que seu vídeo está disponível.\n\n"
        corpo += "Aqui estão os detalhes dos vídeos selecionados:\n\n"
        for index, row in df_video.iterrows():
            corpo += f"Nome: {row['nome']}\n"
            corpo += f"Link: {row['link']}\n\n"
        corpo += "Atenciosamente,\nEquipe backend"

        try:
            mensagem = self.mailhog.send_email(nome, email, titulo, corpo)
            if mensagem:  # Verificar se o envio foi bem-sucedido
                logger.info("Preparando retorno do método")
                resultado_streaming = {
                    'cpf': df_cliente['cpf'].values[0],
                    'videos': df_video.to_dict(orient='records')
                }
                return resultado_streaming
            else:
                logger.error("Erro ao enviar email")
                return None
        except Exception as e:
            logger.error(f"Erro ao enviar email: {e}")
            return None

    async def envio_video(self, df: pd.DataFrame) -> Optional[dict]:
        await self.db_connection.connect()
        session = self.db_connection.session

        required_columns = ['data', 'cliente_id',
                            'detalhes_compra.id_streaming']
        if not all(col in df.columns for col in required_columns):
            logger.error("DataFrame não contém todas as colunas necessárias")
            await self.db_connection.close()
            return None

        try:
            for _, row in df.iterrows():
                logger.info("Localizando dados do vídeo solicitado")
                query = text(settings.queries.select_streaming)
                result = session.execute(
                    query, {'id_steaming': row['detalhes_compra.id_streaming']})
                rows = result.fetchall()
                dados_video = pd.DataFrame(rows, columns=result.keys())

                logger.info(f"Video  : {dados_video}")

                logger.info("Localizando dados do cliente")
                query = text(settings.queries.select_email_cliente)
                result = session.execute(query, {'cpf': row['cliente_id']})
                rows = result.fetchall()
                dados_cliente = pd.DataFrame(rows, columns=result.keys())

                logger.info(f"Cliente: {dados_cliente}")

                if not dados_video.empty and not dados_cliente.empty:
                    logger.info(
                        "Dados do cliente e video localizados! Preparando o envio...")
                    retorno_dados = await self.envia_email_cliente(dados_cliente, dados_video)
                    if retorno_dados and retorno_dados.get('videos'):
                        logger.info("Email enviado com sucesso")
                        return retorno_dados
                    else:
                        return None
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Erro ao processar Streaming: {e}")
            return None
        finally:
            await self.db_connection.close()

    async def process_message(self, message):
        stream, message_data = message
        for msg_id, msg in message_data:
            json_data = msg[b'data'].decode('utf-8')
            json_dict = json.loads(json_data)
            df = pd.json_normalize(json_dict)

            return_final = await self.envio_video(df)
            if return_final:
                logger.info("Videos enviado com sucesso")
                self.r.xadd('stream_app4_app1', {
                            'status': 'true', 'video': str(return_final)})
                logger.info("Confirmação enviada para app1.")
            else:
                logger.info("Problemas na associação - Verifique o log")
                self.r.xadd('stream_app4_app1', {'status': 'false'})
                logger.info("Erro enviada para app1.")
            self.last_id = msg_id

    async def main(self):
        while True:
            messages = self.r.xread(
                {'stream_app1_app4': self.last_id}, block=1000)
            if messages:
                for message in messages:
                    await self.process_message(message)
            await asyncio.sleep(1)


if __name__ == "__main__":
    processor = VideoProcessor()
    asyncio.run(processor.main())
