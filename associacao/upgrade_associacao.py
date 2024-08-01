import logging
import pandas as pd
from config import settings
from sqlalchemy import text
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError
from tools.db_connection import PostgreSQLConnection

# Configuração do log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UpgradeProcessor:
    def __init__(self):
        self.db_connection = PostgreSQLConnection()

    async def upgrade_associacao(self, df: pd.DataFrame) -> bool:

        await self.db_connection.connect()
        session = self.db_connection.session

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
                    if dados_cliente['ativo'].isin([True]).any():
                        query = text(settings.queries.update_associacao)

                        session.execute(query, {
                            'novo_plano': row['data'],
                            'cliente_id': row['detalhes_compra.tipo_produto']
                        })
                        session.commit()

            return True

        except SQLAlchemyError as e:
            session.rollback()
            print(Exception(f"Erro ao processar compra: {e}"))
            return False

        finally:
            await self.db_connection.close()
