import pandas as pd
from config import settings
from sqlalchemy import text
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError
from tools.db_connection import PostgreSQLConnection


class AssocicacaoProcessor:
    def __init__(self):
        self.db_connection = PostgreSQLConnection()

    async def processar_associacao(self, df: pd.DataFrame) -> bool:

        await self.db_connection.connect()
        session = self.db_connection.session

        try:
            # Converter DataFrame para um formato apropriado
            for _, row in df.iterrows():
                query = text(settings.queries.nova_associacao)
                session.execute(query, {
                    'data_geracao': row['data'],
                    'cliente_id': row['cliente_id'],
                    'plano': row['detalhes_compra.tipo_produto'],
                    'ativo': True
                })
                session.commit()
            return True

        except SQLAlchemyError as e:
            session.rollback()
            print(Exception(f"Erro ao processar compra: {e}"))
            return False

        finally:
            await self.db_connection.close()
