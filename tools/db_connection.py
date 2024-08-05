import numpy as np
import pandas as pd
from sqlalchemy import text
from config import settings, logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base

DATABASE_URL = f"postgresql+psycopg2://{settings.database.username}:{settings.database.password}@{
    settings.database.host}:{settings.database.port}/{settings.database.database}"

Base = declarative_base()
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class PostgreSQLConnection:
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine)
        self.connection = None
        self.session = None

    async def connect(self):
        try:
            self.session = self.SessionLocal()
            print("Conexão com o banco de dados estabelecida com sucesso!")
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {e}")

    async def close(self):
        if self.session:
            self.session.close()
            print("Conexão fechada com sucesso!")

    async def executa_busca_retorna_df(self, session: Session, query: str, df: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
        results = []
        for _, linha in df.iterrows():
            # params = {sql_param: row[col]
            #          for col, sql_param in column_mapping.items()}
            params = {sql_param: int(linha[col]) if isinstance(linha[col], np.integer) else linha[col]
                      for col, sql_param in column_mapping.items()}

            result = session.execute(text(query), params)
            rows = result.fetchall()
            if rows:
                results.extend(rows)

        if results:
            result_df = pd.DataFrame(results, columns=result.keys())
        else:
            result_df = pd.DataFrame()

        return result_df

    async def executa_insercao(self, session: Session, query: str, df: pd.DataFrame, column_mapping: dict) -> None:
        """
        Executa inserções no banco de dados com base nos dados do DataFrame.

        Args:
            session (Session): Sessão do SQLAlchemy.
            query (str): Consulta SQL para inserção.
            df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
            column_mapping (dict): Mapeamento de colunas do DataFrame para parâmetros da consulta.
        """
        for _, row in df.iterrows():
            params = {sql_param: row[col]
                      for col, sql_param in column_mapping.items()}
            try:
                session.execute(text(query), params)
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"Erro ao inserir: {e}")

    async def executa_insercao_retorna_id(self, session: Session, query: str, df: pd.DataFrame, column_mapping: dict) -> int:
        """
        Executa uma inserção no banco de dados com base nos dados do DataFrame e retorna o ID gerado.

        Args:
            session (AsyncSession): Sessão assíncrona do SQLAlchemy.
            query (str): Consulta SQL para inserção.
            df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
            column_mapping (dict): Mapeamento de colunas do DataFrame para parâmetros da consulta.

        Returns:
            int: O ID gerado pela inserção.
        """
        for _, row in df.iterrows():
            params = {sql_param: row[col]
                      for col, sql_param in column_mapping.items()}
            try:
                result = session.execute(text(query), params)

                logger.info(result)

                session.commit()
                inserted_id = result.fetchone()[0]
                logger.info(f"Inserido com sucesso, ID: {inserted_id}")
                return inserted_id
            except Exception as e:
                session.rollback()
                logger.error(f"Erro ao inserir: {e}")
                return None


db_connection = PostgreSQLConnection()
