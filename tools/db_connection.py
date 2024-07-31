from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import settings

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


db_connection = PostgreSQLConnection()
