import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from tools.db_connection import PostgreSQLConnection


class ProdutoFisicoProcessor:
    def __init__(self):
        self.db_connection = PostgreSQLConnection()

    async def processar_compra(self, df: pd.DataFrame) -> int:
        # Conectar ao banco de dados
        await self.db_connection.connect()
        session = self.db_connection.session

        print("Colunas do DataFrame:", df.columns)
        print("Primeiras linhas do DataFrame:", df.head())
        df.to_csv('recebida_livro.csv', index=False)

        try:
            # Converter DataFrame para um formato apropriado
            for _, row in df.iterrows():
                query = text("""
                INSERT INTO vendas (data, cliente_id, vendedor_id, tipo_compra, produto_id, quantidade, preco, tipo_pagamento)
                VALUES (:data, :cliente_id, :vendedor_id, :tipo_compra, :produto_id, :quantidade, :preco, :tipo_pagamento)
                RETURNING id
                """)

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
                session.commit()

            return venda_id

        except SQLAlchemyError as e:
            session.rollback()
            raise Exception(f"Erro ao processar compra: {e}")

        finally:
            await self.db_connection.close()
