import pandas as pd
from config import settings
from sqlalchemy import text
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError
from tools.db_connection import PostgreSQLConnection


class ProdutoFisicoProcessor:
    def __init__(self):
        self.db_connection = PostgreSQLConnection()

    async def processar_compra(self, df: pd.DataFrame) -> int:
        """
        Processa um DataFrame contendo informações de compras e insere os dados na tabela 'vendas' do banco de dados.

        Args:
            df (pd.DataFrame): DataFrame contendo as informações das compras. As colunas esperadas são:
                - 'data': Data da compra.
                - 'cliente_id': ID do cliente.
                - 'vendedor_id': ID do vendedor.
                - 'tipo_compra': Tipo de compra.
                - 'detalhes_compra.produto_id': ID do produto.
                - 'detalhes_compra.quantidade': Quantidade do produto.
                - 'detalhes_compra.preco': Preço do produto.
                - 'detalhes_compra.tipo_pagamento': Tipo de pagamento.

        Returns:
            int: O ID da última venda inserida no banco de dados.

        Raises:
            Exception: Se ocorrer um erro ao processar a compra, uma exceção será levantada com a mensagem de erro.

        """
        await self.db_connection.connect()
        session = self.db_connection.session

        try:
            # Converter DataFrame para um formato apropriado
            for _, row in df.iterrows():
                query = text(settings.queries.insert_livros)

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
                
                if row['detalhes_compra.tipo_produto'] == 'livro':
                    # Inserir na tabela guias_royalty
                    query_guias_royalty = text(
                        settings.queries.insert_guias_royalty)
                    session.execute(query_guias_royalty, {
                        'venda_id': venda_id,
                        'data_geracao': row['data'],
                        'status': 'enviado',
                        'valor': row['detalhes_compra.valor_royalty']
                    })

                else:
                    query_guias_remessa = text(
                        settings.queries.insert_guias_remessa)
                    session.execute(query_guias_remessa, {
                        'venda_id': venda_id,
                        'cliente_id': row['cliente_id'],
                        'data_geracao': row['data'],
                        'status': 'pendente',
                        'data_prevista_entrega': datetime.now() + timedelta(days=15)
                    })

                # Inserir na tabela comissoes
                query_comissoes = text(settings.queries.insert_comissoes)
                session.execute(query_comissoes, {
                    'venda_id': venda_id,
                    'vendedor_id': row['vendedor_id'],
                    'data_pagamento': row['data'],
                    'valor': row['detalhes_compra.preco'],
                    'status': 'fechado'
                })

                session.commit()

            return venda_id

        except SQLAlchemyError as e:
            session.rollback()
            raise Exception(f"Erro ao processar compra: {e}")

        finally:
            await self.db_connection.close()
