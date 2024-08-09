import json
import pytest
import pandas as pd
from redis import Redis
from config import settings
from sqlalchemy.exc import SQLAlchemyError
from unittest.mock import AsyncMock, MagicMock, patch
from produto_fisico.app import VendaProcessor


class TestesProdutosFisicos:

    @pytest.fixture
    def compra_fisica(self):
        return VendaProcessor()

    @pytest.fixture
    def df_entrada_livro(self):
        json_data = '''
        {
            "data": "2024-07-25",
            "cliente_id": "123.456.789-00",
            "vendedor_id": "1",
            "tipo_compra": "produto_fisico",
            "detalhes_compra": {
                "produto_id": "8",
                "tipo_produto": "livro",  
                "quantidade": 1,
                "preco": 150.00,
                "nome_produto": "The Two Towers",
                "tipo_pagamento": "PIX",
                "especificacoes": "null",  
                "garantia": 0,  
                "autor": "J.R.R. Tolkien",  
                "isbn": "978-0-618-00224-4",
                "valor_royalty" : "6%"   
                }
            }
        '''

        dict_data = json.loads(json_data)
        df = pd.json_normalize(dict_data)
        print(df)
        print(df.columns)
        return df

    @pytest.fixture
    def df_entrada_produto(self):
        json_data = '''
        {
            "data": "2024-07-29",
            "cliente_id": "901.234.567-89",
            "vendedor_id": "1",
            "tipo_compra": "produto_fisico",
            "detalhes_compra": {
                "produto_id": "1",
                "tipo_produto": "laptop",
                "quantidade": 1,
                "preco": 1500.00,
                "nome_produto": "laptop LeNovo",
                "tipo_pagamento": "Cartão",
                "especificacoes": "4gb memoria, placa de video, processador intel",
                "garantia": 24,
                "autor": "null",
                "isbn": "null",
                "valor_royalty" : "null"
            }
        }
        '''

        dict_data = json.loads(json_data)
        df = pd.DataFrame([dict_data])
        df = pd.json_normalize(df)
        return df

    @pytest.mark.asyncio
    async def test_insere_livro_sucesso(self, compra_fisica, df_entrada_livro):

        with patch.object(compra_fisica.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'executa_insercao_retorna_id', new_callable=AsyncMock) as mock_executa_insert:
            mock_executa_insert.return_value = 7
            resultado = await compra_fisica.insere_venda_livro(df_entrada_livro)
        assert resultado == 7
        mock_executa_insert.assert_called_once()

    @pytest.mark.asyncio
    async def test_insere_livro_sem_id(self, compra_fisica, df_entrada_livro):
        with patch.object(compra_fisica.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'executa_insercao_retorna_id', new_callable=AsyncMock) as mock_executa_insert:
            mock_executa_insert.return_value = None
            resultado = await compra_fisica.insere_venda_livro(df_entrada_livro)
            assert resultado is None
            mock_executa_insert.assert_called_once()
