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
        df = pd.json_normalize(dict_data)
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

    @pytest.mark.asyncio
    async def test_insere_comissao_sucesso(self, compra_fisica, df_entrada_livro):

        with patch.object(compra_fisica.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'executa_insercao_retorna_id', new_callable=AsyncMock) as mock_executa_insert:
            mock_executa_insert.return_value = 7
            resultado = await compra_fisica.insere_venda_comissao(df_entrada_livro, 7)
        assert resultado == 7
        mock_executa_insert.assert_called_once()

    @pytest.mark.asyncio
    async def test_insere_comissao_none(self, compra_fisica, df_entrada_livro):

        with patch.object(compra_fisica.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'executa_insercao_retorna_id', new_callable=AsyncMock) as mock_executa_insert:
            mock_executa_insert.return_value = None
            resultado = await compra_fisica.insere_venda_comissao(df_entrada_livro, 7)
        assert resultado is None
        mock_executa_insert.assert_called_once()

    @pytest.mark.asyncio
    async def test_insere_royalty_remessa_livro_sucesso(self, compra_fisica, df_entrada_livro):

        with patch.object(compra_fisica.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'executa_insercao_retorna_id', new_callable=AsyncMock) as mock_executa_insert:
            mock_executa_insert.return_value = 7
            resultado = await compra_fisica.insere_venda_royalty_remessa(df_entrada_livro, 7)
        assert resultado == 7
        mock_executa_insert.assert_called_once()

    @pytest.mark.asyncio
    async def test_insere_royalty_remessa_livro_none(self, compra_fisica, df_entrada_livro):

        with patch.object(compra_fisica.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'executa_insercao_retorna_id', new_callable=AsyncMock) as mock_executa_insert:
            mock_executa_insert.return_value = None
            resultado = await compra_fisica.insere_venda_royalty_remessa(df_entrada_livro, 7)
        assert resultado is None
        mock_executa_insert.assert_called_once()

    @pytest.mark.asyncio
    async def test_insere_royalty_remessa_produto_sucesso(self, compra_fisica, df_entrada_produto):
        with patch.object(compra_fisica.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'executa_insercao_retorna_id', new_callable=AsyncMock) as mock_executa_insert:
            mock_executa_insert.return_value = 7
            resultado = await compra_fisica.insere_venda_royalty_remessa(df_entrada_produto, 7)
        assert resultado == 7
        mock_executa_insert.assert_called_once()

    @pytest.mark.asyncio
    async def test_insere_royalty_remessa_produto_none(self, compra_fisica, df_entrada_produto):
        with patch.object(compra_fisica.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(compra_fisica.db_connection, 'executa_insercao_retorna_id', new_callable=AsyncMock) as mock_executa_insert:
            mock_executa_insert.return_value = None
            resultado = await compra_fisica.insere_venda_royalty_remessa(df_entrada_produto, 7)
        assert resultado is None
        mock_executa_insert.assert_called_once()

    @pytest.mark.asyncio
    async def test_insere_dados_banco_sucesso(self, compra_fisica):
        db_connection_mock = AsyncMock()
        db_connection_mock.executa_insercao_retorna_id.return_value = 1
        session_mock = AsyncMock()
        db_connection_mock.session = session_mock
        compra_fisica.db_connection = db_connection_mock
        df = pd.DataFrame({'coluna1': [1], 'coluna2': [2]})
        queries = "INSERT INTO tabela (coluna1, coluna2) VALUES (:coluna1, :coluna2)"
        parameters = {'coluna1': 1, 'coluna2': 2}
        resultado = await compra_fisica.insere_dados_banco(df, queries, parameters)
        assert resultado == 1
        db_connection_mock.executa_insercao_retorna_id.assert_called_once_with(
            session_mock, queries, df, parameters
        )

    @pytest.mark.asyncio
    async def test_insere_dados_banco_none(self, compra_fisica):
        db_connection_mock = AsyncMock()
        db_connection_mock.executa_insercao_retorna_id.return_value = None
        session_mock = AsyncMock()
        db_connection_mock.session = session_mock
        compra_fisica.db_connection = db_connection_mock
        df = pd.DataFrame({'coluna1': [1], 'coluna2': [2]})
        queries = "INSERT INTO tabela (coluna1, coluna2) VALUES (:coluna1, :coluna2)"
        parameters = {'coluna1': 1, 'coluna2': 2}
        resultado = await compra_fisica.insere_dados_banco(df, queries, parameters)
        assert resultado is None
        db_connection_mock.executa_insercao_retorna_id.assert_called_once_with(
            session_mock, queries, df, parameters
        )

    @pytest.mark.asyncio
    async def test_insere_dados_banco_excecao(self, compra_fisica):
        db_connection_mock = AsyncMock()
        db_connection_mock.executa_insercao_retorna_id.side_effect = Exception(
            "Erro ao inserir no banco de dados")
        session_mock = AsyncMock()
        db_connection_mock.session = session_mock
        compra_fisica.db_connection = db_connection_mock
        df = pd.DataFrame({'coluna1': [1], 'coluna2': [2]})
        queries = "INSERT INTO tabela (coluna1, coluna2) VALUES (:coluna1, :coluna2)"
        parameters = {'coluna1': 1, 'coluna2': 2}
        with pytest.raises(Exception) as exc_info:
            await compra_fisica.insere_dados_banco(df, queries, parameters)
        assert str(exc_info.value) == "Erro ao inserir no banco de dados"
        db_connection_mock.executa_insercao_retorna_id.assert_called_once_with(
            session_mock, queries, df, parameters
        )

    @pytest.mark.asyncio
    async def test_process_message_sucesso(self, compra_fisica):
        # Mockando os métodos insere_venda_livro, insere_venda_comissao e insere_venda_royalty_remessa
        compra_fisica.insere_venda_livro = AsyncMock(return_value=1)
        compra_fisica.insere_venda_comissao = AsyncMock(return_value=1)
        compra_fisica.insere_venda_royalty_remessa = AsyncMock(return_value=1)

        # Mockando o Redis
        redis_mock = MagicMock()
        compra_fisica.r = redis_mock

        message_data = {
            b'data': b'{"coluna1": 1, "coluna2": 2}'
        }
        message = ('stream_app1_app3', [(b'msg_id_1', message_data)])
        await compra_fisica.process_message(message)
        compra_fisica.insere_venda_livro.assert_called_once()
        compra_fisica.insere_venda_comissao.assert_called_once()

        compra_fisica.insere_venda_royalty_remessa.assert_called_once()
        redis_mock.xadd.assert_called_once_with(
            'stream_app3_app1', {'status': 'true', 'venda_id': '1'}
        )
        assert compra_fisica.last_id == b'msg_id_1'
