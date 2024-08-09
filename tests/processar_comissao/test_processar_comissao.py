import pytest
import pandas as pd
from redis import Redis
from sqlalchemy.exc import SQLAlchemyError
from unittest.mock import AsyncMock, MagicMock, patch
from processar_comissao.app import CalculoComissaoVendas


class TestesCalculoComissoes:

    @pytest.fixture
    def mock_db_connection(self):
        with patch('processar_comissao.app.PostgreSQLConnection') as MockDBConnection:
            instance = MockDBConnection.return_value
            instance.connect = AsyncMock()
            instance.close = AsyncMock()
            instance.session = MagicMock()
            yield instance

    @pytest.fixture
    def comissao(self):
        return CalculoComissaoVendas()

    @pytest.fixture
    def entrada_dataframe(self):
        data = {
            'ano': [2024],
            'mes': [7],
            'vendedor_id': [1]
        }
        return pd.DataFrame(data)

    @pytest.fixture
    def dicionario_de_saida(self):
        return [
            {'id': 1, 'nome_vendedor': 'João', 'total_vendas': 100,
                'total_vendas_valor': 1000.0, 'total_recebimentos': 100.0}
        ]

    @pytest.mark.asyncio
    async def test_comissao_vendedores_sucesso(self, comissao, entrada_dataframe, dicionario_de_saida, mock_db_connection):

        with patch.object(comissao.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(comissao.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(comissao.db_connection, 'executa_busca_retorna_df', new_callable=AsyncMock) as mock_executa_busca:
            mock_executa_busca.return_value = pd.DataFrame(dicionario_de_saida)
            resultado = await comissao.comissao_vendedores(entrada_dataframe)
        assert resultado == dicionario_de_saida
        mock_executa_busca.assert_called_once()

    @pytest.mark.asyncio
    async def test_comissao_vendedores_Null(self, comissao, entrada_dataframe, mock_db_connection):

        with patch.object(comissao.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(comissao.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(comissao.db_connection, 'executa_busca_retorna_df', new_callable=AsyncMock) as mock_executa_busca:
            mock_executa_busca.return_value = pd.DataFrame()
            resultado = await comissao.comissao_vendedores(entrada_dataframe)
        assert resultado == None
        mock_executa_busca.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message(self, comissao, mocker):
        # Criar uma mensagem de teste
        message = ('stream_app1_app5', [
            (b'1234567890', {
             b'data': b'{"ano": 2022, "mes": 12, "vendedor_id": 1}'})
        ])

        mocker.patch.object(comissao, 'comissao_vendedores', return_value=[
                            {'vendedor_id': 1, 'comissao': 100.0}])

        mocker.patch.object(comissao.r, 'xadd')
        await comissao.process_message(message)
        comissao.comissao_vendedores.assert_called_once()
        comissao.r.xadd.assert_called_once_with('stream_app5_app1', {
            'status': 'true', 'vendedores': '[{"vendedor_id": 1, "comissao": 100.0}]'})

    @ pytest.mark.asyncio
    async def test_process_message_error(self, comissao, mocker):
        # Criar uma mensagem de teste
        message = ('stream_app1_app5', [
            (b'1234567890', {
                b'data': b'{"ano": 2022, "mes": 12, "vendedor_id": 1}'})
        ])
        mocker.patch.object(comissao, 'comissao_vendedores', return_value=None)
        mocker.patch.object(comissao.r, 'xadd')
        await comissao.process_message(message)
        comissao.comissao_vendedores.assert_called_once()
        comissao.r.xadd.assert_called_once_with(
            'stream_app5_app1', {'status': 'false'})

    @ pytest.mark.asyncio
    async def test_comissao_vendedores_erro_sqlalchemy(self, comissao, entrada_dataframe, mock_db_connection):
        with patch.object(comissao.db_connection, 'connect', new_callable=AsyncMock) as mock_connect, \
                patch.object(comissao.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(comissao.db_connection, 'executa_busca_retorna_df', new_callable=AsyncMock) as mock_executa_busca, \
                patch('processar_comissao.app.logger') as mock_logger:

            mock_executa_busca.side_effect = SQLAlchemyError(
                "Erro na consulta")
            mock_connect.return_value.__aenter__.return_value = mock_db_connection
            resultado = await comissao.comissao_vendedores(entrada_dataframe)
            assert resultado is None
            mock_executa_busca.assert_called_once()
            mock_logger.error.assert_called_once_with(
                "Erro ao calcular comissões: Erro na consulta")
