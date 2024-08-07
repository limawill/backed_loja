import pytest
import pandas as pd
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
            {
                'id': 1,
                'nome_vendedor': 'João',
                'total_vendas': 100,
                'total_vendas_valor': 1000.0,
                'total_recebimentos': 100.0
            }
        ]

    def test_comissao_vendedores_sucesso(self, entrada_dataframe, dicionario_de_saida, mock_db_connection):
        # Arrange
        comissao = CalculoComissaoVendas()
        comissao.db_connection = mock_db_connection

        # Configurar o mock do resultado da execução da consulta
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [
            (1, 'João', 100, 1000.0, 100.0)
        ]
        mock_result.keys.return_value = [
            'id', 'nome_vendedor', 'total_vendas', 'total_vendas_valor', 'total_recebimentos'
        ]

        # Configurar o mock de session.execute
        mock_db_connection.session.execute = AsyncMock(
            return_value=mock_result)

        # Act
        resultado = await comissao.comissao_vendedores(entrada_dataframe)

        # Assert
        assert resultado == dicionario_de_saida
        assert mock_db_connection.connect.called
        assert mock_db_connection.close.called
