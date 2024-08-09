import json
import pytest
import logging
import pandas as pd
from redis import Redis
from sqlalchemy.exc import SQLAlchemyError
from unittest.mock import AsyncMock, MagicMock, patch
from processar_guia_remessa.app import GerarGuiaRemessa


class TesteGerarGuiaRemessa:
    @pytest.fixture
    def guiaremessa(self):
        return GerarGuiaRemessa()

    @pytest.fixture
    def entrada_dataframe(self):
        data = {"codigo_venda": [1]}
        return pd.DataFrame(data)

    @pytest.fixture
    def convert_to_json_mock(self):
        return pd.DataFrame(
            {
                "numero_guia": ["GR-1"],
                "data_emissao": ["08/08/2024"],
                "Departamento de Royalty": [True],
                "remetente_nome": ["Nova Terra Comércio Ltda."],
                "remetente_endereco": [
                    "Rua Dr. José Maria Rodrigues, 123, Centro, CEP 13010-010"
                ],
                "remetente_telefone": ["(19) 3232-1111"],
                "remetente_cnpj": ["43.745.219/0001-55"],
                "destinatario_nome": ["Carlos Silva"],
                "destinatario_endereco": ["Rua A, 123, São Paulo/SP - 01010-000"],
                "destinatario_telefone": ["(11) 1234-5678"],
                "destinatario_cnpj": ["123.456.789-00"],
                "produto_codigo": [8],
                "produto_descricao": ["The Two Towers"],
                "produto_tipo": ["livro"],
                "produto_quantidade": [1],
                "produto_valor_unitario": [150.0],
                "produto_valor_total": [150.0],
                "remetente_peso": [20.0],
                "remetente_volume": [1],
                "remetente_transpor": ["Rápido Norte Transportes Ltda."],
                "condicoes_pagamento": ["PIX"],
                "remetente_observ": ["Fragil - Manusear com cuidado"],
            }
        )

    @pytest.fixture
    def dicionario_de_saida(self):
        return [
            {
                "message": "Recebido e processado por Remessa",
                "data": {
                    "remessa": {
                        "numero_guia": "GR-1",
                        "data_emissao": "08/08/2024",
                        "Departamento de Royalty": "True",
                        "remetente": {
                            "nome": "Nova Terra Comércio Ltda.",
                            "endereco": "Rua Dr. José Maria Rodrigues, 123, Centro, CEP 13010-010",
                            "telefone": "(19) 3232-1111",
                            "cnpj": "43.745.219/0001-55",
                        },
                        "destinatario": {
                            "nome": "Carlos Silva",
                            "endereco": "Rua A, 123, São Paulo/SP - 01010-000",
                            "telefone": "(11) 1234-5678",
                            "cnpj/cpf": "123.456.789-00",
                        },
                        "produtos": [
                            {
                                "codigo": 8,
                                "descricao": "The Two Towers",
                                "tipo": "livro",
                                "quantidade": 1,
                                "valor_unitario": 150.0,
                                "valor_total": 150.0,
                            }
                        ],
                        "peso_total": 20.0,
                        "volume": 1,
                        "transportadora": "Rápido Norte Transportes Ltda.",
                        "condicoes_pagamento": "PIX",
                        "observacoes": "Fragil - Manusear com cuidado",
                    }
                },
            }
        ]

    # Teste dos metodos

    def test_convert_to_json(self, guiaremessa, convert_to_json_mock):
        json_data = guiaremessa.convert_to_json(convert_to_json_mock)
        assert isinstance(json_data, str)
        assert json.loads(json_data)

        # verificando alguns campos
        json_data_dict = json.loads(json_data)
        assert json_data_dict['numero_guia'] == 'GR-1'
        assert json_data_dict['data_emissao'] == '08/08/2024'
        assert json_data_dict['remetente']['nome'] == 'Nova Terra Comércio Ltda.'
        assert json_data_dict['remetente']['endereco'] == 'Rua Dr. José Maria Rodrigues, 123, Centro, CEP 13010-010'
        assert json_data_dict['remetente']['telefone'] == '(19) 3232-1111'
        assert json_data_dict['remetente']['cnpj'] == '43.745.219/0001-55'
        assert json_data_dict['destinatario']['nome'] == 'Carlos Silva'
        assert json_data_dict['destinatario']['endereco'] == 'Rua A, 123, São Paulo/SP - 01010-000'
        assert json_data_dict['destinatario']['telefone'] == '(11) 1234-5678'
        assert json_data_dict['destinatario']['cnpj/cpf'] == '123.456.789-00'
        assert json_data_dict['produtos'][0]['codigo'] == 8

    def test_adiciona_to_json(self, guiaremessa):
        # Criar um DataFrame de exemplo
        df = pd.DataFrame({"produto_tipo": ["livro"]})
        df_updated = guiaremessa.adiciona_to_json(df)
        assert "remetente_nome" in df_updated.columns
        assert "remetente_endereco" in df_updated.columns
        assert "remetente_cidade" in df_updated.columns
        assert "remetente_telefone" in df_updated.columns
        assert "remetente_cnpj" in df_updated.columns
        assert "remetente_peso" in df_updated.columns
        assert "remetente_volume" in df_updated.columns
        assert "remetente_transpor" in df_updated.columns
        assert "remetente_observ" in df_updated.columns
        assert "Departamento de Royalty" in df_updated.columns

    @pytest.mark.asyncio
    async def test_gera_guia_sucesso(self, guiaremessa, entrada_dataframe, dicionario_de_saida):
        # Mock do banco de dados e métodos internos
        with patch.object(guiaremessa.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(guiaremessa.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(guiaremessa.db_connection, 'executa_busca_retorna_df', new_callable=AsyncMock) as mock_executa_busca, \
                patch.object(guiaremessa, 'adiciona_to_json', return_value=entrada_dataframe) as mock_adiciona_to_json, \
                patch.object(guiaremessa, 'convert_to_json', return_value=dicionario_de_saida) as mock_convert_to_json:

            # Simular o retorno do método executa_busca_retorna_df
            mock_executa_busca.return_value = entrada_dataframe

            # Executa o método que está sendo testado
            resultado = await guiaremessa.gera_guira_remessa(entrada_dataframe)

        # Asserções
        assert resultado == dicionario_de_saida
        mock_executa_busca.assert_called_once()
        mock_adiciona_to_json.assert_called_once_with(entrada_dataframe)
        mock_convert_to_json.assert_called_once_with(entrada_dataframe)

    @pytest.mark.asyncio
    async def test_gera_guia_remessa_vazio(self, guiaremessa):
        with patch.object(guiaremessa.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(guiaremessa.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(guiaremessa.db_connection, 'executa_busca_retorna_df', new_callable=AsyncMock) as mock_executa_busca:

            # Simular o retorno do método executa_busca_retorna_df com um DataFrame vazio
            mock_executa_busca.return_value = pd.DataFrame()

            # Act
            resultado = await guiaremessa.gera_guira_remessa(pd.DataFrame({"codigo_venda": [1]}))

        # Assert
        assert resultado is None
        mock_executa_busca.assert_called_once()

    @pytest.mark.asyncio
    async def test_gera_guia_remessa_excecao(self, guiaremessa):
        with patch.object(guiaremessa.db_connection, 'connect', new_callable=AsyncMock), \
                patch.object(guiaremessa.db_connection, 'close', new_callable=AsyncMock), \
                patch.object(guiaremessa.db_connection, 'executa_busca_retorna_df', new_callable=AsyncMock) as mock_executa_busca, \
                patch('processar_guia_remessa.app.logger') as mock_logger:

            mock_executa_busca.side_effect = SQLAlchemyError(
                "Erro de banco de dados simulado")

            resultado = await guiaremessa.gera_guira_remessa(pd.DataFrame({"codigo_venda": [1]}))

            assert resultado is None
            mock_executa_busca.assert_called_once()
            mock_logger.error.assert_called_once_with(
                "Erro ao criar guia remessa: Erro de banco de dados simulado")
