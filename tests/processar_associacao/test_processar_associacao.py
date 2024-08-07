import json
import pytest
import asyncio
import pandas as pd
from tools.mailhog import Mailhog
from unittest.mock import AsyncMock, patch, MagicMock
from processar_associacao.app import AssocProcess


class TestAssocProcess:
    @pytest.fixture
    def mock_mailhog(self):
        with patch('tools.mailhog.Mailhog') as MockMailhog:
            mock_instance = MockMailhog.return_value
            # Simula sucesso no envio do email
            mock_instance.send_email = MagicMock(return_value=True)
            yield mock_instance

    @pytest.fixture
    def mock_mailhog_with_error(self):
        with patch('tools.mailhog.Mailhog') as MockMailhog:
            mock_instance = MockMailhog.return_value
            mock_instance.send_email = MagicMock(
                side_effect=Exception("Erro ao enviar e-mail"))
            yield mock_instance

    @pytest.fixture
    def df_entrada(self):
        return pd.DataFrame([{
            'nome': 'John Doe',
            'email': 'john.doe@example.com'
        }])

    @pytest.fixture
    def mock_db_connection_no_data(self):
        with patch('processar_associacao.app.PostgreSQLConnection') as MockDBConnection:
            instance = MockDBConnection.return_value
            instance.executa_busca_retorna_df = AsyncMock(
                return_value=pd.DataFrame())
            instance.executa_insercao = AsyncMock()  # Adiciona o AsyncMock
            instance.connect = AsyncMock()
            instance.close = AsyncMock()
            yield instance

    @pytest.fixture
    def mock_db_connection(self):
        with patch('processar_streaming.app.PostgreSQLConnection') as MockDBConnection:
            instance = MockDBConnection.return_value
            instance.executa_busca_retorna_df = AsyncMock(return_value=pd.DataFrame([{
                'nome': 'John Doe', 'email': 'john.doe@example.com', 'cpf': '901.234.567-89'
            }]))
            instance.connect = AsyncMock()
            instance.close = AsyncMock()
            yield instance

    @pytest.fixture
    def mock_db_connection_with_error(self):
        with patch('processar_associacao.app.PostgreSQLConnection') as MockDBConnection:
            instance = MockDBConnection.return_value
            instance.executa_insercao = AsyncMock(
                side_effect=Exception("Erro na inserção"))
            instance.connect = AsyncMock()
            instance.close = AsyncMock()
            yield instance

    @pytest.fixture
    def associacao_json(self):
        return [
            {
                "data": "2024-08-1",
                "cliente_id": "456.789.012-34",
                "vendedor_id": "1",
                "tipo_assinatura": "nova_associacao",
                "detalhes_compra": {"nome_plano": "Básico", "ativo": "True"},
            }
        ]

    @pytest.fixture
    def upgrade_json(self):
        return [
            {
                "data": "2024-08-1",
                "cliente_id": "456.789.012-34",
                "vendedor_id": "1",
                "tipo_assinatura": "upgrade_associacao",
                "detalhes_compra": {"nome_plano": "Plus", "ativo": "True"},
            }
        ]

    @pytest.fixture
    def ativacao_json(self):
        return [
            {
                "data": "2024-08-1",
                "cliente_id": "901.234.567-89",
                "vendedor_id": "1",
                "tipo_assinatura": "ativacao_associacao",
                "detalhes_compra": {"nome_plano": "Plus", "ativo": "True"},
            }
        ]

    @pytest.fixture
    def df_associacao_json(self):
        data = [
            {
                "data": "2024-08-1",
                "cliente_id": "456.789.012-34",
                "vendedor_id": "1",
                "tipo_assinatura": "nova_associacao",
                "detalhes_compra": {"nome_plano": "Básico", "ativo": "True"},
                "nome": "Theresa Parks",
                "email": "towsijo@toc.bf"
            }
        ]
        return pd.DataFrame(data)

    @pytest.mark.asyncio
    async def test_conexao_bd(self, mock_db_connection):
        processor = AssocProcess()

        with patch.object(processor, 'db_connection', mock_db_connection):
            await processor.db_connection.connect()
            mock_db_connection.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_fechar_conexao_bd(self, mock_db_connection):
        processor = AssocProcess()
        with patch.object(processor, 'db_connection', mock_db_connection):
            await processor.db_connection.close()
            mock_db_connection.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_email(self, mock_mailhog):
        nome = "Lois Owen"
        email = "obisapiwo@cil.mx"
        titulo = "Teste de Email"
        corpo = "Este é um teste de email"

        resultado = mock_mailhog.send_email(nome, email, titulo, corpo)
        assert resultado == True

    @pytest.mark.asyncio
    async def test_envia_email_cliente_falha(self, mock_mailhog_with_error):
        nome = "Sue Kim"
        email = "kunit@kemoni.aw"
        tipo_servico = "Assinatura"
        df_entrada = pd.DataFrame([{'nome': nome, 'email': email}])
        processor = AssocProcess()
        with patch('tools.mailhog.Mailhog.send_email', return_value=False):
            result = await processor.envia_email_cliente(df_entrada, tipo_servico)
            assert result is False

    @pytest.mark.asyncio
    async def test_envia_email_cliente_excecao(self, mock_mailhog_with_error, caplog):
        nome = "Sue Kim"
        email = "kunit@kemoni.aw"
        tipo_servico = "Assinatura"

        # Prepare o DataFrame de entrada com os dados do cliente
        df_entrada = pd.DataFrame([{'nome': nome, 'email': email}])

        processor = AssocProcess()

        # Mock send_email to raise an exception
        with patch.object(processor.mailhog, 'send_email', side_effect=Exception("Erro simulado ao enviar e-mail")):
            result = await processor.envia_email_cliente(df_entrada, tipo_servico)
            assert result is False
            assert "Erro ao enviar email: Erro simulado ao enviar e-mail" in caplog.text

    @pytest.mark.asyncio
    async def test_processar_associacao_falha(self, mock_db_connection_with_error, associacao_json):
        processor = AssocProcess()
        df_associacao = pd.json_normalize(associacao_json)
        with patch.object(processor, 'db_connection', mock_db_connection_with_error):
            with pytest.raises(Exception, match="Erro na inserção"):
                await processor.processar_associacao(df_associacao)

    @pytest.mark.asyncio
    async def test_processar_associacao_erro_busca(self, mock_db_connection_with_error, associacao_json):
        processor = AssocProcess()
        df_associacao = pd.json_normalize(associacao_json)
        with patch.object(processor, 'db_connection', mock_db_connection_with_error):
            with pytest.raises(Exception, match="Erro na inserção"):
                await processor.processar_associacao(df_associacao)

    @pytest.mark.asyncio
    async def test_processar_associacao_coluna_faltando(self, mock_db_connection, associacao_json):
        processor = AssocProcess()
        df_associacao = pd.DataFrame([{
            "data": "2024-08-1",
            "cliente_id": "456.789.012-34",
            "tipo_assinatura": "nova_associacao",
            "detalhes_compra": {"nome_plano": "Básico", "ativo": "True"},
        }])
        with patch.object(processor, 'db_connection', mock_db_connection):
            resultado = await processor.processar_associacao(df_associacao)
            assert resultado is False

    @pytest.mark.asyncio
    async def test_process_message_mensagem_desconhecida(self, mock_db_connection):
        json_dict = {
            "data": "2024-08-1",
            "cliente_id": "901.234.567-89",
            "vendedor_id": "1",
            "tipo_assinatura": "desconhecida",
            "detalhes_compra": {"nome_plano": "Plus"}
        }
        message = ('stream_app1_app2', [
                   (b'1', {b'data': json.dumps(json_dict).encode('utf-8')})])
        processor = AssocProcess()
        with patch.object(processor, 'db_connection', mock_db_connection):
            with patch.object(processor.r, 'xadd') as mock_xadd:
                await processor.process_message(message)
                mock_xadd.assert_called_once_with(
                    'stream_app2_app1', {'status': 'false'})

    @pytest.mark.asyncio
    async def test_upgrade_associacao_cliente_inativo(self, mock_db_connection, upgrade_json):
        df_upgrade = pd.DataFrame([{
            "data": "2024-08-1",
            "cliente_id": "456.789.012-34",
            "vendedor_id": "1",
            "tipo_assinatura": "upgrade_associacao",
            "detalhes_compra": {"nome_plano": "Plus"}
        }])
        processor = AssocProcess()
        with patch.object(processor, 'db_connection', mock_db_connection):
            resultado = await processor.upgrade_associacao(df_upgrade)
            assert resultado is False

    @pytest.mark.asyncio
    async def test_processar_associacao_coluna_obrigatoria_faltando(self, mock_db_connection, associacao_json):
        df_associacao = pd.DataFrame([{
            "data": "2024-08-1",
            "cliente_id": "456.789.012-34",
            "vendedor_id": "1",
            "tipo_assinatura": "nova_associacao"
            # "detalhes_compra" está faltando
        }])
        processor = AssocProcess()
        with patch.object(processor, 'db_connection', mock_db_connection):
            resultado = await processor.processar_associacao(df_associacao)
            assert resultado is False

    @pytest.mark.asyncio
    async def test_process_message_associacao(self, mock_db_connection, associacao_json):
        json_dict = associacao_json[0]
        message = ('stream_app1_app2', [
                   (b'1', {b'data': json.dumps(json_dict).encode('utf-8')})])
        processor = AssocProcess()

        # Mock do Redis
        with patch.object(processor, 'r') as mock_redis:
            mock_redis.xread.return_value = [('stream_app1_app2', [(
                b'1', {b'data': b'{"tipo_assinatura": "nova_associacao"}'})])]
            mock_redis.xadd = AsyncMock()

            # Mock do método processar_associacao
            with patch.object(processor, 'processar_associacao', new_callable=AsyncMock) as mock_processar_associacao:
                await processor.process_message(message)
                mock_processar_associacao.assert_called_once()

    @pytest.mark.asyncio
    async def test_envia_email_cliente_upgrade(self, mock_mailhog, df_associacao_json):
        processor = AssocProcess()
        tipo_servico = 'Upgrade'
        with patch.object(processor.mailhog, 'send_email', AsyncMock(return_value=True)) as mock_send_email:
            result = await processor.envia_email_cliente(df_associacao_json, tipo_servico)
            assert result is True
            mock_send_email.assert_called_once()

    @pytest.mark.asyncio
    async def test_processar_associacao_falha_insercao(self, mock_db_connection_with_error, df_associacao_json):
        processor = AssocProcess()
        with patch.object(processor, 'db_connection', mock_db_connection_with_error):
            result = await processor.processar_associacao(df_associacao_json)

        assert result is False

    @pytest.mark.asyncio
    async def test_upgrade_associacao_cliente_inativo(self, mock_db_connection):
        df_upgrade = pd.DataFrame([{
            "data": "2024-08-1",
            "cliente_id": "456.789.012-34",
            "vendedor_id": "1",
            "tipo_assinatura": "upgrade_associacao",
            "detalhes_compra": {"nome_plano": "Plus", "ativo": "False"}
        }])
        processor = AssocProcess()
        with patch.object(processor, 'db_connection', mock_db_connection):
            resultado = await processor.upgrade_associacao(df_upgrade)
            assert resultado is False

    @pytest.mark.asyncio
    async def test_processar_associacao_sem_dados(self, mock_db_connection_no_data, df_associacao_json):
        processor = AssocProcess()
        with patch.object(processor, 'db_connection', mock_db_connection_no_data):
            resultado = await processor.processar_associacao(df_associacao_json)
            assert resultado is False, f"Resultado esperado False, mas recebeu {
                resultado}"

    def test_assoc_process_initialization(self):
        processor = AssocProcess()
        assert processor.db_connection is not None
        assert processor.mailhog is not None
        assert processor.r is not None

    @pytest.mark.asyncio
    async def test_upgrade_associacao_dados_invalidos(self, mock_db_connection):
        df_upgrade = pd.DataFrame([{
            "data": "2024-08-1",
            "cliente_id": "456.789.012-34",
            "vendedor_id": "1",
            "tipo_assinatura": "upgrade_associacao",
            "detalhes_compra": {"nome_plano": "Plus"}
        }])  # 'ativo' missing
        processor = AssocProcess()
        with patch.object(processor, 'db_connection', mock_db_connection):
            resultado = await processor.upgrade_associacao(df_upgrade)
            assert resultado is False
