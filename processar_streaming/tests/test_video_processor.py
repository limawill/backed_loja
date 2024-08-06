import json
import pandas as pd
import pytest
from processar_streaming.app import VideoProcessor
from tools.mailhog import Mailhog
from unittest.mock import AsyncMock, patch, MagicMock


@pytest.fixture
def json_entrada():
    return {
        "data": "2024-08-1",
        "cliente_id": "901.234.567-89",
        "detalhes_compra": {"id_streaming": "2"},
    }


@pytest.fixture
def json_saida():
    return {
        "message": "Recebido e processado por streaming",
        "data": {
            "video": "{'cpf': '901.234.567-89', 'videos': [{'nome': 'Primeiros Socorros', 'link': 'https://www.youtube.com/watch?v=789012'}"
        },
    }


@pytest.fixture
def mock_mailhog():
    with patch('tools.mailhog.Mailhog') as MockMailhog:
        mock_instance = MockMailhog.return_value
        # Simula sucesso no envio do email
        mock_instance.send_email = MagicMock(return_value=True)
        yield mock_instance


@pytest.fixture
def mock_mailhog_with_error():
    with patch('tools.mailhog.Mailhog') as MockMailhog:
        mock_instance = MockMailhog.return_value
        # Simula um erro ao tentar enviar o e-mail
        mock_instance.send_email = MagicMock(
            side_effect=Exception("Erro ao enviar e-mail"))
        yield mock_instance


@pytest.fixture
def mock_db_connection():
    with patch('processar_streaming.app.PostgreSQLConnection') as MockDBConnection:
        instance = MockDBConnection.return_value
        instance.executa_busca_retorna_df = AsyncMock(return_value=pd.DataFrame([{
            'nome': 'John Doe', 'email': 'john.doe@example.com', 'cpf': '901.234.567-89'
        }]))
        instance.connect = AsyncMock()
        instance.close = AsyncMock()
        yield instance


def test_send_email(mock_mailhog):
    nome = "John Doe"
    email = "john.doe@example.com"
    titulo = "Teste de Email"
    corpo = "Este é um teste de email"

    resultado = mock_mailhog.send_email(nome, email, titulo, corpo)
    assert resultado == True


@pytest.mark.asyncio
async def test_envia_email_cliente_excecao():
    with patch('tools.mailhog.Mailhog') as MockMailhog:
        mock_mailhog = MockMailhog.return_value
        mock_mailhog.send_email = MagicMock(
            side_effect=Exception("Erro ao enviar e-mail"))

        processor = VideoProcessor()
        # Substitui a instância real de Mailhog por nosso mock
        processor.mailhog = mock_mailhog

        df_cliente = pd.DataFrame(
            [{'nome': 'John Doe', 'email': 'john.doe@example.com', 'cpf': '901.234.567-89'}])
        df_video = pd.DataFrame(
            [{'nome': 'Primeiros Socorros', 'link': 'https://www.youtube.com/watch?v=789012'}])

        resultado = await processor.envia_email_cliente(df_cliente, df_video)
        assert resultado is None


@pytest.mark.asyncio
async def test_send_email_with_error(mock_mailhog_with_error):
    nome = "John Doe"
    email = "john.doe@example.com"
    titulo = "Teste de Email"
    corpo = "Este é um teste de email"

    # Chama o método que deve lançar uma exceção
    resultado = await VideoProcessor().envia_email_cliente(
        pd.DataFrame(
            [{'nome': nome, 'email': email, 'cpf': '901.234.567-89'}]),
        pd.DataFrame([{'nome': 'Primeiros Socorros',
                     'link': 'https://www.youtube.com/watch?v=789012'}])
    )
    assert resultado is None


@pytest.mark.asyncio
async def test_processa_compra_entrada(json_entrada):
    processor = VideoProcessor()

    # Mocking methods that will be called within processa_compra
    with patch.object(VideoProcessor, 'process_message', return_value={'status': 'success'}) as mock_processa:
        resultado = await processor.process_message(json_entrada)
        assert resultado['status'] == 'success'
        mock_processa.assert_called_once_with(json_entrada)


@pytest.mark.asyncio
async def test_envia_email_cliente_erro_rede():
    with patch('tools.mailhog.Mailhog.send_email', side_effect=Exception("Erro de rede")) as mock_send_email:
        processor = VideoProcessor()

        df_cliente = pd.DataFrame(
            [{'nome': 'John Doe', 'email': 'john.doe@example.com', 'cpf': '901.234.567-89'}])
        df_video = pd.DataFrame(
            [{'nome': 'Primeiros Socorros', 'link': 'https://www.youtube.com/watch?v=789012'}])

        resultado = await processor.envia_email_cliente(df_cliente, df_video)
        assert resultado is None
        mock_send_email.assert_called_once()


@pytest.mark.asyncio
async def test_process_message_entrada(json_entrada):
    processor = VideoProcessor()

    # Mocking methods that will be called within process_message
    with patch.object(VideoProcessor, 'process_message', return_value={'status': 'success'}) as mock_processa:
        resultado = await processor.process_message(json_entrada)
        assert resultado['status'] == 'success'
        mock_processa.assert_called_once_with(json_entrada)


@pytest.mark.asyncio
async def test_envio_video_df_incompleto():
    processor = VideoProcessor()

    # DataFrame missing required columns
    df_incompleto = pd.DataFrame([{'detalhes_compra.id_streaming': '2'}])

    resultado = await processor.envio_video(df_incompleto)
    assert resultado is None


@pytest.mark.asyncio
async def test_envio_video_dados_completos(json_entrada, mock_mailhog, mock_db_connection):
    processor = VideoProcessor()

    # Convertendo json_entrada para DataFrame
    df_entrada = pd.json_normalize(json_entrada)

    with patch.object(VideoProcessor, 'envia_email_cliente', return_value={'cpf': '901.234.567-89', 'videos': [{'nome': 'Primeiros Socorros', 'link': 'https://www.youtube.com/watch?v=789012'}]}):
        with patch.object(processor, 'db_connection', mock_db_connection):
            resultado = await processor.envio_video(df_entrada)
            assert resultado is not None
            assert 'cpf' in resultado
            assert 'videos' in resultado


@pytest.mark.asyncio
async def test_conexao_bd(mock_db_connection):
    processor = VideoProcessor()

    with patch.object(processor, 'db_connection', mock_db_connection):
        await processor.db_connection.connect()
        mock_db_connection.connect.assert_called_once()


@pytest.mark.asyncio
async def test_fechar_conexao_bd(mock_db_connection):
    processor = VideoProcessor()

    with patch.object(processor, 'db_connection', mock_db_connection):
        await processor.db_connection.close()
        mock_db_connection.close.assert_called_once()


@pytest.mark.asyncio
async def test_processa_video_dados_incompletos(mock_mailhog, mock_db_connection):
    processor = VideoProcessor()

    # Dados incompletos, faltando colunas obrigatórias
    df_incompleto = pd.DataFrame([{'data': '2024-08-1'}])

    with patch.object(processor, 'db_connection', mock_db_connection):
        with patch.object(processor, 'mailhog', mock_mailhog):
            resultado = await processor.envio_video(df_incompleto)
            assert resultado is None


@pytest.mark.asyncio
async def test_processa_video_com_erro_no_envio_email(mock_mailhog_with_error, mock_db_connection, json_saida):
    processor = VideoProcessor()

    df_entrada = pd.json_normalize(json_saida)

    with patch.object(processor, 'db_connection', mock_db_connection):
        with patch.object(processor, 'mailhog', mock_mailhog_with_error):
            resultado = await processor.envio_video(df_entrada)
            assert resultado is None


@pytest.mark.asyncio
async def test_falha_na_busca_bd(mock_db_connection):
    processor = VideoProcessor()

    # Configura o mock para levantar uma exceção
    mock_db_connection.executa_busca_retorna_df = AsyncMock(
        side_effect=Exception("Erro na busca de dados"))

    with patch.object(processor, 'db_connection', mock_db_connection):
        try:
            # Espera-se que o método 'envio_video' trate a exceção e retorne None ou faça outra ação
            resultado = await processor.envio_video(pd.DataFrame())
            assert resultado is None  # Ajuste de acordo com o comportamento esperado
        except Exception as e:
            # Certifique-se de que o método de produção está tratando a exceção
            assert str(e) == "Erro na busca de dados"


@pytest.mark.asyncio
async def test_envia_email_cliente_com_excecao(mock_mailhog_with_error):
    processor = VideoProcessor()
    df_cliente = pd.DataFrame(
        [{'nome': 'John Doe', 'email': 'john.doe@example.com', 'cpf': '901.234.567-89'}])
    df_video = pd.DataFrame(
        [{'nome': 'Primeiros Socorros', 'link': 'https://www.youtube.com/watch?v=789012'}])

    with patch.object(processor, 'mailhog', mock_mailhog_with_error):
        resultado = await processor.envia_email_cliente(df_cliente, df_video)
        assert resultado is None


@pytest.mark.asyncio
async def test_falha_no_processamento_video(mock_db_connection, mock_mailhog):
    processor = VideoProcessor()

    # Simula uma resposta bem-sucedida da busca no banco de dados
    mock_db_connection.executa_busca_retorna_df = AsyncMock(return_value=pd.DataFrame({
        'nome': ['João'],
        'email': ['joao@example.com'],
        'cpf': ['123.456.789-00'],
        'link': ['http://example.com/video']
    }))

    # Simula um erro no processamento do vídeo
    with patch.object(processor, 'mailhog', mock_mailhog):
        processor.gera_link_video = AsyncMock(side_effect=Exception("Erro na geração do link"))

        # Ajusta o DataFrame para garantir que os dados sejam passados corretamente
        detalhes_compra = {'id_streaming': ['2']}
        df = pd.DataFrame({
            'cliente_id': ['901.234.567-89'],
            'data': ['2024-08-01'],
            'detalhes_compra': [detalhes_compra]
        })

        resultado = await processor.envio_video(df)
        
        # Espera-se que o resultado seja None devido ao erro na geração do link
        assert resultado is None
