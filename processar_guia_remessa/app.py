import json
import redis
import asyncio
import numpy as np
import pandas as pd
from sqlalchemy import text
from typing import Optional, Dict
from config import settings, logger
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError
from tools.db_connection import PostgreSQLConnection


class GerarGuiaRemessa:
    """
    Classe para gerar guias de remessa com base em dados fornecidos e enviar resultados através de Redis.
    """

    def __init__(self):
        """
        Inicializa o objeto GerarGuiaRemessa, configurando a conexão Redis e PostgreSQL.
        """
        self.r = redis.Redis(host=settings.redis.host,
                             port=settings.redis.port)
        self.last_id = '0-0'
        self.db_connection = PostgreSQLConnection()

    def convert_to_json(self, df):
        """
        Converte um DataFrame em um JSON formatado.

        Args:
            df (pd.DataFrame): DataFrame contendo os dados da guia de remessa.

        Returns:
            str: JSON formatado contendo os dados da guia de remessa.
        """
        data = {}

        logger.info("Preencha os campos da guia")
        data['numero_guia'] = str(df['numero_guia'].values[0])
        data['data_emissao'] = str(df['data_emissao'].values[0])
        data['Departamento de Royalty'] = str(
            df['Departamento de Royalty'].values[0])
        data['remetente'] = {
            'nome': df['remetente_nome'].values[0],
            'endereco': df['remetente_endereco'].values[0],
            'telefone': df['remetente_telefone'].values[0],
            'cnpj': df['remetente_cnpj'].values[0]
        }
        data['destinatario'] = {
            'nome': df['destinatario_nome'].values[0],
            'endereco': df['destinatario_endereco'].values[0],
            'telefone': df['destinatario_telefone'].values[0],
            'cnpj/cpf': df['destinatario_cnpj'].values[0]
        }
        data['produtos'] = [{
            'codigo': int(df['produto_codigo'].values[0]),
            'descricao': df['produto_descricao'].values[0],
            'tipo': df['produto_tipo'].values[0],
            'quantidade': int(df['produto_quantidade'].values[0]),
            'valor_unitario': float(df['produto_valor_unitario'].values[0]),
            'valor_total': float(df['produto_valor_total'].values[0])
        }]
        data['peso_total'] = float(df['remetente_peso'].values[0])
        data['volume'] = int(df['remetente_volume'].values[0])
        data['transportadora'] = df['remetente_transpor'].values[0]
        data['condicoes_pagamento'] = df['condicoes_pagamento'].values[0]
        data['observacoes'] = df['remetente_observ'].values[0]

        logger.info("Converta o dicionário para JSON")
        json_data = json.dumps(data, indent=4)

        return json_data

    def adiciona_to_json(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona campos faltantes ao DataFrame com base nas configurações da empresa.

        Args:
            df (pd.DataFrame): DataFrame contendo os dados da guia de remessa.

        Returns:
            pd.DataFrame: DataFrame atualizado com os campos adicionados.
        """
        logger.info("Preencha os campos faltantes ...")
        df['remetente_nome'] = settings.empresa.Nome
        df['remetente_endereco'] = settings.empresa.Endereco
        df['remetente_cidade'] = settings.empresa.Cidade_estado
        df['remetente_telefone'] = settings.empresa.Telefone
        df['remetente_cnpj'] = settings.empresa.CNPJ
        df['remetente_peso'] = settings.empresa.Peso_total
        df['remetente_volume'] = settings.empresa.Volume
        df['remetente_transpor'] = settings.empresa.Transportadora
        df['remetente_observ'] = settings.empresa.Observacao_venda

        df['Departamento de Royalty'] = df['produto_tipo'].str.contains(
            'livro')
        logger.info(f"Valor de is_royalty: {df['Departamento de Royalty']}")

        return df

    async def gera_guira_remessa(self, df: pd.DataFrame) -> json:
        """
        Gera a guia de remessa com base nos dados fornecidos.

        Args:
            df (pd.DataFrame): DataFrame contendo os detalhes da venda.

        Returns:
            json: JSON contendo os dados da guia de remessa ou None se não houver dados.
        """
        await self.db_connection.connect()
        session = self.db_connection.session
        try:
            for _, row in df.iterrows():

                logger.info("Iniciando geração da guia de remessa e royalts")
                remessa_royalt = await self.db_connection.executa_busca_retorna_df(
                    session,
                    settings.queries.gera_guia_remessa,
                    df,
                    {
                        'codigo_venda': 'codigo_venda'
                    }
                )

            if not remessa_royalt.empty:
                logger.info("Gerando a guia ...")
                remessa_royalt = self.adiciona_to_json(remessa_royalt)
                json_data = self.convert_to_json(remessa_royalt)
                logger.info("Guia remessa criada!")
                return json_data
            else:
                return None

        except SQLAlchemyError as e:
            logger.error(f"Erro ao criar guia remessa: {e}")
            return None
        finally:
            await self.db_connection.close()

    async def process_message(self, message):
        """
        Processa uma mensagem recebida do stream Redis.

        Args:
            message: Mensagem recebida do stream Redis.
        """
        stream, message_data = message

        for msg_id, msg in message_data:
            json_data = msg[b'data'].decode('utf-8')
            json_dict = json.loads(json_data)
            df = pd.json_normalize(json_dict)
            df = df.astype({"codigo_venda": "int16"})
            remesa = await self.gera_guira_remessa(df)

            if remesa is not None:
                logger.info("A guia de remessa gerada com sucesso")
                self.r.xadd('stream_app6_app1', {
                    'status': 'true', 'remessa': remesa})
                logger.info("Confirmação enviada para app1.")
            else:
                logger.info("Erro ao calcular comissões.")
                self.r.xadd('stream_app6_app1', {'status': 'false'})
                logger.info("Confirmação enviada para app1.")
            self.last_id = msg_id

    async def main(self):
        """
        Método principal que lê mensagens do stream Redis e gera guias de remessa.
        """
        while True:
            messages = self.r.xread(
                {'stream_app1_app6': self.last_id}, block=1000)
            if messages:
                for message in messages:
                    await self.process_message(message)
            await asyncio.sleep(1)


if __name__ == '__main__':
    gerar_guia_remessa = GerarGuiaRemessa()
    asyncio.run(gerar_guia_remessa.main())
