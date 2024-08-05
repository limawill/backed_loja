import json
import pandas as pd
from config import settings, logger


class JsonConverter:
    """
    Classe para converter DataFrames em JSON e adicionar campos faltantes aos DataFrames.
    """

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
