import logging
import pandas as pd
from typing import Optional
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from tools.db_connection import db_connection
from produto_fisico.produto_fisico_processor import ProdutoFisicoProcessor
from associacao.processar_associacao import AssocicacaoProcessor
from associacao.upgrade_associacao import UpgradeProcessor

# Configuração do log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicialize o processador de produto físico com a conexão do banco de dados
produto_fisico_processor = ProdutoFisicoProcessor()
associacao_nova = AssocicacaoProcessor()
associacao_upgrade = UpgradeProcessor()

# Inicializar FastAPI
app = FastAPI()


@app.on_event("startup")
async def startup():
    await db_connection.connect()
    logger.info("Conexão com o banco de dados testada na inicialização!")


@app.on_event("shutdown")
async def shutdown():
    await db_connection.close()
    logger.info("Conexão com o banco de dados fechada na finalização!")


@app.get("/test_connection")
async def test_connection():
    try:
        await db_connection.connect()
        logger.info("Conexão com o banco de dados testada com sucesso!")
        await db_connection.close()
        return {"status": "success", "message": "Conexão com o banco de dados testada com sucesso"}
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        return HTTPException(status_code=500, detail="Erro ao conectar ao banco de dados")


class DetalhesCompra(BaseModel):
    produto_id: Optional[int]
    tipo_produto: str
    quantidade: int
    preco: float
    nome_produto: Optional[str]
    tipo_pagamento: Optional[str]
    especificacoes: Optional[str]
    garantia: Optional[int]
    autor: Optional[str]
    isbn: Optional[str]
    valor_royalty: Optional[str]


class Compra(BaseModel):
    data: str
    cliente_id: str
    vendedor_id: str
    tipo_compra: str
    detalhes_compra: DetalhesCompra

    def clear(self):
        self.data = ""
        self.cliente_id = ""
        self.vendedor_id = ""
        self.tipo_compra = ""
        self.detalhes_compra = DetalhesCompra(
            produto_id=0, tipo_produto="", quantidade=0, preco=0.0, nome_produto="", tipo_pagamento="")


@app.post("/processar_compra")
async def processar_compra(compra: Compra):

    logger.info(f"Processando compra: {compra.tipo_compra}")

    compra_json = compra.dict()

    logger.info(compra.tipo_compra)

    match compra.tipo_compra:
        case "nova_associacao":
            logger.info('Tipo de compra: nova_associacao')
            df = pd.json_normalize(compra_json)
            if (await associacao_nova.processar_associacao(df)):
                compra_json.clear()
                return {"message": "Associação assinada com sucesso"}
            else:
                return {"message": "Error verifique log"}
        case "upgrade_associacao":
            logger.info('Tipo de compra: upgrade_associacao')
            df = pd.json_normalize(compra_json)
            if (await associacao_upgrade.upgrade_associacao(df)):
                compra_json.clear()
                return {"message": "Associação assinada com sucesso"}
            else:
                return {"message": "Error verifique log"}
        case "produto_fisico":
            logger.info('Tipo de compra: produto_fisico')
            # Converter detalhes_compra para DataFrame
            df = pd.json_normalize(compra_json)
            # Processar compra usando o produto_fisico_processor
            venda_id = await produto_fisico_processor.processar_compra(df)
            return {"message": "Compra processada com sucesso", "venda_id": venda_id}
        case _:
            raise HTTPException(
                status_code=400, detail="Tipo de compra não suportado")
