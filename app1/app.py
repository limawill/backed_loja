import json
import time
import redis
from config import settings, logger
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from models import (DetalhesCompra, Compra, Associacao,
                    DetalhesAssociacao, DetalhesStreaming,
                    Streaming, Comissao, Remessa)

# Configurar Redis
r = redis.Redis(host=settings.redis.host, port=settings.redis.port)

# Inicializar FastAPI
app = FastAPI()

class Processador:
    def __init__(self):
        self.redis_client = r

    def enviar_para_classe(self, stream_name: str, data_json: str):
        self.redis_client.xadd(stream_name, {'data': data_json})

    async def processar_compra(self, compra: Compra):
        compra_json = compra.json()
        match compra.tipo_compra:
            case "produto_fisico":
                self.enviar_para_classe('stream_app1_app3', compra_json)
            case _:
                raise HTTPException(
                    status_code=400, detail="Tipo de compra não suportado"
                )

        logger.info("Aguardando respostas ...")

        while True:
            response_app3 = self.redis_client.xread({'stream_app3_app1': '0-0'}, block=1000)

            if response_app3:
                for stream, messages in response_app3:
                    for msg_id, msg in messages:
                        if b'status' in msg:
                            status = msg[b'status'].decode('utf-8')
                            venda_id = msg.get(b'venda_id', b'').decode('utf-8')

                            if status == 'true':
                                logger.info(f"Resposta recebida de app3: Venda: {venda_id}")
                                compra.clear()
                                self.redis_client.xdel('stream_app3_app1', msg_id)
                                return {"message": "Recebido e processado por produto_fisico", "data": {"venda_id": venda_id}}

                            else:
                                raise HTTPException(
                                    status_code=500, detail="Erro ao processar compra."
                                )
                        else:
                            logger.error("Chave 'status' não encontrada na mensagem.")

            print("Nenhuma resposta recebida, continuando a aguardar...")
            time.sleep(1)

    async def processar_associacao(self, associacao: Associacao):
        associacao_json = json.loads(associacao.json())

        logger.info(associacao_json)

        match associacao_json['tipo_assinatura']:
            case "nova_associacao" | "upgrade_associacao" | "ativacao_associacao":
                logger.info("Enviando para app2")
                self.enviar_para_classe('stream_app1_app2', json.dumps(associacao_json))
            case _:
                raise HTTPException(
                    status_code=400, detail="Tipo de assinatura não suportado"
                )

        logger.info("Aguardando respostas ...")

        while True:
            response_app2 = self.redis_client.xread({'stream_app2_app1': '0-0'}, block=1000)

            if response_app2:
                for stream, messages in response_app2:
                    for msg_id, msg in messages:
                        if b'status' in msg:
                            status = msg[b'status'].decode('utf-8')
                            if status == 'true':
                                logger.info(f"Resposta recebida de app2: {msg}")
                                associacao.clear()
                                self.redis_client.xdel('stream_app2_app1', msg_id)
                                return {"message": "Recebido e processado por nova_associacao"}

                            else:
                                raise HTTPException(
                                    status_code=500, detail="Erro ao processar associação."
                                )
                        else:
                            logger.error("Chave 'status' não encontrada na mensagem.")

            logger.info("Nenhuma resposta recebida, continuando a aguardar...")
            time.sleep(1)

    async def processar_streaming(self, streaming: Streaming):
        streaming_json = streaming.json()
        self.enviar_para_classe('stream_app1_app4', streaming_json)
        logger.info("Aguardando respostas ...")

        while True:
            response_app4 = self.redis_client.xread({'stream_app4_app1': '0-0'}, block=1000)

            if response_app4:
                for stream, messages in response_app4:
                    for msg_id, msg in messages:
                        if b'status' in msg:
                            status = msg[b'status'].decode('utf-8')
                            streaming_id = msg.get(b'video', b'').decode('utf-8')

                            if status == 'true':
                                logger.info(f"Resposta recebida de app4: Streaming: {streaming_id}")
                                streaming.clear()
                                self.redis_client.xdel('stream_app4_app1', msg_id)
                                return {"message": "Recebido e processado por streaming", "data": {"video": streaming_id}}

                            else:
                                raise HTTPException(
                                    status_code=500, detail="Erro ao enviar vídeos."
                                )
                        else:
                            logger.error("Chave 'status' não encontrada na mensagem.")

            print("Nenhuma resposta recebida, continuando a aguardar...")
            time.sleep(1)

    async def processar_comissao(self, comissao: Comissao):
        comissao_json = comissao.json()
        self.enviar_para_classe('stream_app1_app5', comissao_json)
        logger.info("Aguardando respostas ...")

        while True:
            response_app5 = self.redis_client.xread({'stream_app5_app1': '0-0'}, block=1000)

            if response_app5:
                for stream, messages in response_app5:
                    for msg_id, msg in messages:
                        if b'status' in msg:
                            status = msg[b'status'].decode('utf-8')
                            comissao_id = msg.get(b'vendedores', b'').decode('utf-8')
                            comissao_data = json.loads(comissao_id)

                            if status == 'true':
                                logger.info(f"Resposta recebida de app5: Comissao: {comissao_data}")
                                comissao.clear()
                                self.redis_client.xdel('stream_app5_app1', msg_id)
                                return {"message": "Recebido e processado por Comissão", "data": {"comissao": comissao_data}}

                            else:
                                raise HTTPException(
                                    status_code=500, detail="Erro ao calcular comissão do vendedor."
                                )
                        else:
                            logger.error("Chave 'status' não encontrada na mensagem.")

            print("Nenhuma resposta recebida, continuando a aguardar...")
            time.sleep(1)

    async def processar_remessa(self, remessa: Remessa):
        remessa_json = remessa.json()
        self.enviar_para_classe('stream_app1_app6', remessa_json)
        logger.info("Aguardando respostas ...")

        while True:
            response_app6 = self.redis_client.xread({'stream_app6_app1': '0-0'}, block=1000)

            if response_app6:
                for stream, messages in response_app6:
                    for msg_id, msg in messages:
                        if b'status' in msg:
                            status = msg[b'status'].decode('utf-8')
                            remessa_id = msg.get(b'remessa', b'').decode('utf-8')
                            remessa_data = json.loads(remessa_id)

                            if status == 'true':
                                logger.info("Resposta recebida de remessa")
                                remessa.clear()
                                self.redis_client.xdel('stream_app6_app1', msg_id)
                                return {"message": "Recebido e processado por Remessa", "data": {"remessa": remessa_data}}

                            else:
                                raise HTTPException(
                                    status_code=500, detail="Erro ao gerar a guia de remessa"
                                )
                        else:
                            logger.error("Chave 'status' não encontrada na mensagem.")

            print("Nenhuma resposta recebida, continuando a aguardar...")
            time.sleep(1)

processador = Processador()

@app.post("/processar_compra")
async def processar_compra_endpoint(compra: Compra):
    return await processador.processar_compra(compra)

@app.post("/processar_associacao")
async def processar_associacao_endpoint(associacao: Associacao):
    return await processador.processar_associacao(associacao)

@app.post("/streaming")
async def processar_streaming_endpoint(streaming: Streaming):
    return await processador.processar_streaming(streaming)

@app.get("/calcular_comissao")
async def processar_comissao_endpoint(comissao: Comissao):
    return await processador.processar_comissao(comissao)

@app.get("/gera_remessa")
async def processar_remessa_endpoint(remessa: Remessa):
    return await processador.processar_remessa(remessa)
