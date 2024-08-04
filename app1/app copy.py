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


def enviar_para_classe(stream_name, compra_json):
    r.xadd(stream_name, {'data': compra_json})


@app.post("/processar_compra")
async def processar_compra(compra: Compra):

    compra_json = compra.json()
    match compra.tipo_compra:
        case "produto_fisico":
            enviar_para_classe('stream_app1_app3', compra_json)
        case _:
            raise HTTPException(
                status_code=400, detail="Tipo de compra não suportado")

    logger.info("Aguardando respostas ...")

    while True:

        response_app3 = r.xread({'stream_app3_app1': '0-0'}, block=1000)

        if response_app3:
            for stream, messages in response_app3:
                for msg_id, msg in messages:
                 # Log detalhado da mensagem

                    if b'status' in msg:
                        status = msg[b'status'].decode('utf-8')
                        venda_id = msg.get(b'venda_id', b'').decode('utf-8')

                        if status == 'true':
                            logger.info(
                                f"Resposta recebida de app3: Venda: {venda_id}")
                            compra.clear()
                            r.xdel('stream_app3_app1', msg_id)
                            return {"message": "Recebido e processado por produto_fisico", "data": {"venda_id": venda_id}}

                        else:
                            raise HTTPException(
                                status_code=500, detail="Erro ao processar compra.")
                    else:
                        logger.error(
                            "Chave 'status' não encontrada na mensagem.")

        print("Nenhuma resposta recebida, continuando a aguardar...")
        time.sleep(1)


@app.post("/processar_associacao")
async def processar_associacao(associacao: Associacao):
    associacao_json = json.loads(associacao.json())

    logger.info(associacao_json)

    match associacao_json['tipo_assinatura']:
        case "nova_associacao" | "upgrade_associacao" | "ativacao_associacao":
            logger.info("Enviando para app2")
            enviar_para_classe('stream_app1_app2', json.dumps(associacao_json))
        case _:
            raise HTTPException(
                status_code=400, detail="Tipo de compra não suportado")

    logger.info("Aguardando respostas ...")

    while True:
        response_app2 = r.xread({'stream_app2_app1': '0-0'}, block=1000)

        if response_app2:
            for stream, messages in response_app2:
                for msg_id, msg in messages:
                    if b'status' in msg:
                        status = msg[b'status'].decode('utf-8')
                        if status == 'true':
                            logger.info(f"Resposta recebida de app2: {msg}")
                            associacao.clear()
                            r.xdel('stream_app2_app1', msg_id)
                            return {"message": "Recebido e processado por nova_associacao"}

                        else:
                            raise HTTPException(
                                status_code=500, detail="Erro ao processar associação.")
                    else:
                        logger.error(
                            "Chave 'status' não encontrada na mensagem.")

        logger.info("Nenhuma resposta recebida, continuando a aguardar...")
        time.sleep(1)


@app.post("/streaming")
async def processar_streaming(streaming: Streaming):

    streaming_json = streaming.json()
    enviar_para_classe('stream_app1_app4', streaming_json)
    logger.info("Aguardando respostas ...")

    while True:

        response_app4 = r.xread({'stream_app4_app1': '0-0'}, block=1000)

        if response_app4:
            for stream, messages in response_app4:
                for msg_id, msg in messages:
                    # Log detalhado da mensagem

                    if b'status' in msg:
                        status = msg[b'status'].decode('utf-8')
                        streaming_id = msg.get(b'video', b'').decode('utf-8')

                        if status == 'true':
                            logger.info(
                                f"Resposta recebida de app4: Streaming: {streaming_id}")
                            streaming.clear()
                            r.xdel('stream_app4_app1', msg_id)
                            return {"message": "Recebido e processado por produto_fisico", "data": {"video": streaming_id}}

                        else:
                            raise HTTPException(
                                status_code=500, detail="Erro ao enviar Videos.")
                    else:
                        logger.error(
                            "Chave 'status' não encontrada na mensagem.")

        print("Nenhuma resposta recebida, continuando a aguardar...")
        time.sleep(1)


@app.get("/calcular_comissao")
async def processar_comissao(comissao: Comissao):
    comissao_json = comissao.json()

    enviar_para_classe('stream_app1_app5', comissao_json)
    logger.info("Aguardando respostas ...")

    while True:
        response_app5 = r.xread({'stream_app5_app1': '0-0'}, block=1000)
        if response_app5:
            for stream, messages in response_app5:
                for msg_id, msg in messages:
                    # Log detalhado da mensagem
                    if b'status' in msg:
                        status = msg[b'status'].decode('utf-8')
                        comissao_id = msg.get(
                            b'vendedores', b'').decode('utf-8')
                        comissao_data = json.loads(comissao_id)
                        if status == 'true':
                            logger.info(f"Resposta recebida de app5: Comissao: {
                                        comissao_data}")
                            comissao.clear()
                            r.xdel('stream_app5_app1', msg_id)
                            return {"message": "Recebido e processado por Comissão", "data": {"comissao": comissao_data}}
                        else:
                            raise HTTPException(
                                status_code=500, detail="Erro ao calcular comissão do vendedor.")
                    else:
                        logger.error(
                            "Chave 'status' não encontrada na mensagem.")

        print("Nenhuma resposta recebida, continuando a aguardar...")
        time.sleep(1)


@app.get("/gera_remessa")
async def processar_remessa(remessa: Remessa):
    remessa_json = remessa.json()

    enviar_para_classe('stream_app1_app6', remessa_json)
    logger.info("Aguardando respostas ...")

    while True:
        response_app6 = r.xread({'stream_app6_app1': '0-0'}, block=1000)
        if response_app6:
            for stream, messages in response_app6:
                for msg_id, msg in messages:
                    # Log detalhado da mensagem
                    if b'status' in msg:
                        status = msg[b'status'].decode('utf-8')
                        remessa_id = msg.get(
                            b'remessa', b'').decode('utf-8')
                        remessa_data = json.loads(remessa_id)
                        if status == 'true':
                            logger.info("Resposta recebida de remessa")
                            remessa.clear()
                            r.xdel('stream_app6_app1', msg_id)
                            return {"message": "Recebido e processado por Remessa", "data": {"remessa": remessa_data}}
                        else:
                            raise HTTPException(
                                status_code=500, detail="Erro ao gerar a guia de remessa")
                    else:
                        logger.error(
                            "Chave 'status' não encontrada na mensagem.")

        print("Nenhuma resposta recebida, continuando a aguardar...")
        time.sleep(1)
