from pydantic import BaseModel


class DetalhesCompra(BaseModel):
    produto_id: int
    tipo_produto: str
    quantidade: int
    preco: float
    nome_produto: str
    tipo_pagamento: str
    especificacoes: str = None  # Opcional para produtos físicos
    garantia: int = None  # Opcional para produtos físicos
    autor: str = None  # Opcional para livros
    isbn: str = None  # Opcional para livros
    valor_royalty: str = None  # Opcional para livros


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
            produto_id=0, tipo_produto="", quantidade=0, preco=0.0, nome_produto="", tipo_pagamento="", especificacoes="", garantia=0, autor="", isbn="", valor_royalty="")


class DetalhesAssociacao(BaseModel):
    nome_plano: str
    ativo: bool


class Associacao(BaseModel):
    data: str
    cliente_id: str
    vendedor_id: str
    tipo_assinatura: str
    detalhes_compra: DetalhesAssociacao

    def clear(self):
        self.data = ""
        self.cliente_id = ""
        self.vendedor_id = ""
        self.tipo_assinatura = ""
        self.detalhes_compra = DetalhesAssociacao(nome_plano="", ativo=False)


class DetalhesStreaming(BaseModel):
    id_streaming: str


class Streaming(BaseModel):
    data: str
    cliente_id: str
    detalhes_compra: DetalhesStreaming

    def clear(self):
        self.data = ""
        self.cliente_id = ""
        self.detalhes_compra = DetalhesStreaming(id_streaming="")


class Comissao(BaseModel):
    mes: int
    ano: int
    vendedor_id: int

    def clear(self):
        self.mes = ""
        self.ano = ""
        self.vendedor_id = ""


class Remessa(BaseModel):
    codigo_venda: int

    def clear(self):
        self.codigo_venda = ""
