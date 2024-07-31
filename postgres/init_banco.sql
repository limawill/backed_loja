CREATE TABLE IF NOT EXISTS cliente (
    cpf VARCHAR(20) PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    rg VARCHAR(20) NOT NULL,
    endereco VARCHAR(255) NOT NULL,
    cidade VARCHAR(100) NOT NULL,
    estado VARCHAR(2) NOT NULL,
    cep VARCHAR(10) NOT NULL,
    email VARCHAR(100) NOT NULL,
    telefone VARCHAR(20) NOT NULL
);

INSERT INTO cliente (cpf, nome, rg, endereco, cidade, estado, cep, email, telefone) VALUES
('123.456.789-00', 'Carlos Silva', 'MG-12.345.678', 'Rua A, 123', 'São Paulo', 'SP', '01010-000', 'carlos.silva@example.com', '(11) 1234-5678'),
('234.567.890-12', 'Ana Souza', 'MG-12.345.679', 'Rua B, 456', 'Rio de Janeiro', 'RJ', '02020-000', 'ana.souza@example.com', '(21) 2345-6789'),
('345.678.901-23', 'Lucas Pereira', 'MG-12.345.680', 'Rua C, 789', 'Belo Horizonte', 'MG', '03030-000', 'lucas.pereira@example.com', '(31) 3456-7890'),
('456.789.012-34', 'Fernanda Costa', 'MG-12.345.681', 'Rua D, 101', 'Curitiba', 'PR', '04040-000', 'fernanda.costa@example.com', '(41) 4567-8901'),
('567.890.123-45', 'Ricardo Rocha', 'MG-12.345.682', 'Rua E, 202', 'Porto Alegre', 'RS', '05050-000', 'ricardo.rocha@example.com', '(51) 5678-9012'),
('678.901.234-56', 'Juliana Lima', 'MG-12.345.683', 'Rua F, 303', 'Florianópolis', 'SC', '06060-000', 'juliana.lima@example.com', '(41) 6789-0123'),
('789.012.345-67', 'Eduardo Santos', 'MG-12.345.684', 'Rua G, 404', 'Salvador', 'BA', '07070-000', 'eduardo.santos@example.com', '(71) 7890-1234'),
('890.123.456-78', 'Patricia Alves', 'MG-12.345.685', 'Rua H, 505', 'Fortaleza', 'CE', '08080-000', 'patricia.alves@example.com', '(85) 8901-2345'),
('901.234.567-89', 'João Martins', 'MG-12.345.686', 'Rua I, 606', 'Brasília', 'DF', '09090-000', 'joao.martins@example.com', '(61) 9012-3456'),
('012.345.678-90', 'Mariana Souza', 'MG-12.345.687', 'Rua J, 707', 'Manaus', 'AM', '10010-000', 'mariana.souza@example.com', '(92) 0123-4567');

CREATE TABLE IF NOT EXISTS vendedor (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    porcentagem FLOAT NOT NULL
);

INSERT INTO vendedor (nome, porcentagem) VALUES
('Pedro Silva', 5.0),
('Maria Oliveira', 6.0),
('Carlos Santos', 7.0),
('Joana Lima', 4.5),
('Lucas Rocha', 5.5),
('Fernanda Costa', 6.5),
('Ricardo Pereira', 7.5),
('Juliana Alves', 5.0),
('Eduardo Souza', 6.0),
('Patricia Martins', 7.0);


CREATE TABLE IF NOT EXISTS produtos (
    id SERIAL PRIMARY KEY,
    nome TEXT NOT NULL,
    preco REAL NOT NULL,
    tipo TEXT NOT NULL  -- 'notebook', 'livro', 'mouse', etc.
);

CREATE TABLE IF NOT EXISTS detalhes_produtos_fisicos (
    id SERIAL PRIMARY KEY,
    produto_id INTEGER NOT NULL,
    especificacoes TEXT,
    garantia INTEGER,
    FOREIGN KEY (produto_id) REFERENCES produtos(id)
);

CREATE TABLE IF NOT EXISTS livros (
    id SERIAL PRIMARY KEY,
    produto_id INTEGER NOT NULL,
    autor TEXT,
    isbn TEXT,
    FOREIGN KEY (produto_id) REFERENCES produtos(id)
);


-- Inserção de produtos físicos
INSERT INTO produtos (nome, preco, tipo) VALUES ('Laptop', 1500.00, 'notebook');
INSERT INTO detalhes_produtos_fisicos (produto_id, especificacoes, garantia) VALUES (1, '8GB RAM, 256GB SSD', 24);

INSERT INTO produtos (nome, preco, tipo) VALUES ('Mouse', 50.00, 'mouse');
INSERT INTO detalhes_produtos_fisicos (produto_id, especificacoes, garantia) VALUES (2, 'Wireless, 1600 DPI', 12);

INSERT INTO produtos (nome, preco, tipo) VALUES ('Caneta', 5.00, 'caneta');
INSERT INTO detalhes_produtos_fisicos (produto_id, especificacoes, garantia) VALUES (3, 'Tinta azul, corpo plástico', NULL);

INSERT INTO produtos (nome, preco, tipo) VALUES ('Teclado', 100.00, 'teclado');
INSERT INTO detalhes_produtos_fisicos (produto_id, especificacoes, garantia) VALUES (4, 'Mecânico, RGB', 24);

INSERT INTO produtos (nome, preco, tipo) VALUES ('Tênis', 200.00, 'tenis');
INSERT INTO detalhes_produtos_fisicos (produto_id, especificacoes, garantia) VALUES (5, 'Número 42, cor preta', 6);

-- Inserção de livros de J.R.R. Tolkien
INSERT INTO produtos (nome, preco, tipo) VALUES ('The Hobbit', 25.00, 'livro');
INSERT INTO livros (produto_id, autor, isbn) VALUES (6, 'J.R.R. Tolkien', '978-0-618-00221-3');

INSERT INTO produtos (nome, preco, tipo) VALUES ('The Fellowship of the Ring', 30.00, 'livro');
INSERT INTO livros (produto_id, autor, isbn) VALUES (7, 'J.R.R. Tolkien', '978-0-618-00222-0');

INSERT INTO produtos (nome, preco, tipo) VALUES ('The Two Towers', 30.00, 'livro');
INSERT INTO livros (produto_id, autor, isbn) VALUES (8, 'J.R.R. Tolkien', '978-0-618-00223-7');

INSERT INTO produtos (nome, preco, tipo) VALUES ('The Return of the King', 30.00, 'livro');
INSERT INTO livros (produto_id, autor, isbn) VALUES (9, 'J.R.R. Tolkien', '978-0-618-00224-4');

INSERT INTO produtos (nome, preco, tipo) VALUES ('The Silmarillion', 35.00, 'livro');
INSERT INTO livros (produto_id, autor, isbn) VALUES (10, 'J.R.R. Tolkien', '978-0-618-00225-1');


-- controle de produtos fisicos (vendas, royalty, etc)

CREATE TABLE IF NOT EXISTS vendas (
    id SERIAL PRIMARY KEY,
    data DATE NOT NULL,
    cliente_id VARCHAR(20) NOT NULL,
    vendedor_id INTEGER NOT NULL,
    tipo_compra TEXT NOT NULL,
    produto_id INTEGER NOT NULL,
    quantidade INTEGER NOT NULL,
    preco REAL NOT NULL,
    tipo_pagamento TEXT NOT NULL,
    FOREIGN KEY (cliente_id) REFERENCES cliente(cpf),
    FOREIGN KEY (vendedor_id) REFERENCES vendedor(id),
    FOREIGN KEY (produto_id) REFERENCES produtos(id)
);

CREATE TABLE IF NOT EXISTS guias_royalty (
    id SERIAL PRIMARY KEY,
    venda_id INTEGER NOT NULL,
    data_geracao DATE NOT NULL,
    status TEXT NOT NULL,
    valor REAL NOT NULL,
    FOREIGN KEY (venda_id) REFERENCES vendas(id)
);

CREATE TABLE IF NOT EXISTS  comissoes (
    id SERIAL PRIMARY KEY,
    venda_id INTEGER NOT NULL,
    vendedor_id INTEGER NOT NULL,
    data_pagamento DATE NOT NULL,
    valor REAL NOT NULL,
    status TEXT NOT NULL,
    FOREIGN KEY (venda_id) REFERENCES vendas(id),
    FOREIGN KEY (vendedor_id) REFERENCES vendedor(id)
);
