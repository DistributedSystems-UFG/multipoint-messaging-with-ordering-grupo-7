[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/ItUD98Nn)

# Multipoint Messaging with Naming Service — PP 1.4

Distributed systems – PP 1.4 (extends PP 1.3)  
Equipe: Rafael, Lya, Igor

## Visão Geral

Extensão do trabalho anterior (PP 1.2) com a adição de um **Serviço de Nomes** simples (sem hierarquia, sem replicação). O Serviço de Nomes elimina toda configuração estática de endereços: **o único endereço fixo permitido nos peers é o do próprio Serviço de Nomes**.

O Serviço de Nomes acumula também a função de **Serviço de Diretório**, substituindo o Group Manager da versão anterior: a operação `discover("peer")` retorna todos os peers registrados e seus endereços.

---

## Arquitetura

```
naming-service:50050          (único endereço estático conhecidos pelos peers)
       │
       ├─ bind / lookup / unbind / register / discover
       │
peer-1:50070 ←─── descobre peer-2..4 via discover("peer") ───→ peer-2:50070
peer-3:50070 ←──────────────────────────────────────────────→ peer-4:50070
```

Cada peer, ao iniciar:
1. Conecta ao Serviço de Nomes (endereço vem de `NAME_SERVICE_ADDRESS`).
2. Chama `bind(nome, "host:porta")` — registra seu endereço.
3. Chama `register(nome, "peer")` — informa seu tipo.
4. Sobe um servidor gRPC para receber mensagens de outros peers.
5. Periodicamente chama `discover("peer")` e envia mensagens aleatórias.
6. Ao ser encerrado (SIGTERM): chama `unbind(nome)` antes de sair.

---

## Serviço de Nomes — Interface gRPC

Definida em `proto/naming.proto`:

| Operação | Descrição |
|---|---|
| `bind(name, address)` | Cria registro nome→endereço. Retorna `ok` ou `error`. |
| `lookup(name)` | Retorna endereço associado ao nome, ou erro se não existir. |
| `unbind(name)` | Remove o nome e seu registro. |
| `register(name, type)` | Associa um tipo a um nome já registrado; erro se nome não existir. |
| `discover(type)` | Retorna lista de `{name, address}` do tipo indicado. |

---

## Estrutura do Projeto

```
.
├── proto/
│   ├── naming.proto        # interface do Serviço de Nomes
│   └── peer.proto          # interface de mensagens entre peers
├── naming_service/
│   ├── server.py           # implementação do Serviço de Nomes
│   └── Dockerfile
├── peer/
│   ├── peer.py             # nó peer: registra-se, descobre, envia mensagens
│   └── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Como Executar

### Pré-requisitos

- Docker e Docker Compose instalados.

### Subir tudo

```bash
docker compose up --build
```

### Observar logs de um peer específico

```bash
docker compose logs -f peer-1
```

### Simular saída e entrada de um peer

```bash
# remover peer-3 (unbind automático via SIGTERM)
docker compose stop peer-3

# re-adicionar
docker compose start peer-3
```

### Parar tudo

```bash
docker compose down
```

---

## Exemplo de Saída

**Serviço de Nomes:**
```
[NAMING-SVC] BIND    peer-1               peer-1:50070
[NAMING-SVC] REGISTER peer-1              type=peer
[NAMING-SVC] DISCOVER type=peer       4 result(s)
```

**Peer:**
```
[peer-1] Bound   peer-1  →  peer-1:50070
[peer-1] Registered peer-1 as type=peer
[peer-1] → peer-3               "Hello from peer-1! (token=4217)"
[peer-1] ← peer-2               "Hello from peer-2! (token=8831)"
```

---

---

## Modelo de Consistência — Consistência Causal (CO)

### Análise do requisito

Cada peer mantém um **log de mensagens entregues** — essa é a réplica cuja consistência precisa ser garantida. O requisito central é:

> Se a mensagem M₂ foi enviada *depois* que o remetente de M₂ recebeu M₁ (relação de causalidade M₁ → M₂), então **todo peer** que receber M₂ já deve ter M₁ no seu log antes de M₂.

Isso é exatamente a definição de **consistência causal**. Ela é mais forte que FIFO (que garante ordem apenas por par remetente→destino) e mais fraca que consistência sequencial (que imporia uma ordem total mesmo entre mensagens sem relação causal). Para uma aplicação de mensagens distribuídas, a consistência causal é o modelo adequado: o usuário espera que respostas apareçam depois das perguntas a que respondem, mas não se importa com a ordem relativa de mensagens independentes.

### Implementação — Relógios Vetoriais + Fila de Espera

**Cada peer P mantém:**
- `VC_P`: relógio vetorial `{nome_peer → inteiro}` (entrada ausente = 0)
- `HBQ`: fila de espera (*hold-back queue*) de mensagens ainda não entregáveis

**Ao ENVIAR:**
1. Incrementa `VC_P[P]`
2. Faz uma cópia do `VC_P` (snapshot)
3. Envia o snapshot piggybacked no campo `vector_clock` da mensagem
4. Envia para **todos** os peers conhecidos (multicast)

**Ao RECEBER** mensagem de Q com relógio `VC_msg`:

Condição de entregabilidade CO:
```
VC_msg[Q]  ==  VC_P[Q] + 1       (próxima mensagem de Q em sequência)
VC_msg[X]  <=  VC_P[X]  ∀ X ≠ Q  (já vimos tudo que Q havia visto)
```

- Se entregável: entrega imediatamente, atualiza `VC_P = max(VC_P, VC_msg)`, verifica HBQ
- Se não entregável: coloca na HBQ; tenta reentrega após cada entrega bem-sucedida

**Proto alterado** (`proto/peer.proto`):
```protobuf
message MessageRequest {
  string sender      = 1;
  string content     = 2;
  map<string, int64> vector_clock = 3;   // piggybacked VC
}
```

### O que os logs mostram

| Prefixo | Significado |
|---------|-------------|
| `SEND → N peers … vc={…}` | Broadcast com relógio vetorial |
| `DELIVER ← sender … vc={…}` | Mensagem entregue ao log (CO satisfeita) |
| `HBQ ← sender … [buffered, waiting]` | Mensagem na fila de espera (CO não satisfeita ainda) |

---

## Implantação AWS — 6 peers em 6 regiões

| Processo | Região AWS | Papel |
|----------|-----------|-------|
| naming-service | us-east-1 (N. Virginia) | registro e descoberta |
| peer-1 | us-east-1 (N. Virginia) | réplica 1 |
| peer-2 | us-west-2 (Oregon) | réplica 2 |
| peer-3 | eu-west-1 (Irlanda) | réplica 3 |
| peer-4 | ap-southeast-1 (Singapura) | réplica 4 |
| peer-5 | sa-east-1 (São Paulo) | réplica 5 |
| peer-6 | ap-northeast-1 (Tóquio) | réplica 6 |

### Build e push das imagens

```bash
# Na raiz do projeto
docker build -t <SEU_USUARIO>/pp14-naming -f naming_service/Dockerfile .
docker build -t <SEU_USUARIO>/pp14-peer   -f peer/Dockerfile            .
docker push <SEU_USUARIO>/pp14-naming
docker push <SEU_USUARIO>/pp14-peer
```

### Deploy automatizado

```bash
# Edite DOCKERHUB_USER e KEY_NAME no script antes de executar
chmod +x deploy/aws_deploy.sh
./deploy/aws_deploy.sh
```

O script cria as instâncias EC2, instala Docker e sobe os containers automaticamente.

### Deploy manual (por instância)

```bash
# Na instância do naming-service (us-east-1):
docker run -d --restart=always -p 50050:50050 \
  -e PORT=50050 \
  <SEU_USUARIO>/pp14-naming

# Em cada instância de peer (substitua os valores):
PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)
docker run -d --restart=always -p 50070:50070 \
  -e PEER_NAME="peer-N" \
  -e PEER_HOST="$PUBLIC_IP" \
  -e PEER_PORT=50070 \
  -e NAME_SERVICE_ADDRESS="<IP_NAMING_SERVICE>:50050" \
  <SEU_USUARIO>/pp14-peer
```

### Security Groups necessários

| Serviço | Porta | Protocolo |
|---------|-------|-----------|
| naming-service | 50050 | TCP (inbound 0.0.0.0/0) |
| peer | 50070 | TCP (inbound 0.0.0.0/0) |
| SSH | 22 | TCP (inbound seu IP) |

---

## Decisões de Projeto

- **Middleware:** gRPC (Python), alinhado com os trabalhos anteriores do grupo.
- **Configuração:** variáveis de ambiente no `docker-compose.yml`. O único endereço estático em qualquer peer é `NAME_SERVICE_ADDRESS: "naming-service:50050"`.
- **Sem Group Manager:** `discover("peer")` substitui completamente o papel do Group Manager.
- **Desligamento gracioso:** cada peer captura `SIGTERM` e chama `unbind` antes de encerrar, mantendo o registro consistente.
- **Rebind:** se um peer com o mesmo nome se reconectar, `bind` sobrescreve o endereço anterior sem erro.
- **Consistência causal via relógios vetoriais:** campo `vector_clock` piggybacked em cada `MessageRequest`; fila de espera (HBQ) retém mensagens até que a condição CO seja satisfeita.
- **Multicast:** cada peer envia para *todos* os peers descobertos (não apenas um aleatório), garantindo que o mesmo VC seja distribuído uniformemente.
- **6 réplicas:** `docker-compose.yml` sobe peer-1…peer-6; no AWS, cada peer é implantado em uma região diferente para demonstrar a manutenção da consistência sob latências reais inter-regionais.

---

## Para terminar — O que falta fazer (leia antes de começar)

O código está completo e as imagens Docker já estão publicadas no Docker Hub:
- `rafaelstaveira/pp14-naming`
- `rafaelstaveira/pp14-peer`

### Se precisar rebuildar as imagens (só se mudar o código)

```bash
docker build -t rafaelstaveira/pp14-naming -f naming_service/Dockerfile .
docker build -t rafaelstaveira/pp14-peer -f peer/Dockerfile .
docker push rafaelstaveira/pp14-naming
docker push rafaelstaveira/pp14-peer
```

### Deploy na AWS — 7 instâncias EC2

Abrir console AWS. Key pair necessária: `pp14-key` (já existe em us-east-1 e us-west-2).

#### 1. Naming service → us-east-1

EC2 → Launch Instance:
- AMI: Ubuntu 22.04 LTS / t2.micro / Key: `pp14-key`
- Security group: crie `pp14-ns-sg` abrindo TCP 50050 e 22
- User data:
```
#!/bin/bash
apt-get update -y && apt-get install -y docker.io
systemctl start docker
docker pull rafaelstaveira/pp14-naming
docker run -d --restart=always -p 50050:50050 -e PORT=50050 rafaelstaveira/pp14-naming
```
Após lançar, copie o **IP público** da instância — você vai precisar dele nos scripts abaixo (`NS_IP`).

#### 2. Peers 1, 2, 3 → us-east-1

Repita 3x mudando `peer-1` → `peer-2` → `peer-3` e substituindo `NS_IP` pelo IP real:
- Security group: `pp14-peer-sg` (crie se não existir, abrindo TCP 50070 e 22)
- User data:
```
#!/bin/bash
apt-get update -y && apt-get install -y docker.io
systemctl start docker
PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)
docker pull rafaelstaveira/pp14-peer
docker run -d --restart=always -p 50070:50070 \
  -e PEER_NAME=peer-1 \
  -e PEER_HOST=$PUBLIC_IP \
  -e PEER_PORT=50070 \
  -e NAME_SERVICE_ADDRESS=NS_IP:50050 \
  -e MSG_INTERVAL_LO=3.0 \
  -e MSG_INTERVAL_HI=7.0 \
  rafaelstaveira/pp14-peer
```

#### 3. Peers 4, 5, 6 → us-west-2

Troque a região para **us-west-2** e repita com `peer-4`, `peer-5`, `peer-6`. Criar `pp14-peer-sg` nesta região também (mesmas portas).

### Verificar (~5 min após lançar)

```bash
ssh -i pp14-key.pem ubuntu@<IP_PEER> "docker logs \$(docker ps -q) 2>&1 | tail -20"
```
Deve mostrar linhas com `SEND`, `DELIVER` e `HBQ`.

### Gravar o vídeo

Mostrar logs de 2-3 peers simultâneos evidenciando:
- `SEND → N peers … vc={...}` — broadcast com relógio vetorial
- `DELIVER ← peer-X … vc={...}` — entrega causal
- `HBQ ← peer-X … [buffered]` — mensagem aguardando na fila causal
