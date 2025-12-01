# Instalação do ClickHouse no Windows via WSL2

## Pré-requisitos

1. Windows 10 versão 2004 ou superior, ou Windows 11
2. WSL2 instalado

## Passo 1: Instalar WSL2 (se ainda não tiver)

Abra o PowerShell como **Administrador** e execute:

```powershell
wsl --install
```

Reinicie o computador quando solicitado.

## Passo 2: Instalar uma distribuição Linux (Ubuntu recomendado)

Após reiniciar, o Ubuntu será instalado automaticamente. Se não, execute:

```powershell
wsl --install -d Ubuntu
```

## Passo 3: Configurar o Ubuntu

1. Abra o Ubuntu no menu Iniciar
2. Crie um usuário e senha quando solicitado
3. Atualize o sistema:

```bash
sudo apt update && sudo apt upgrade -y
```

## Passo 4: Instalar ClickHouse no WSL

Execute os seguintes comandos no terminal Ubuntu:

```bash
# Adicionar repositório do ClickHouse
sudo apt-get install -y apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4

echo "deb https://repo.clickhouse.com/deb stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

# Instalar ClickHouse servidor e cliente
sudo apt-get install -y clickhouse-server clickhouse-client

# Iniciar o serviço
sudo service clickhouse-server start

# Verificar se está rodando
sudo service clickhouse-server status
```

## Passo 5: Configurar o ClickHouse

1. Editar o arquivo de configuração:

```bash
sudo nano /etc/clickhouse-server/config.xml
```

2. Adicionar as configurações necessárias (ou copiar o config.xml do projeto):

```bash
# No WSL, você pode acessar os arquivos do Windows em /mnt/c/
# Exemplo: /mnt/c/Users/SeuUsuario/Projeto CNPJ/v2/clickhouse/config.xml
```

3. Copiar o config.xml do projeto para o ClickHouse:

```bash
# Ajuste o caminho conforme necessário
sudo cp /mnt/c/Users/SeuUsuario/Projeto\ CNPJ/v2/clickhouse/config.xml /etc/clickhouse-server/config.d/custom.xml
```

4. Reiniciar o serviço:

```bash
sudo service clickhouse-server restart
```

## Passo 6: Configurar acesso remoto (opcional)

Se quiser acessar do Windows, edite `/etc/clickhouse-server/config.xml`:

```xml
<listen_host>0.0.0.0</listen_host>
```

E reinicie:

```bash
sudo service clickhouse-server restart
```

## Passo 7: Testar a conexão

No terminal Ubuntu:

```bash
clickhouse-client
```

Ou do Windows, usando o IP do WSL:

```bash
# Descobrir o IP do WSL
wsl hostname -I
```

## Passo 8: Configurar variáveis de ambiente (opcional)

Crie um arquivo `.env` no diretório `v2/importacao/`:

```env
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=cnpj
```

## Comandos úteis

```bash
# Iniciar ClickHouse
sudo service clickhouse-server start

# Parar ClickHouse
sudo service clickhouse-server stop

# Reiniciar ClickHouse
sudo service clickhouse-server restart

# Ver status
sudo service clickhouse-server status

# Ver logs
sudo tail -f /var/log/clickhouse-server/clickhouse-server.log
```

## Notas

- O ClickHouse no WSL usa o IP localhost, então `CLICKHOUSE_HOST=localhost` funciona normalmente
- Os dados ficam em `/var/lib/clickhouse/` no WSL
- Para acessar arquivos do Windows no WSL, use `/mnt/c/` (ex: `/mnt/c/Users/...`)

