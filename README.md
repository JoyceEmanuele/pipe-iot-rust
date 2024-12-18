# Dash Performance Server

 O Dash Performance Server é composto por dois serviços, sendo eles:

O `rusthist` serve para buscar telemetrias de dentro do DynamoDB, processar (filtrando e compactando) para gerar um formato mais simples para exibir em gráficos no dash. Quando iniciado fica escutando em uma porta por requisições HTTP que chegarão do API-Server.

O `iotrelay` serve para se conectar ao(s) broker(s) MQTT, processar algumas telemetrias e repassar para o API-Server. Quando iniciado fica escutando em uma porta por conexões TCP. O API-Server se conecta a este serviço e envia a configuração de hardware dos dispositivos. Aí o serviço passa a converter alguns campos das telemetrias de DAC, como por exemplo de T0 para Tamb e de P0 para Psuc (de acordo com a configuração de sensores informada pelo API-Server). O API-Server envia pacotes MQTT através deste serviço, que por sua vez vai repassar a mensagem para todos os brokers aos quais estiver conectado.

## 🛠️ Ferramentas

- O projeto foi desenvolvido utilizando `Rust` como principal linguagem, sua documentação é bem interessante e pode ser encontrada no próprio [site da linguagem](https://prev.rust-lang.org/pt-BR/documentation.html). 

## 💻 Pré-requisitos

Antes de começar, é necessário verificar se todos os requisitos estão instalados em seu computador:

### Versão mais recente da linguagem Rust
O tutorial de instalação pode ser encontrado no próprio [site da Rust](https://www.rust-lang.org/tools/install). 

### Acesso do Cargo ao git 

O projeto se conecta com o projeto Falhas Repentinas, dessa maneira é necessário permitir que o Cargo se conecte com o Git, executando o seguinte comando:

```sh 
export CARGO_NET_GIT_FETCH_WITH_CLI=true
```

### O Cmake deve estar instalado

#### Windows
O instalador do Windows pode ser encontrado no próprio [site da Cmake](https://cmake.org/download/) 

#### Ubuntu (WSL)
Para instalar no Ubuntu ou WSL com Ubuntu o tutorial abaixo pode ser seguido: 

Instalando ferramentas e bibliotecas necessárias ao Cmake

```sh 
sudo apt-get install build-essential libssl-dev

```

Para fazer o donwload:

```sh 
cd /tmp
wget https://github.com/Kitware/CMake/releases/download/v3.20.0/cmake-3.20.0.tar.gz
tar -zxvf cmake-3.20.0.tar.gz
```

Compilando e instalando 
```sh 
cd cmake-3.20.0
./bootstrap
```

Dessa maneira o comando abaixo deve funcionar: 

```sh 
make
```

Por último então: 

```sh 
sudo make install
```

Para conferir a instalação: 

```sh 
cmake --version
```

### OpenSSL 

Pode ser necessário instalar uma lib OpenSSL. 

#### Windows

Para a instalação no Windows é interessante fazer uso do pacote VCPKG, segue abaixo o tutorial: 

```sh 
git clone https://github.com/microsoft/vcpkg.git
```

Vá para o diretório em que baixou o projeto e execute a instalação:

```sh 
cd /vcpkg
./bootstrap-vcpkg.bat
```

Instale o openSSL:

```sh 
./vcpkg.exe install openssl-windows:x64-windows
./vcpkg.exe install openssl:x64-windows-static
./vcpkg.exe install openssl:x64-windows-static-md
./vcpkg.exe integrate install
```

Adicione as variáveis de ambiente do sistema: 

```sh 
set VCPKGRS_DYNAMIC=
$env:OPENSSL_DIR="<caminho até vcpkg>\installed\x64-windows-static"
```

#### Ubuntu (WSL)

No caso do Ubuntu (WSL) pode ser instalada a seguinte: 

```sh 
sudo apt install librust-openssl-dev
```

### Cliente MySQL

Também pode ser necessário instalar um cliente mySQL

#### Windows 

No caso do Windows pode ser utilizado o pacote vcpkg, como para o OpenSSL

```sh 
./vcpkg.exe install install libmysql:x64-windows
./vcpkg.exe integrate install
```

#### Ubuntu (WSL)

No caso do Ubuntu (WSL) pode ser instalada a seguinte: 

```sh 
 sudo apt install libmysqlclient-dev
```

#### Protobuf 
Necessário para utilização das dependências do bigQuery.
Em versão Ubuntu abaixo da 24.04, não está funcionando instalação do protoc (compilador do protobuf) pelo apt.
Para contornar, necessário baixar a release diretamente no repositório da dependência e atualizar o PATH com a pasta bin.

Repositorio:  https://github.com/protocolbuffers/protobuf/releases
versão: protoc-28.1-linux-x86_64.zip

Comandos:
```
wget https://github.com/protocolbuffers/protobuf/releases/download/v28.1/protoc-28.1-linux-x86_64.zip
unzip protoc-28.1-linux-x86_64.zip

export PATH=$PATH:[caminho-arquivo-extraído]/bin
```
## 🚀 Subindo o projeto

```sh 
cargo build
```
### Para rodar o `rusthist` 

```sh 
 cargo run --bin rusthist
```
### Para rodar o `iotrelay` 

```sh 
 cargo run --bin iotrelay
```


## Configuração dos ambientes no GCP
- Criar um service-account (se já não existir): https://console.cloud.google.com/iam-admin/serviceaccounts
- Conceder permissão de "usuários de jobs do bigquery": https://console.cloud.google.com/iam-admin/iam
- Criar um dataset no BigQuery
- Conceder permissão de escrita no dataset para a service-account


## Referências Bibliográficas

- https://vitux.com/how-to-install-cmake-on-ubuntu/
- https://stackoverflow.com/questions/55912871/how-to-work-with-openssl-for-rust-within-a-windows-development-environment/61921362#61921362
