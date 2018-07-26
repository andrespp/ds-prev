prevbr dataset
==============

## Introdução

Dados da previdência social brasileira

## Origem dos dados
Dados públicos, disponibilidados pela CPI da Previdência, disponíveis
[neste link](https://legis.senado.leg.br/comissoes/docsRecCPI?codcol=2093) (Doc090/Midia018 e Doc097/Midia019).

## Conteúdo

| Origem | Arquivo | Tabela | # de Registros |
| ------ | ------- | -------| ---------------|
| DOC090 | APOSENTADORIA.csv | fato_aposentadoria | 46.545.767 |
| DOC090 | PENSAO.csv | fato_pensao | 5.647.457 |
| DOC097 | SEN_MICRODADOS01.APO.AUX.csv | 57.955.872 | |
| DOC097 | SEN_MICRODADOS02_PENS.csv | 7.964.185 | |

## Configuração do Ambiente

O dataset disponibilizado foi convertido para uma base [PostgreSQL](https://www.postgresql.org/) utilizando a ferramenta de [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) Pentaho Data Integration ([PDI v7.1](https://sourceforge.net/projects/pentaho/files/Data%20Integration/7.1/)).

Para simplificar a implantação do serviço, foi criado uma solução via containers [Docker](https://www.docker.com/what-docker) contendo o servidor PostgreSQL e uma instância do [Pgadmin4](https://www.pgadmin.org/) (cliente)

### Requisitos

Distribuição Linux com as seguintes ferramentas instaladas:

* GIT
* Curl
* unzip
* Docker
* Docker Compose

### Clone do Repositório

```bash
$ git clone ssh://git@go.andrepereira.eng.br:8372/bi/ds-prevbr.git
```

### Obtenção do dataset

```bash
$ cd ds-prevbr
$ mkdir dataset datasrc
$ # Download doc090 (1,5Gb) e doc097 (997Mb)
$ curl -L http://legis.senado.leg.br/sdleg-getter/documento/download/88fccff2-8836-4ea0-8847-869bade11cfa --output datasrc/doc090mid018.zip
$ curl -l http://legis.senado.leg.br/sdleg-getter/documento/download/d5774162-848a-4c60-bbb5-2d9291b9210e --output datasrc/doc097mid019.zip
$ ls datasrc/
 doc090mid018.zip  doc097mid019.zip
```

```bash
$ cd datasrc
$ unzip doc090mid018.zip
$ cd DOC\ 090\ MID\ 018/
$ unzip APOSENTADORIA.zip
$ mv APOSENTADORIA.CSV PENSAO.CSV ../../dataset/
$ cd ..
$ rm -rf DOC\ 090\ MID\ 018/
```

```bash
$ unzip doc097mid019.zip
$ cd DOC\ 097\ mid\ 019/
$ unzip D.ANA.INF.104.20170531.DM066978.ZIP
$ mv SEN_MICRODADOS01.APO.AUX.csv SEN_MICRODADOS02_PENS.csv ../../dataset/
$ cd ..
$ rm -rf DOC\ 097\ mid\ 019/
```

```bash
$ ls ../dataset/
 APOSENTADORIA.CSV  PENSAO.CSV  SEN_MICRODADOS01.APO.AUX.csv  SEN_MICRODADOS02_PENS.csv
```

### Servidor PostgreSQL

```bash
$ cd ../compose
$ docker-compose up -d
```

### Carregamento da Base

Ajustar o arquivo `kettle.properties` de acordo com o servidor desejado (`localhost` é o padrão)

```bash
$ vim ../kettle.properties
```
Ajustar quais tabelas devem ser carregadas na base (todas por padrão)

```bash
$ ./load_dataset.sh
```

## Utilizando o Dataset

## References

* [CPIPREV](https://legis.senado.leg.br/comissoes/docsRecCPI?codcol=2093 "DOC090 / Midia 018 and DOC097 e Midia 019") 
* [Anuário estatístico da Previdência Social em 2015](http://www.previdencia.gov.br/wp-content/uploads/2015/08/AEPS-2015-FINAL.pdf)
* [Wiki do projeto](https://prev.andrepereira.eng.br)
