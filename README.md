prevbr dataset
==============

## Introduction
Import CSV dataset to PostgreSQL server.

Dataset obtained [here](https://legis.senado.leg.br/comissoes/docsRecCPI?codcol=2093) (Doc090/Midia018 e Doc097/Midia019).

## Overview

This dataset contains...

## How to

1. Download database and extract csv files to `ds-prevbr/dataset`

2. Convert files that contains special characters to UTF-8 to avoid encoding errors:
```bash
ds-prevbr/dataset$ iconv -f ISO-8859-1 -t UTF-8 APOSENTADORIA.CSV > APOSENTADORIA.csv
ds-prevbr/dataset$ rm APOSENTADORIA.CSV
```

3. Run the job

## Using the dataset

## References

* [CPIPREV](https://legis.senado.leg.br/comissoes/docsRecCPI?codcol=2093 "DOC090 / Midia 018 and DOC097 e Midia 019") 
* [Anuário estatístico da Previdência Social em 2015](http://www.previdencia.gov.br/wp-content/uploads/2015/08/AEPS-2015-FINAL.pdf)
