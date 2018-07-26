#!/bin/bash
#
#
PDI_KITCHEN="docker container run --name etlprevds --rm -v $PWD:/jobs -v $PWD/kettle.properties:/data-integration/kettle.properties andrespp/pdi runj"
PDI_PAN="docker container run --name etlprevds --rm -v $PWD:/jobs -v $PWD/kettle.properties:/data-integration/kettle.properties andrespp/pdi runt"

A=`date`

############################################
## Individual transformation files
PDI_TRF=""

## Auxiliary transformations
PDI_TRF="$PDI_TRF ./aux_create_tables.ktr"

## Dimension Tables
PDI_TRF="$PDI_TRF ./dim_data.ktr"
PDI_TRF="$PDI_TRF ./dim_situacao_beneficio.ktr"
PDI_TRF="$PDI_TRF ./dim_mot_cessacao.ktr"
PDI_TRF="$PDI_TRF ./dim_clientela.ktr"
PDI_TRF="$PDI_TRF ./dim_especie_classificacao.ktr"
PDI_TRF="$PDI_TRF ./dim_especie.ktr"
PDI_TRF="$PDI_TRF ./dim_sexo.ktr"

## Fact Tables (core)
PDI_TRF="$PDI_TRF ./fato_auxilio.ktr"
PDI_TRF="$PDI_TRF ./fato_aposentadoria.ktr"
PDI_TRF="$PDI_TRF ./fato_pensao2.ktr"
PDI_TRF="$PDI_TRF ./fato_pensao.ktr"

## Fact Tables (auxiliary)
PDI_TRF="$PDI_TRF ./fato_auxilio_sample.ktr"
PDI_TRF="$PDI_TRF ./fato_auxilio_2015.ktr"
PDI_TRF="$PDI_TRF ./fato_auxilio_raw.ktr"

# Run transformations
for i in $PDI_TRF ; do
	echo $PDI_PAN $i
	$PDI_PAN $i
done

echo "Started at: $A"
echo "Finised at: `date`"

