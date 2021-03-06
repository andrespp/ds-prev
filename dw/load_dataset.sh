#!/bin/bash
#
#
PDI_KITCHEN="docker container run -v $PWD/../:/jobs -v $PWD/kettle.properties:/data-integration/kettle.properties \
			--name etlprevds --network prevnet --rm  andrespp/pdi runj"
PDI_PAN="docker container run -v $PWD/../:/jobs -v $PWD/kettle.properties:/data-integration/kettle.properties \
			--name etlprevds --network prevnet --rm  andrespp/pdi runt"
A=`date`

############################################
## Individual transformation files
PDI_TRF=""

## Auxiliary transformations
PDI_TRF="$PDI_TRF ./dw/aux_create_tables.ktr"

## Dimension Tables
PDI_TRF="$PDI_TRF ./dw/dim_data.ktr"
PDI_TRF="$PDI_TRF ./dw/dim_situacao_beneficio.ktr"
PDI_TRF="$PDI_TRF ./dw/dim_mot_cessacao.ktr"
PDI_TRF="$PDI_TRF ./dw/dim_clientela.ktr"
PDI_TRF="$PDI_TRF ./dw/dim_especie_classificacao.ktr"
PDI_TRF="$PDI_TRF ./dw/dim_especie.ktr"
PDI_TRF="$PDI_TRF ./dw/dim_sexo.ktr"

## Fact Tables (core)
PDI_TRF="$PDI_TRF ./dw/fato_auxilio.ktr"
PDI_TRF="$PDI_TRF ./dw/fato_aposentadoria.ktr"
PDI_TRF="$PDI_TRF ./dw/fato_pensao2.ktr"
PDI_TRF="$PDI_TRF ./dw/fato_pensao.ktr"

## Fact Tables (auxiliary)
PDI_TRF="$PDI_TRF ./dw/fato_auxilio_sample.ktr"
PDI_TRF="$PDI_TRF ./dw/fato_auxilio_2015.ktr"
PDI_TRF="$PDI_TRF ./dw/fato_auxilio_raw.ktr"

# Run transformations
LOG="\n"
for i in $PDI_TRF ; do
	echo $PDI_PAN $i
	$PDI_PAN $i

	LOG="$LOG$i exited with status $? \n"
done

echo -e $LOG
echo "Started at: $A"
echo "Finised at: `date`"

