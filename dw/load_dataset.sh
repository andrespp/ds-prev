#!/bin/bash
#
#
PDI_KITCHEN="/home/andre/Downloads/pentaho/data-integration-ce-5.0.1.A-stable/kitchen.sh"
PDI_PAN="/home/andre/Downloads/pentaho/data-integration-ce-5.0.1.A-stable/pan.sh"


############################################
## Individual transformation files
PDI_TRF=""

## Dimension Tables
#PDI_TRF="$PDI_TRF ./dim_situacao_beneficio.ktr"
PDI_TRF="$PDI_TRF ./dim_especie.ktr"

## Fact Tables (core)
#PDI_TRF="$PDI_TRF ./fato_aposentadoria.ktr"
#PDI_TRF="$PDI_TRF ./fato_auxilio.ktr"
#PDI_TRF="$PDI_TRF ./fato_pensao2.ktr"
#PDI_TRF="$PDI_TRF ./fato_pensao.ktr"

## Fact Tables (auxiliary)
#PDI_TRF="$PDI_TRF ./fato_auxilio_2015.ktr"

# Run transformations
for i in $PDI_TRF ; do
	echo $PDI_PAN -file $i
	$PDI_PAN -file $i
done

#############################################
## Full Job for Fact Tables (not recomended)
PDI_JOB="./load_dataset.kjb"
PDI_JOB_DIM="./load_dimensions.kjb"

#echo $PDI_KITCHEN -file $PDI_JOB_DIM
#$PDI_KITCHEN -file $PDI_JOB_DIM

#echo $PDI_KITCHEN -file $PDI_JOB
# $PDI_KITCHEN -file $PDI_JOB

