#!/bin/bash
#
#
PDI_KITCHEN="/home/andre/Downloads/pentaho/data-integration-ce-5.0.1.A-stable/kitchen.sh"
PDI_JOB="./load_dataset.kjb"

PDI_PAN="/home/andre/Downloads/pentaho/data-integration-ce-5.0.1.A-stable/pan.sh"
#PDI_TRF="./fato_aposentadoria.ktr"
#PDI_TRF="./fato_auxilio.ktr"
PDI_TRF="./fato_pensao2.ktr"
#PDI_TRF="./fato_pensao.ktr"

# ALL IN ONE
#echo $PDI_KITCHEN -file $PDI_JOB
# $PDI_KITCHEN -file $PDI_JOB

echo $PDI_PAN -file $PDI_TRF
$PDI_PAN -file $PDI_TRF
