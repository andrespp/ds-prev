#!/bin/bash
#
#
PDI_KITCHEN="/home/andre/Downloads/pentaho/data-integration-ce-5.0.1.A-stable/kitchen.sh"
PDI_JOB="./load_dataset.kjb"

echo $PDI_KITCHEN -file $PDI_JOB
$PDI_KITCHEN -file $PDI_JOB
