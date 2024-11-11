#!/bin/bash

export FRONTEND_NAME=$(terraform output -raw frontend_function_name)
export GENERATOR_NAME=$(terraform output -raw generator_function_name)
export TERASORT_NAME=$(terraform output -raw terasort_function_name)
export ANALYTICS_NAME=$(terraform output -raw analytics_tc_function_name)
export ETL_NAME=$(terraform output -raw etl_function_name)

export AWS_PROFILE_NAME="default"