set -gx FRONTEND_NAME (terraform output -raw frontend_function_name)
set -gx GENERATOR_NAME (terraform output -raw generator_function_name)
set -gx TERASORT_NAME (terraform output -raw terasort_function_name)
set -gx ANALYTICS_NAME (terraform output -raw analytics_tc_function_name)
set -gx ETL_NAME (terraform output -raw etl_function_name)

set -x AWS_PROFILE_NAME "default"