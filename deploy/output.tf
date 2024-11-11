# VPC
output "vpc_id" {
  description = "The ID of the VPC"
  value       = module.vpc.vpc_id
}

output "bastion_node_ip_address" {
  value = aws_instance.bastion_node.public_ip
}

output "placement_group_id" {
  value = aws_placement_group.cluster_group.id
}

output "internal_sg_id" {
  description = "The ID of the cluster security group"
  value       = module.sg.this_security_group_id
}

output "all_en4s_controller_id" {
  description = "The ID of all the En4S controllers"
  value       = aws_instance.en4s_controller[*].private_ip
}

output "all_en4s_storage_id" {
  description = "The ID of all the En4S servers"
  value       = aws_instance.en4s_storage[*].private_ip
}

output "terasort_function_name" {
  description = "Terasort Function Name"
  value       = module.terasort.lambda_function_name
}

output "analytics_ps_function_name" {
  description = "Analytics Process Function Name"
  value       = module.analytics_process.lambda_function_name
}

output "analytics_tc_function_name" {
  description = "Analytics Function Name"
  value       = module.analytics_transcode.lambda_function_name
}
output "etl_function_name" {
  description = "ETL Function Name"
  value       = module.etl.lambda_function_name
}