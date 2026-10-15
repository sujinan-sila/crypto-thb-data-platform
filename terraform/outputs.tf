output "resource_group" {
  value = azurerm_resource_group.rg.name
}

output "storage_account" {
  value = azurerm_storage_account.lake.name
}

output "key_vault" {
  value = azurerm_key_vault.kv.name
}

output "data_factory" {
  value = azurerm_data_factory.adf.name
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.uc.id
}

output "eventhub_namespace" {
  value = var.create_eventhubs ? azurerm_eventhub_namespace.ehns[0].name : null
}

output "databricks_url" {
  value = var.create_databricks ? "https://${azurerm_databricks_workspace.dbw[0].workspace_url}" : null
}

output "eh_send_connection" {
  value     = var.create_eventhubs ? azurerm_eventhub_authorization_rule.send[0].primary_connection_string : null
  sensitive = true
}

output "eh_listen_connection" {
  value     = var.create_eventhubs ? azurerm_eventhub_authorization_rule.listen[0].primary_connection_string : null
  sensitive = true
}
