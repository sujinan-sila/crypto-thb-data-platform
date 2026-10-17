terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

data "azurerm_client_config" "me" {}

locals {
  tags = {
    project = "crypto-thb-data-platform"
  }
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-${var.prefix}"
  location = var.location
  tags     = local.tags
}

# ---------- storage: ADLS Gen2 ----------
resource "azurerm_storage_account" "lake" {
  name                     = "st${var.prefix}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # hierarchical namespace = ADLS Gen2
  min_tls_version          = "TLS1_2"
  tags                     = local.tags
}

# landing: raw files written by Data Factory; lakehouse: Delta tables of the catalog
resource "azurerm_storage_container" "landing" {
  name               = "landing"
  storage_account_id = azurerm_storage_account.lake.id
}

resource "azurerm_storage_container" "lakehouse" {
  name               = "lakehouse"
  storage_account_id = azurerm_storage_account.lake.id
}

# ---------- Event Hubs: Kafka endpoint ----------
resource "azurerm_eventhub_namespace" "ehns" {
  count               = var.create_eventhubs ? 1 : 0
  name                = "ehns-${var.prefix}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Standard" # Basic has no Kafka endpoint
  capacity            = 1
  tags                = local.tags
}

resource "azurerm_eventhub" "trades" {
  count             = var.create_eventhubs ? 1 : 0
  name              = "trades"
  namespace_id      = azurerm_eventhub_namespace.ehns[0].id
  partition_count   = 3
  message_retention = 3 # days Databricks can be off without losing data
}

# separate rights: the producer can only send, Databricks can only listen
resource "azurerm_eventhub_authorization_rule" "send" {
  count               = var.create_eventhubs ? 1 : 0
  name                = "producer-send"
  namespace_name      = azurerm_eventhub_namespace.ehns[0].name
  eventhub_name       = azurerm_eventhub.trades[0].name
  resource_group_name = azurerm_resource_group.rg.name
  send                = true
}

resource "azurerm_eventhub_authorization_rule" "listen" {
  count               = var.create_eventhubs ? 1 : 0
  name                = "databricks-listen"
  namespace_name      = azurerm_eventhub_namespace.ehns[0].name
  eventhub_name       = azurerm_eventhub.trades[0].name
  resource_group_name = azurerm_resource_group.rg.name
  listen              = true
}

# ---------- Key Vault: Bank of Thailand tokens for Data Factory ----------
resource "azurerm_key_vault" "kv" {
  name                       = "kv-${var.prefix}"
  resource_group_name        = azurerm_resource_group.rg.name
  location                   = azurerm_resource_group.rg.location
  tenant_id                  = data.azurerm_client_config.me.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  purge_protection_enabled   = false
  soft_delete_retention_days = 7
  tags                       = local.tags
}

# you can write secrets; Data Factory can only read them
resource "azurerm_role_assignment" "me_kv_officer" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.me.object_id
}

# ---------- Data Factory ----------
resource "azurerm_data_factory" "adf" {
  name                = "adf-${var.prefix}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }

  lifecycle {
    ignore_changes = [github_configuration] # Git is linked in ADF Studio
  }
}

resource "azurerm_role_assignment" "adf_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

resource "azurerm_role_assignment" "adf_kv" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

# ---------- Databricks ----------
resource "azurerm_databricks_workspace" "dbw" {
  count               = var.create_databricks ? 1 : 0
  name                = "dbw-${var.prefix}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium" # Premium features, free DBUs for 14 days
  tags                = local.tags
}

# the identity Unity Catalog uses to read and write ADLS, instead of storage keys
resource "azurerm_databricks_access_connector" "uc" {
  name                = "dbac-${var.prefix}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "uc_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}
