variable "subscription_id" {
  type = string
}

variable "location" {
  type        = string
  description = "Azure region; must also be an Azure Databricks region (eastasia for this subscription)"
}

variable "prefix" {
  type        = string
  description = "Short unique name, lowercase letters and digits only, e.g. cryptothb7"
}

# Both default to off so nothing that bills by the hour exists before it is needed.
variable "create_eventhubs" {
  type        = bool
  default     = false
  description = "Event Hubs Standard bills about 1 USD a day; turn on in Part 04"
}

variable "create_databricks" {
  type        = bool
  default     = false
  description = "The 14-day Databricks trial starts at creation; turn on in Part 05"
}
