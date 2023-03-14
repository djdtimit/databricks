{% set inv = inventory.parameters %}
# Only use team members which have role of admin in bootstrap process.
data "azuread_users" "users" {
  user_principal_names = {{ inv.team.datafoundation_a.members | selectattr("role", "equalto", "admin") | map(attribute='email') | list | replace("'", "\"") }}
}

{% if inv.environment == "shared" %}
data "azurerm_resource_group" "infrastructure" {
  name = "{{ inv.azure.resource_groups.prefix }}-infrastructure-{{ inv.azure.location_short }}-{{ inv.environment }}"
}

{% else %}
{% for group in inv.azure.resource_groups.groups %}
data "azurerm_resource_group" "{{ group }}" {
  name = "{{ inv.azure.resource_groups.prefix }}-{{ group }}-{{ inv.azure.location_short }}-{{ inv.environment }}"
}

{% endfor %}
{% endif %}

# Generate a map containing the current values of the default tags provided by the ccc
locals {
  default_tags = {
    "project" = "{{ inv.project_prefix }}", 
    "datadog" = "true", 
    "environment" = "{{ inv.environment }}", 
    "project_product" = data.azurerm_resource_group.infrastructure.tags["project_product"],
    "cost_unit" = data.azurerm_resource_group.infrastructure.tags["cost_unit"],
    "project_resource_group" = data.azurerm_resource_group.infrastructure.tags["project_resource_group"],
    "cost_webhook" = data.azurerm_resource_group.infrastructure.tags["cost_webhook"],
    "sc_alerts_webhook" = data.azurerm_resource_group.infrastructure.tags["sc_alerts_webhook"]
  }
}
