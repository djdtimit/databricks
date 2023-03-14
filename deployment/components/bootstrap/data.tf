{% set inv = inventory.parameters %}

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
