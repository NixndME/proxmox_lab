package com.morpheusdata.proxmox.ve

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.providers.SecurityGroupProvider
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.OptionType
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.proxmox.ve.util.ProxmoxApiFirewallUtil
import groovy.util.logging.Slf4j

@Slf4j
class ProxmoxSecurityGroupProvider implements SecurityGroupProvider {
    ProxmoxVePlugin plugin
    MorpheusContext morpheus

    ProxmoxSecurityGroupProvider(ProxmoxVePlugin plugin, MorpheusContext morpheus) {
        this.plugin = plugin
        this.morpheus = morpheus
    }

    @Override
    Plugin getPlugin() {
        return plugin
    }

    @Override
    MorpheusContext getMorpheus() {
        return morpheus
    }

    @Override
    String getCode() {
        return 'proxmox-ve.security-group'
    }

    @Override
    String getName() {
        return 'Proxmox Security Groups'
    }

    @Override
    Collection<OptionType> getOptionTypes() {
        return []
    }

    ServiceResponse listGroups(Cloud cloud) {
        HttpApiClient client = new HttpApiClient()
        try {
            Map authConfig = plugin.getAuthConfig(cloud)
            return ProxmoxApiFirewallUtil.listClusterFirewallGroups(client, authConfig)
        } finally {
            client.shutdownClient()
        }
    }

    ServiceResponse listRules(Cloud cloud, ComputeServer server) {
        HttpApiClient client = new HttpApiClient()
        try {
            Map authConfig = plugin.getAuthConfig(cloud)
            String vmId = server.externalId
            String nodeName = server.parentServer?.name
            if(!vmId || !nodeName)
                return ServiceResponse.error('Missing VM id or node name')
            return ProxmoxApiFirewallUtil.listVmFirewallRules(client, authConfig, nodeName, vmId)
        } finally {
            client.shutdownClient()
        }
    }

    ServiceResponse addRule(Cloud cloud, ComputeServer server, Map ruleConfig) {
        HttpApiClient client = new HttpApiClient()
        try {
            Map authConfig = plugin.getAuthConfig(cloud)
            String vmId = server.externalId
            String nodeName = server.parentServer?.name
            if(!vmId || !nodeName)
                return ServiceResponse.error('Missing VM id or node name')
            return ProxmoxApiFirewallUtil.createVmFirewallRule(client, authConfig, nodeName, vmId, ruleConfig)
        } finally {
            client.shutdownClient()
        }
    }

    ServiceResponse removeRule(Cloud cloud, ComputeServer server, Integer rulePos) {
        HttpApiClient client = new HttpApiClient()
        try {
            Map authConfig = plugin.getAuthConfig(cloud)
            String vmId = server.externalId
            String nodeName = server.parentServer?.name
            if(!vmId || !nodeName)
                return ServiceResponse.error('Missing VM id or node name')
            return ProxmoxApiFirewallUtil.deleteVmFirewallRule(client, authConfig, nodeName, vmId, rulePos)
        } finally {
            client.shutdownClient()
        }
    }
}
