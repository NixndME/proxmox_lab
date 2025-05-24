package com.morpheusdata.proxmox.ve.sync

import com.morpheusdata.proxmox.ve.ProxmoxVePlugin
import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.Network
import com.morpheusdata.model.projection.NetworkIdentityProjection
import groovy.util.logging.Slf4j


@Slf4j
class NetworkSync {

    private Cloud cloud
    private MorpheusContext morpheusContext
    private ProxmoxVePlugin plugin
    private HttpApiClient apiClient
    private Map authConfig

    public NetworkSync(ProxmoxVePlugin proxmoxVePlugin, Cloud cloud, HttpApiClient apiClient) {
        this.@plugin = proxmoxVePlugin
        this.@cloud = cloud
        this.@morpheusContext = proxmoxVePlugin.morpheus
        this.@apiClient = apiClient
        this.@authConfig = plugin.getAuthConfig(cloud)
    }



    def execute() {
        try {

            log.debug "BEGIN: execute NetworkSync: ${cloud.id}"

            def cloudItemsResp = ProxmoxApiComputeUtil.listProxmoxNetworks(apiClient, authConfig)
            if(!cloudItemsResp.success) {
                log.error "NetworkSync error fetching networks: ${cloudItemsResp.msg}"
                return
            }
            def cloudItems = cloudItemsResp.data

            def domainRecords = morpheusContext.async.network.listIdentityProjections(
                    new DataQuery().withFilters([
                            new DataFilter('typeCode', 'proxmox-ve-bridge-network'),
                            new DataFilter('refId', cloud.id)
                    ])
            )

            SyncTask<NetworkIdentityProjection, Map, Network> syncTask = new SyncTask<>(domainRecords, cloudItems)

            syncTask.addMatchFunction { NetworkIdentityProjection domainObject, Map network ->
                domainObject?.externalId == network?.iface
            }.onAdd { itemsToAdd ->
                addMissingNetworks(cloud, itemsToAdd)
            }.onDelete { itemsToDelete ->
                removeMissingNetworks(itemsToDelete)
            }.withLoadObjectDetails { updateItems ->
                Map<Long, SyncTask.UpdateItemDto<NetworkIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                return morpheusContext.async.network.listById(updateItems?.collect { it.existingItem.id }).map { Network network ->
                    return new SyncTask.UpdateItem<Network, Map>(existingItem: network, masterItem: updateItemMap[network.id].masterItem)
                }             
            }.onUpdate { itemsToUpdate ->
                updateMatchedNetworks(itemsToUpdate)
            }.start()
        } catch(e) {
            log.error "Error in NetworkSync execute : ${e}", e
        }
        log.debug "Execute NetworkSync COMPLETED: ${cloud.id}"
    }


    private addMissingNetworks(Cloud cloud, Collection addList) {
        log.debug "addMissingNetworks: ${cloud} ${addList.size()}"
        def networkType = morpheusContext.async.network.type.list(new DataQuery().withFilter('code', 'proxmox-ve-bridge-network')).blockingFirst()
        def networks = []
        try {
            for(cloudItem in addList) {
                log.debug("Adding Network: $cloudItem")
                networks << new Network(
                        externalId   : cloudItem.iface,
                        name         : cloudItem.iface,
                        cloud        : cloud,
                        displayName  : cloudItem.name,
                        description  : cloudItem.networkAddress,
                        cidr         : cloudItem.networkAddress,
                        status       : cloudItem.active,
                        code         : "proxmox.network.${cloudItem.iface}",
                        typeCode     : networkType.code,
                        type         : networkType,
                        owner        : cloud.account,
                        tenantName   : cloud.account.name,
                        refType      : "ComputeZone",
                        refId        : cloud.id,
                        networkServer: cloud.networkServer,
                        providerId   : "",
                        gateway      : cloudItem.gateway,
                        dnsPrimary   : cloudItem.gateway,
                        dnsSecondary : "8.8.8.8",
                        dhcpServer   : true,


                )
            }
            log.debug("Saving ${networks.size()} Networks")
            if (!morpheusContext.async.network.bulkCreate(networks).blockingGet()){
                log.error "Error saving new networks!"
            }

        } catch(e) {
            log.error "Error in creating networks: ${e}", e
        }
    }


    private updateMatchedNetworks(List<SyncTask.UpdateItem<Network, Map>> updateItems) {
        log.debug("Updating ${updateItems.size()} Networks ...")
        def saveList = []
        updateItems.each { updateItem ->
            Network existingItem = updateItem.existingItem
            Map cloudItem = updateItem.masterItem
            boolean save = false

            if(existingItem.name != cloudItem.iface) {
                existingItem.name = cloudItem.iface
                save = true
            }
            if(existingItem.displayName != cloudItem.name) {
                existingItem.displayName = cloudItem.name
                save = true
            }
            if(existingItem.cidr != cloudItem.networkAddress) {
                existingItem.cidr = cloudItem.networkAddress
                existingItem.description = cloudItem.networkAddress
                save = true
            }
            if(existingItem.gateway != cloudItem.gateway) {
                existingItem.gateway = cloudItem.gateway
                existingItem.dnsPrimary = cloudItem.gateway
                save = true
            }
            if(existingItem.status != cloudItem.active) {
                existingItem.status = cloudItem.active
                save = true
            }

            if(save) {
                saveList << existingItem
            }
        }

        if(saveList) {
            morpheusContext.async.network.bulkSave(saveList).blockingGet()
        }
    }


    private removeMissingNetworks(List<NetworkIdentityProjection> removeItems) {
        log.info("Remove Networks...")
        morpheusContext.async.network.bulkRemove(removeItems).blockingGet()
    }
}