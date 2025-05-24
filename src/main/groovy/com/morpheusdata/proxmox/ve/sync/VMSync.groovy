package com.morpheusdata.proxmox.ve.sync


import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.providers.CloudProvider
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeCapacityInfo
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.OsType
import com.morpheusdata.model.projection.ComputeServerIdentityProjection
import com.morpheusdata.proxmox.ve.ProxmoxVePlugin
import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil
import groovy.json.JsonOutput
import groovy.util.logging.Slf4j

@Slf4j
class VMSync {

    private Cloud cloud
    private MorpheusContext context
    private ProxmoxVePlugin plugin
    private HttpApiClient apiClient
    private CloudProvider cloudProvider
    private Map authConfig


    VMSync(ProxmoxVePlugin proxmoxVePlugin, Cloud cloud, HttpApiClient apiClient, CloudProvider cloudProvider) {
        this.@plugin = proxmoxVePlugin
        this.@cloud = cloud
        this.@apiClient = apiClient
        this.@context = proxmoxVePlugin.morpheus
        this.@cloudProvider = cloudProvider
        this.@authConfig = plugin.getAuthConfig(cloud)
    }



    def execute() {
        try {
            log.debug "Execute VMSync STARTED: ${cloud.id}"
            def cloudItems = ProxmoxApiComputeUtil.listVMs(apiClient, authConfig).data
            def domainRecords = context.async.computeServer.listIdentityProjections(cloud.id, null).filter {
                it.computeServerTypeCode in ['proxmox-qemu-vm-unmanaged', 'proxmox-qemu-vm']
            }

            log.debug("VM cloudItems: ${cloudItems.collect { it.toString() }}")
            log.debug("VM domainObjects: ${domainRecords.map { "${it.externalId} - ${it.name}" }.toList().blockingGet()}")

            SyncTask<ComputeServerIdentityProjection, Map, ComputeServer> syncTask = new SyncTask<>(domainRecords, cloudItems)
            syncTask.addMatchFunction { ComputeServerIdentityProjection domainObject, Map cloudItem ->

                domainObject.externalId == cloudItem.vmid.toString()
            }.onAdd { itemsToAdd ->
                addMissingVirtualMachines(cloud, itemsToAdd)
            }.onDelete { itemsToDelete ->
                removeMissingVMs(itemsToDelete)
            }.withLoadObjectDetails { updateItems ->
                Map<Long, SyncTask.UpdateItemDto<ComputeServerIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                return context.async.computeServer.listById(updateItems?.collect { it.existingItem.id }).map { ComputeServer server ->
                    return new SyncTask.UpdateItem<ComputeServer, Map>(existingItem: server, masterItem: updateItemMap[server.id].masterItem)
                } 
            }.onUpdate { itemsToUpdate ->
                updateMatchingVMs(itemsToUpdate)
            }.start()
        } catch(e) {
            log.error "Error in VMSync execute : ${e}", e
        }
        log.debug "Execute VMSync COMPLETED: ${cloud.id}"
    }


    private void addMissingVirtualMachines(Cloud cloud, Collection items) {
        log.info("Adding ${items.size()} new VMs for Proxmox cloud ${cloud.name}")

        def newVMs = []

        // Map available Proxmox hosts by their externalId (node name) so VMs can be linked to the correct host
        def hostIdentitiesMap = context.async.computeServer.listIdentityProjections(cloud.id, null).filter {
            it.computeServerTypeCode == 'proxmox-ve-node'
        }.toMap { it.externalId }.blockingGet()

        def computeServerType = cloudProvider.computeServerTypes.find {
            it.code == 'proxmox-qemu-vm-unmanaged'
        }

        items.each { Map cloudItem ->
            log.debug("VMSync.addMissingVirtualMachines - Creating ComputeServer for cloudItem: name=${cloudItem.name}, vmid=${cloudItem.vmid}")
            def configMap = [:]
            // Disks
            def totalDiskSizeBytes = cloudItem.disks?.sum { it.sizeBytes ?: 0L } ?: 0L
            configMap.proxmoxDisks = JsonOutput.toJson(cloudItem.disks ?: [])

            // Network Interfaces
            configMap.proxmoxNics = JsonOutput.toJson(cloudItem.networkInterfaces ?: [])

            // QEMU Agent
            configMap.qemuAgentStatus = cloudItem.qemuAgent?.status ?: 'unknown'
            configMap.qemuAgentData = cloudItem.qemuAgent?.data ? JsonOutput.toJson(cloudItem.qemuAgent.data) : null
            configMap.qemuAgentRawInterfaces = cloudItem.qemuAgent?.networkInterfaces ? JsonOutput.toJson(cloudItem.qemuAgent.networkInterfaces) : null
            // VM Tags
            configMap.proxmoxTags = JsonOutput.toJson(cloudItem.tags ?: [])

            def newVM = new ComputeServer(
                account          : cloud.account,
                externalId       : cloudItem.vmid.toString(),
                name             : cloudItem.name ?: "VM ${cloudItem.vmid}",
                externalIp       : cloudItem.ip ?: "0.0.0.0",
                internalIp       : cloudItem.ip ?: "0.0.0.0",
                sshHost          : cloudItem.ip ?: "0.0.0.0",
                sshUsername      : 'root', // Default, may not always be correct
                provision        : false,
                cloud            : cloud,
                lvmEnabled       : false, // Proxmox specific, generally false for Morpheus context
                managed          : false, // This is for unmanaged VMs
                serverType       : 'vm',
                status           : 'provisioned', // Initial status in Morpheus
                uniqueId         : cloudItem.vmid.toString(),
                powerState       : (cloudItem.status == 'running' || cloudItem.status == 'online') ? ComputeServer.PowerState.on : ComputeServer.PowerState.off,
                maxMemory        : cloudItem.maxmem ?: 0L, // From cluster/resources (bytes)
                maxCores         : cloudItem.maxCores ?: 1L, // Calculated in listVMs
                coresPerSocket   : cloudItem.coresPerSocket ?: 1L, // Calculated in listVMs
                maxStorage       : totalDiskSizeBytes, // Sum of all virtual disks
                // usedStorage will be set by updateMachineMetrics from cloudItem.disk if available
                parentServer     : hostIdentitiesMap[cloudItem.node],
                osType           : cloudItem.qemuAgent?.data?.ostype ? (cloudItem.qemuAgent.data.ostype.toLowerCase().contains('windows') ? 'windows' : cloudItem.qemuAgent.data.ostype) : 'unknown',
                serverOs         : cloudItem.qemuAgent?.data?.id ? new OsType(code: cloudItem.qemuAgent.data.id.toLowerCase(), name: cloudItem.qemuAgent.data.name, vendor: cloudItem.qemuAgent.data.id) : new OsType(code: 'unknown'),
                category         : "proxmox.ve.vm.${cloud.id}",
                computeServerType: computeServerType,
                configMap        : configMap
            )
            newVMs << newVM
        }
        if(newVMs) {
            context.async.computeServer.bulkCreate(newVMs).blockingGet()
        }
    }


    private updateMatchingVMs(List<SyncTask.UpdateItem<ComputeServer, Map>> updateItems) {
        for (def updateItem in updateItems) {
            def existingItem = updateItem.existingItem
            def cloudItem = updateItem.masterItem // This is the enriched map from listVMs
            log.debug("VMSync.updateMatchingVMs - Updating ComputeServer: id=${existingItem.id}, name=${existingItem.name}, externalId=${existingItem.externalId} with cloudItem: name=${cloudItem.name}, vmid=${cloudItem.vmid}")
            def doSave = false

            // Initialize configMap if null
            if (existingItem.configMap == null) {
                existingItem.configMap = [:]
            }

            // Update basic properties
            if (cloudItem.name && cloudItem.name != existingItem.name) {
                existingItem.name = cloudItem.name
                doSave = true
            }
            if (cloudItem.ip && cloudItem.ip != existingItem.externalIp) {
                existingItem.externalIp = cloudItem.ip
                existingItem.internalIp = cloudItem.ip // Assuming same for now
                existingItem.sshHost = cloudItem.ip
                doSave = true
            }
            def newPowerState = (cloudItem.status == 'running' || cloudItem.status == 'online') ? ComputeServer.PowerState.on : ComputeServer.PowerState.off
            if (newPowerState != existingItem.powerState) {
                existingItem.powerState = newPowerState
                doSave = true
            }
            if (cloudItem.maxCores && cloudItem.maxCores != existingItem.maxCores) {
                 existingItem.maxCores = cloudItem.maxCores
                 doSave = true
            }
            if (cloudItem.coresPerSocket && cloudItem.coresPerSocket != existingItem.coresPerSocket) {
                 existingItem.coresPerSocket = cloudItem.coresPerSocket
                 doSave = true
            }
             if (cloudItem.maxmem && cloudItem.maxmem != existingItem.maxMemory) {
                 existingItem.maxMemory = cloudItem.maxmem
                 doSave = true
            }
            
            // Disks
            def totalDiskSizeBytes = cloudItem.disks?.sum { it.sizeBytes ?: 0L } ?: 0L
            if (totalDiskSizeBytes != existingItem.maxStorage) {
                existingItem.maxStorage = totalDiskSizeBytes
                doSave = true
            }
            def newDisksJson = JsonOutput.toJson(cloudItem.disks ?: [])
            if (newDisksJson != existingItem.configMap.proxmoxDisks) {
                existingItem.configMap.proxmoxDisks = newDisksJson
                doSave = true
            }

            // Network Interfaces
            def newNicsJson = JsonOutput.toJson(cloudItem.networkInterfaces ?: [])
            if (newNicsJson != existingItem.configMap.proxmoxNics) {
                existingItem.configMap.proxmoxNics = newNicsJson
                doSave = true
            }

            def newTagsJson = JsonOutput.toJson(cloudItem.tags ?: [])
            if (newTagsJson != existingItem.configMap.proxmoxTags) {
                existingItem.configMap.proxmoxTags = newTagsJson
                doSave = true
            }

            // QEMU Agent
            def newAgentStatus = cloudItem.qemuAgent?.status ?: 'unknown'
            if (newAgentStatus != existingItem.configMap.qemuAgentStatus) {
                existingItem.configMap.qemuAgentStatus = newAgentStatus
                doSave = true
            }
            def newAgentDataJson = cloudItem.qemuAgent?.data ? JsonOutput.toJson(cloudItem.qemuAgent.data) : null
            if (newAgentDataJson != existingItem.configMap.qemuAgentData) {
                existingItem.configMap.qemuAgentData = newAgentDataJson
                doSave = true
            }
            def newAgentNicsJson = cloudItem.qemuAgent?.networkInterfaces ? JsonOutput.toJson(cloudItem.qemuAgent.networkInterfaces) : null
             if (newAgentNicsJson != existingItem.configMap.qemuAgentRawInterfaces) {
                existingItem.configMap.qemuAgentRawInterfaces = newAgentNicsJson
                doSave = true
            }

            // OS Type (if agent provides it and it's different)
            def newOsTypeCode = cloudItem.qemuAgent?.data?.id?.toLowerCase() ?: 'unknown'
            def newOsTypeName = cloudItem.qemuAgent?.data?.name ?: 'Unknown OS'
            if (existingItem.serverOs == null || newOsTypeCode != existingItem.serverOs.code) {
                existingItem.osType = cloudItem.qemuAgent?.data?.ostype ? (cloudItem.qemuAgent.data.ostype.toLowerCase().contains('windows') ? 'windows' : cloudItem.qemuAgent.data.ostype) : 'unknown'
                existingItem.serverOs = new OsType(code: newOsTypeCode, name: newOsTypeName, vendor: cloudItem.qemuAgent?.data?.id) // Assuming vendor can be the ID
                doSave = true
            }


            if (doSave) {
                context.async.computeServer.bulkSave([existingItem]).blockingGet()
            }

            // Update metrics using the new detailed info
            // cloudItem.disk is usedBytes from Proxmox 'status' or 'cluster/resources' (where it's confusingly called maxdisk)
            // cloudItem.maxmem is total memory in bytes from Proxmox 'status' or 'cluster/resources'
            // cloudItem.mem is used memory in bytes from Proxmox 'status' or 'cluster/resources'
            // cloudItem.maxCores is already calculated based on sockets and cores
            updateMachineMetrics(
                existingItem,
                cloudItem.maxCores,    // maxCores (already calculated number of cores)
                totalDiskSizeBytes,    // maxStorage (sum of all virtual disks)
                cloudItem.disk,        // usedStorage (Proxmox `disk` field from status, in bytes)
                cloudItem.maxmem,      // maxMemory (in bytes)
                cloudItem.mem,         // usedMemory (in bytes)
                cloudItem.maxCores,    // maxCpu (using maxCores as it represents total vCPUs)
                newPowerState
            )
        }
    }


    private removeMissingVMs(List<ComputeServerIdentityProjection> removeItems) {
        log.info("Remove ${removeItems.size()} VMs...")
        context.async.computeServer.bulkRemove(removeItems).blockingGet()
    }


    private updateMachineMetrics(ComputeServer server, Long maxCores, Long maxStorage, Long usedStorage, Long maxMemory, Long usedMemory, Long maxCpu, ComputeServer.PowerState status) {
        log.debug "updateMachineMetrics for ${server}"
        try {
            def updates = !server.getComputeCapacityInfo()
            ComputeCapacityInfo capacityInfo = server.getComputeCapacityInfo() ?: new ComputeCapacityInfo()

            if(capacityInfo.maxCores != maxCores || server.maxCores != maxCores) {
                capacityInfo.maxCores = maxCores
                server?.maxCores = maxCores
                updates = true
            }

            if(capacityInfo.maxStorage != maxStorage || server.maxStorage != maxStorage) {
                capacityInfo.maxStorage = maxStorage
                server?.maxStorage = maxStorage
                updates = true
            }

            if(capacityInfo.usedStorage != usedStorage || server.usedStorage != usedStorage) {
                capacityInfo.usedStorage = usedStorage
                server?.usedStorage = usedStorage
                updates = true
            }

            if(capacityInfo.maxMemory != maxMemory || server.maxMemory != maxMemory) {
                capacityInfo?.maxMemory = maxMemory
                server?.maxMemory = maxMemory
                updates = true
            }

            if(capacityInfo.usedMemory != usedMemory || server.usedMemory != usedMemory) {
                capacityInfo?.usedMemory = usedMemory
                server?.usedMemory = usedMemory
                updates = true
            }

            if(capacityInfo.maxCpu != maxCpu || server.usedCpu != maxCpu) {
                capacityInfo?.maxCpu = maxCpu
                server?.usedCpu = maxCpu
                updates = true
            }

            def powerState = capacityInfo.maxCpu > 0 ? ComputeServer.PowerState.on : ComputeServer.PowerState.off
            if(server.powerState != powerState) {
                server.powerState = powerState
                updates = true
            }

            if(updates == true) {
                server.capacityInfo = capacityInfo
                context.async.computeServer.bulkSave([server]).blockingGet()
            }
        } catch(e) {
            log.warn("error updating host stats: ${e}", e)
        }
    }

}