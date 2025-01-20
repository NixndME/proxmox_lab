package com.morpheusdata.proxmox.ve.sync


import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.providers.CloudProvider
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Account
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ImageType
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.model.VirtualImageLocation
import com.morpheusdata.model.projection.VirtualImageIdentityProjection
import com.morpheusdata.proxmox.ve.ProxmoxVePlugin
import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

@Slf4j
class VirtualImageSync {

    private Cloud cloud
    private MorpheusContext context
    private ProxmoxVePlugin plugin
    private HttpApiClient apiClient
    private CloudProvider cloudProvider
    private Map authConfig


    VirtualImageSync(ProxmoxVePlugin proxmoxVePlugin, Cloud cloud, HttpApiClient apiClient, CloudProvider cloudProvider) {
        this.@plugin = proxmoxVePlugin
        this.@cloud = cloud
        this.@apiClient = apiClient
        this.@context = proxmoxVePlugin.morpheus
        this.@cloudProvider = cloudProvider
        this.@authConfig = plugin.getAuthConfig(cloud)
    }



    def execute() {
        try {
            log.info "Execute VirtualImageSync STARTED: ${cloud.id}"
            def cloudItems = ProxmoxAPIComputeUtil.listTemplates(apiClient, authConfig).data
            log.info("Proxmox templates found: $cloudItems")

            Observable domainRecords = context.async.virtualImage.listIdentityProjections(new DataQuery().withFilter(
                    new DataFilter("account", cloud.account))
            )

            log.info("Creating VritaulImage sync task")
            SyncTask<VirtualImageIdentityProjection, Map, VirtualImage> syncTask = new SyncTask<>(domainRecords, cloudItems)
            syncTask.addMatchFunction { VirtualImageIdentityProjection domainObject, Map cloudItem ->
                domainObject.externalId == cloudItem.vmid.toString()
            }.onAdd { List<Map> newItems ->
                //addMissingVirtualImages(newItems)
            }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<VirtualImageIdentityProjection, Map>> updateItems ->
                Map<Long, SyncTask.UpdateItemDto<VirtualImageIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                return context.async.virtualImage.listById(updateItems?.collect { it.existingItem.id }).map { VirtualImage vi ->
                    return new SyncTask.UpdateItem<VirtualImage, Map>(existingItem: vi, masterItem: updateItemMap[vi.id].masterItem)
                } 
            }.onUpdate { List<SyncTask.UpdateItem<VirtualImage, Map>> updateItems ->
                //updateMatchedVirtualImages(updateItems)
            }.onDelete { removeItems ->
                //removeMissingVirtualImages(removeItems)
            }.start()
        } catch(e) {
            log.error "Error in VirtualImageSync execute : ${e}", e
        }
        log.debug "Execute VirtualImageSync COMPLETED: ${cloud.id}"
    }


    private addMissingVirtualImages(Collection<Map> addList) {
        log.info "addMissingVirtualImages ${addList?.size()}"

        def adds = []
        addList.each {
            log.debug("Creating virtual image: $it")
            VirtualImage virtImg = new VirtualImage(buildVirtualImageConfig(it))
            VirtualImageLocation virtImgLoc = buildLocationConfig(virtImg)
            virtImg.imageLocations = [virtImgLoc]
            adds << virtImg
        }

        log.debug "About to create ${adds.size()} virtualImages"
        context.async.virtualImage.create(adds, cloud).blockingGet()
    }


    private updateMatchedVirtualImages(List<SyncTask.UpdateItem<VirtualImage, Map>> updateItems) {
        log.info "updateMissingVirtualImages ${addList?.size()}"
        def saves = []
        for (def updateItem in updateItems) {
            def existingItem = updateItem.existingItem
            def cloudItem = updateItem.masterItem
            def saveIt = false

            log.info("Updating VirtualImage $existingItem.name")

            def existingLocation = existingItem.imageLocations.find{
                it.refType == 'ComputeZone' && it.refId == cloud.id
            }
            if (!existingLocation) {
                existingItem.imageLocations << new VirtualImageLocation(buildLocationConfig(existingItem))
                saveIt = true
            }
            //Add other update logic here...
            if (saveIt) {
                saves << existingItem
            }
        }
        if (saves) {
            context.async.virtualImage.save(saves, cloud).blockingGet()
        }


    }


    private Map buildLocationConfig(VirtualImage image) {
        return [
                virtualImage: image,
                code        : "nutanix.prism.image.${cloud.id}.${image.externalId}",
                internalId  : image.internalId,
                externalId  : image.externalId,
                imageName   : image.name,
                imageRegion : cloud.regionCode,
                isPublic    : false,
                refType     : 'ComputeZone',
                refId       : cloud.id
        ]
    }


    private buildVirtualImageConfig(Map cloudItem) {
        Account account = cloud.account
        def regionCode = cloud.regionCode

        def imageConfig = [
                account    : account,
                category   : "nutanix.prism.image.${cloud.id}",
                name       : cloudItem.status.name,
                code       : "nutanix.prism.image.${cloud.id}.${cloudItem.metadata.uuid}",
                imageType  : ImageType.qcow2,
                status     : 'Active',
                minDisk    : cloudItem.status.resources.size_bytes?.toLong(),
                isPublic   : false,
                remotePath : cloudItem.status.resources?.retrieval_uri_list?.getAt(0) ?: cloudItem.status.resources?.source_uri,
                externalId : cloudItem.metadata.uuid,
                imageRegion: regionCode,
                internalId : cloudItem.metadata.uuid,
                uniqueId   : cloudItem.metadata.uuid,
                bucketId   : cloudItem.status.resources?.current_cluster_reference_list?.getAt(0)?.uuid,
                systemImage: false
        ]

        return imageConfig
    }


    private removeMissingVirtualImages(List<VirtualImageIdentityProjection> removeItems) {
        log.info("Remove virtual images...")
        context.async.virtualImage.bulkRemove(removeItems).blockingGet()
    }
}