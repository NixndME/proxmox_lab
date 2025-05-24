import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.CloudPool
import com.morpheusdata.model.projection.CloudPoolIdentityProjection
import com.morpheusdata.proxmox.ve.ProxmoxVePlugin
import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil
import groovy.util.logging.Slf4j

@Slf4j
class ClusterSync {

    private Cloud cloud
    private MorpheusContext context
    private ProxmoxVePlugin plugin
    private HttpApiClient apiClient
    private Map authConfig

    ClusterSync(ProxmoxVePlugin plugin, Cloud cloud, HttpApiClient apiClient) {
        this.@plugin = plugin
        this.@cloud = cloud
        this.@context = plugin.morpheus
        this.@apiClient = apiClient
        this.@authConfig = plugin.getAuthConfig(cloud)
    }

    def execute() {
        log.debug "Execute ClusterSync for cloud ${cloud?.id}"
        def resp = ProxmoxApiComputeUtil.listProxmoxClusters(apiClient, authConfig)
        if(!resp.success) {
            log.error "Error listing clusters: ${resp.msg}"
            return
        }
        def cloudItems = resp.data ?: []
        def domainRecords = context.async.cloud.pool.listSyncProjections(cloud.id)

        SyncTask<CloudPoolIdentityProjection, Map, CloudPool> syncTask = new SyncTask<>(domainRecords, cloudItems)
        syncTask.addMatchFunction { CloudPoolIdentityProjection domain, Map item ->
            domain.externalId == item.name
        }.onAdd { addItems ->
            addClusters(addItems)
        }.onDelete { removeItems ->
            removeClusters(removeItems)
        }.withLoadObjectDetails { updateItems ->
            Map<Long, SyncTask.UpdateItemDto<CloudPoolIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it] }
            context.async.cloud.pool.listById(updateItems.collect { it.existingItem.id }).map { CloudPool pool ->
                new SyncTask.UpdateItem<CloudPool, Map>(existingItem: pool, masterItem: updateItemMap[pool.id].masterItem)
            }
        }.onUpdate { updateItems ->
            updateClusters(updateItems)
        }.start()
    }

    private addClusters(Collection<Map> addItems) {
        def adds = []
        addItems?.each { Map item ->
            CloudPool pool = new CloudPool(
                cloud      : cloud,
                account    : cloud.owner,
                name       : item.name,
                externalId : item.name,
                description: (item.roles ?: []).join(', '),
                refType    : 'ComputeZone',
                refId      : cloud.id,
                code       : "proxmox.cluster.${item.name}"
            )
            adds << pool
        }
        if(adds)
            context.async.cloud.pool.create(adds).blockingGet()
    }

    private updateClusters(List<SyncTask.UpdateItem<CloudPool, Map>> updateItems) {
        updateItems?.each { updateItem ->
            CloudPool pool = updateItem.existingItem
            Map item = updateItem.masterItem
            boolean save = false
            if(pool.name != item.name) {
                pool.name = item.name
                save = true
            }
            String desc = (item.roles ?: []).join(', ')
            if(pool.description != desc) {
                pool.description = desc
                save = true
            }
            if(save)
                context.async.cloud.pool.save([pool]).blockingGet()
        }
    }

    private removeClusters(List<CloudPoolIdentityProjection> removeItems) {
        if(removeItems)
            context.async.cloud.pool.bulkRemove(removeItems).blockingGet()
    }
}
