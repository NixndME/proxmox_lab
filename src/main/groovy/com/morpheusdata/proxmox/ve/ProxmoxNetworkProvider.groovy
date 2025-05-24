package com.morpheusdata.proxmox.ve

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.providers.NetworkProvider
import com.morpheusdata.core.providers.CloudInitializationProvider
import com.morpheusdata.core.providers.SecurityGroupProvider
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.Network
import com.morpheusdata.model.NetworkServer
import com.morpheusdata.model.NetworkServerType
import com.morpheusdata.model.NetworkSubnet
import com.morpheusdata.model.NetworkType
import com.morpheusdata.model.OptionType
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil
import com.morpheusdata.proxmox.ve.util.ProxmoxSslUtil
import org.apache.http.entity.ContentType
import groovy.util.logging.Slf4j

@Slf4j
class ProxmoxNetworkProvider implements NetworkProvider, CloudInitializationProvider {

	public static final String CLOUD_PROVIDER_CODE = 'proxmox-ve.cloud'

	ProxmoxVePlugin plugin
	MorpheusContext morpheus
	SecurityGroupProvider securityGroupProvider

	final String code = 'proxmox-ve.network'
	final String name = 'Proxmox Network'
	final String description = 'Proxmox Network Provider'


	ProxmoxNetworkProvider(ProxmoxVePlugin plugin, MorpheusContext morpheusContext) {
		this.plugin = plugin
		this.morpheus = morpheusContext
	}


	@Override
	String getNetworkServerTypeCode() {
		return 'proxmox-ve.network'
	}

	/**
	 * The CloudProvider code that this NetworkProvider should be attached to.
	 * When this NetworkProvider is registered with Morpheus, all Clouds that match this code will have a
	 * NetworkServer of this type attached to them. Network actions will then be handled via this provider.
	 * @return String Code of the Cloud type
	 */
	@Override
	String getCloudProviderCode() {
		return CLOUD_PROVIDER_CODE
	}

	/**
	 * Provides a Collection of NetworkTypes that can be managed by this provider
	 * @return Collection of NetworkType
	 */
	@Override
	Collection<NetworkType> getNetworkTypes() {
		NetworkType bridgeNetwork = new NetworkType([
				code              : 'proxmox-ve-bridge-network',
				externalType      : 'LinuxBridge',
				cidrEditable      : true,
				dhcpServerEditable: true,
				dnsEditable       : true,
				gatewayEditable   : true,
				vlanIdEditable    : false,
				canAssignPool     : true,
				name              : 'Proxmox VE Bridge Network',
				hasNetworkServer  : true,
				creatable: true
		])

		return [bridgeNetwork]

	}


	@Override
	Collection<OptionType> getOptionTypes() {
		return null
	}

	@Override
	Collection<OptionType> getSecurityGroupOptionTypes() {
		return []
	}

	@Override
	ServiceResponse initializeProvider(Cloud cloud) {
                log.info("Initializing network provider for ${cloud.name}")
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			NetworkServer networkServer = new NetworkServer(
				name: cloud.name,
				type: new NetworkServerType(code:"proxmox-ve.network")
			)
			morpheus.integration.registerCloudIntegration(cloud.id, networkServer).blockingGet()
			rtn.success = true
		} catch (Exception e) {
			rtn.success = false
			log.error("initializeProvider error: {}", e, e)
		}

		return rtn
	}

	@Override
	ServiceResponse deleteProvider(Cloud cloud) {
		log.info("Deleting network provider for ${cloud.name}")
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			// cleanup is done by type, so we do not need to load the record
			// NetworkServer networkServer = morpheusContext.services.networkServer.find(new DataQuery().withFilters([new DataFilter('type.code', "amazon"), new DataFilter('zoneId', cloud.id)]))
			// NetworkServer networkServer = cloud.networkServer // this works too, ha
			NetworkServer networkServer = new NetworkServer(
				name: cloud.name,
				type: new NetworkServerType(code:"proxmox-ve.network")
			)
			morpheus.integration.deleteCloudIntegration(cloud.id, networkServer).blockingGet()
			rtn.success = true
		} catch (Exception e) {
			rtn.success = false
			log.error("deleteProvider error: {}", e, e)
		}

		return rtn
	}

	/**
	 * Creates the Network submitted
	 * @param network Network information
	 * @param opts additional configuration options
	 * @return ServiceResponse
        */
        @Override
        ServiceResponse createNetwork(Network network, Map opts) {
                log.info("NVR: CREATE NETWORK ${network?.name}")
                ServiceResponse rtn = ServiceResponse.prepare()
                HttpApiClient client = new HttpApiClient()
                try {
                        Cloud cloud = network.cloud
                        if(!cloud) {
                                rtn.success = false
                                rtn.msg = "Network cloud is missing"
                                return rtn
                        }

                        Map authConfig = plugin.getAuthConfig(cloud)

                        ServiceResponse hostsResponse = ProxmoxApiComputeUtil.listProxmoxHypervisorHosts(client, authConfig)
                        if(!hostsResponse.success) {
                                rtn.success = false
                                rtn.msg = "Failed to list Proxmox nodes: ${hostsResponse.msg}"
                                return rtn
                        }

                        boolean allSuccess = true
                        hostsResponse.data.each { host ->
                                def tokenResp = ProxmoxApiComputeUtil.getApiV2Token(authConfig)
                                if(!tokenResp.success) {
                                        allSuccess = false
                                        rtn.msg = tokenResp.msg
                                        return
                                }
                                def tokenCfg = tokenResp.data
                                def body = [iface:(network.externalId ?: network.name), type:'bridge']
                                if(network.cidr)
                                        body.cidr = network.cidr
                                if(network.gateway)
                                        body.gateway = network.gateway

                                def optsReq = new HttpApiClient.RequestOptions(
                                        headers:[
                                                'Content-Type':'application/json',
                                                'Cookie':"PVEAuthCookie=${tokenCfg.token}",
                                                'CSRFPreventionToken': tokenCfg.csrfToken
                                        ],
                                        body: body,
                                        contentType: ContentType.APPLICATION_JSON,
                                        ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
                                )

                                def results = client.callJsonApi(authConfig.apiUrl,
                                        "${authConfig.v2basePath}/nodes/${host.node}/network",
                                        null, null, optsReq, 'POST')

                                if(!results.success) {
                                        allSuccess = false
                                        rtn.msg = "Failed creating network on ${host.node}: ${results.msg ?: results.content}"
                                }
                        }

                        rtn.success = allSuccess
                        if(allSuccess)
                                rtn.data = network
                } catch(Exception e) {
                        rtn.success = false
                        rtn.msg = "Error creating network: ${e.message}"
                        log.error("createNetwork error", e)
                } finally {
                        client.shutdownClient()
                }
                return rtn
        }

	/**
	 * Updates the Network submitted
	 * @param network Network information
	 * @param opts additional configuration options
	 * @return ServiceResponse
	 */
        @Override
        ServiceResponse<Network> updateNetwork(Network network, Map opts) {
                log.info("NVR: UPDATE NETWORK ${network?.name}")
                ServiceResponse rtn = ServiceResponse.prepare()
                HttpApiClient client = new HttpApiClient()
                try {
                        Cloud cloud = network.cloud
                        if(!cloud) {
                                rtn.success = false
                                rtn.msg = "Network cloud is missing"
                                return rtn
                        }

                        Map authConfig = plugin.getAuthConfig(cloud)

                        ServiceResponse hostsResponse = ProxmoxApiComputeUtil.listProxmoxHypervisorHosts(client, authConfig)
                        if(!hostsResponse.success) {
                                rtn.success = false
                                rtn.msg = "Failed to list Proxmox nodes: ${hostsResponse.msg}"
                                return rtn
                        }

                        boolean allSuccess = true
                        hostsResponse.data.each { host ->
                                def tokenResp = ProxmoxApiComputeUtil.getApiV2Token(authConfig)
                                if(!tokenResp.success) {
                                        allSuccess = false
                                        rtn.msg = tokenResp.msg
                                        return
                                }
                                def tokenCfg = tokenResp.data
                                def body = [:]
                                if(network.cidr)
                                        body.cidr = network.cidr
                                if(network.gateway)
                                        body.gateway = network.gateway

                                def optsReq = new HttpApiClient.RequestOptions(
                                        headers:[
                                                'Content-Type':'application/json',
                                                'Cookie':"PVEAuthCookie=${tokenCfg.token}",
                                                'CSRFPreventionToken': tokenCfg.csrfToken
                                        ],
                                        body: body,
                                        contentType: ContentType.APPLICATION_JSON,
                                        ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
                                )

                                def results = client.callJsonApi(authConfig.apiUrl,
                                        "${authConfig.v2basePath}/nodes/${host.node}/network/${network.externalId ?: network.name}",
                                        null, null, optsReq, 'PUT')

                                if(!results.success) {
                                        allSuccess = false
                                        rtn.msg = "Failed updating network on ${host.node}: ${results.msg ?: results.content}"
                                }
                        }

                        rtn.success = allSuccess
                        if(allSuccess)
                                rtn.data = network
                } catch(Exception e) {
                        rtn.success = false
                        rtn.msg = "Error updating network: ${e.message}"
                        log.error("updateNetwork error", e)
                } finally {
                        client.shutdownClient()
                }
                return rtn
        }

	/**
	 * Deletes the Network submitted
	 * @param network Network information
	 * @return ServiceResponse
	 */
        @Override
        ServiceResponse deleteNetwork(Network network, Map opts) {
                log.info("NVR: DELETE NETWORK ${network?.name}")
                ServiceResponse rtn = ServiceResponse.prepare()
                HttpApiClient client = new HttpApiClient()
                try {
                        Cloud cloud = network.cloud
                        if(!cloud) {
                                rtn.success = false
                                rtn.msg = "Network cloud is missing"
                                return rtn
                        }

                        Map authConfig = plugin.getAuthConfig(cloud)

                        ServiceResponse hostsResponse = ProxmoxApiComputeUtil.listProxmoxHypervisorHosts(client, authConfig)
                        if(!hostsResponse.success) {
                                rtn.success = false
                                rtn.msg = "Failed to list Proxmox nodes: ${hostsResponse.msg}"
                                return rtn
                        }

                        boolean allSuccess = true
                        hostsResponse.data.each { host ->
                                def tokenResp = ProxmoxApiComputeUtil.getApiV2Token(authConfig)
                                if(!tokenResp.success) {
                                        allSuccess = false
                                        rtn.msg = tokenResp.msg
                                        return
                                }
                                def tokenCfg = tokenResp.data

                                def optsReq = new HttpApiClient.RequestOptions(
                                        headers:[
                                                'Content-Type':'application/json',
                                                'Cookie':"PVEAuthCookie=${tokenCfg.token}",
                                                'CSRFPreventionToken': tokenCfg.csrfToken
                                        ],
                                        contentType: ContentType.APPLICATION_JSON,
                                        ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
                                )

                                def results = client.callJsonApi(authConfig.apiUrl,
                                        "${authConfig.v2basePath}/nodes/${host.node}/network/${network.externalId ?: network.name}",
                                        null, null, optsReq, 'DELETE')

                                if(!results.success) {
                                        allSuccess = false
                                        rtn.msg = "Failed deleting network on ${host.node}: ${results.msg ?: results.content}"
                                }
                        }

                        rtn.success = allSuccess
                } catch(Exception e) {
                        rtn.success = false
                        rtn.msg = "Error deleting network: ${e.message}"
                        log.error("deleteNetwork error", e)
                } finally {
                        client.shutdownClient()
                }
                return rtn
        }
	
	@Override
	ServiceResponse createSubnet(NetworkSubnet subnet, Network network, Map opts) {
		log.info("NVR: CREATE SUBNET")
		return ServiceResponse.success()	
	}
	
	@Override
	ServiceResponse updateSubnet(NetworkSubnet subnet, Network network, Map opts) {
		log.info("NVR: UPDATE SUBNET")
		return ServiceResponse.success()	
	}
	
	@Override
	ServiceResponse deleteSubnet(NetworkSubnet subnet, Network network, Map opts) {
		log.info("NVR: DELETE SUBNET")
		return ServiceResponse.success()
	}
	
	@Override
	Collection getRouterTypes() {
		return []
	}



	@Override
	ServiceResponse<Network> prepareNetwork(Network network, Map opts) {
		log.info("NVR: PREPARE NETWORK")
		return ServiceResponse.success(network);
	}


	@Override
	ServiceResponse validateNetwork(Network network, Map opts) {
		log.info("NVR: VALIDATE NETWORK")
		return ServiceResponse.success();
	}


}
