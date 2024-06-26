package com.morpheus.proxmox.ve

import com.morpheusdata.core.AbstractProvisionProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.providers.WorkloadProvisionProvider
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.Datastore
import com.morpheusdata.model.HostType
import com.morpheusdata.model.Icon
import com.morpheusdata.model.LogLevel
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.ProcessEvent
import com.morpheusdata.model.ServicePlan
import com.morpheusdata.model.StorageVolumeType
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.model.VirtualImageLocation
import com.morpheusdata.model.Workload
import com.morpheusdata.model.projection.ComputeServerIdentityProjection
import com.morpheusdata.model.projection.DatastoreIdentity
import com.morpheusdata.model.provisioning.WorkloadRequest
import com.morpheusdata.model.Cloud
import com.morpheusdata.response.PrepareWorkloadResponse
import com.morpheusdata.response.ProvisionResponse
import com.morpheusdata.response.ServiceResponse
import com.morpheus.proxmox.ve.util.ProxmoxComputeUtil
import com.morpheus.proxmox.ve.util.ProxmoxMiscUtil
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import com.jcraft.jsch.JSch
import com.jcraft.jsch.Session
import com.jcraft.jsch.Channel
import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.SftpException


@Slf4j
class ProxmoxVeProvisionProvider extends AbstractProvisionProvider implements WorkloadProvisionProvider {
	public static final String PROVISION_PROVIDER_CODE = 'proxmox-ve.provision'

	protected MorpheusContext context
	protected Plugin plugin

	public ProxmoxVeProvisionProvider(Plugin plugin, MorpheusContext ctx) {
		super()
		this.@context = ctx
		this.@plugin = plugin
	}

	@Override
	Boolean hasNetworks() {
		return true
	}

	@Override
	Boolean supportsAgent() {
		return true
	}

	@Override
	Boolean canCustomizeRootVolume() {
		return true
	}

	@Override
	Boolean canCustomizeDataVolumes() {
		return true
	}

	Boolean createDefaultInstanceType() {
		return false;
	}

	/**
	 * This method is called before runWorkload and provides an opportunity to perform action or obtain configuration
	 * that will be needed in runWorkload. At the end of this method, if deploying a ComputeServer with a VirtualImage,
	 * the sourceImage on ComputeServer should be determined and saved.
	 * @param workload the Workload object we intend to provision along with some of the associated data needed to determine
	 *                 how best to provision the workload
	 * @param workloadRequest the RunWorkloadRequest object containing the various configurations that may be needed
	 *                        in running the Workload. This will be passed along into runWorkload
	 * @param opts additional configuration options that may have been passed during provisioning
	 * @return Response from API
	 */
	@Override
	ServiceResponse<PrepareWorkloadResponse> prepareWorkload(Workload workload, WorkloadRequest workloadRequest, Map opts) {
		ServiceResponse<PrepareWorkloadResponse> resp = new ServiceResponse<PrepareWorkloadResponse>(
			true, // successful
			'', // no message
			null, // no errors
			new PrepareWorkloadResponse(workload:workload) // adding the workload to the response for convenience
		)
		return resp
	}

	/**
	 * Some older clouds have a provision type code that is the exact same as the cloud code. This allows one to set it
	 * to match and in doing so the provider will be fetched via the cloud providers {@link CloudProvider#getDefaultProvisionTypeCode()} method.
	 * @return code for overriding the ProvisionType record code property
	 */
	@Override
	String getProvisionTypeCode() {
		return PROVISION_PROVIDER_CODE
	}

	/**
	 * Provide an icon to be displayed for ServicePlans, VM detail page, etc.
	 * where a circular icon is displayed
	 * @since 0.13.6
	 * @return Icon
	 */
	@Override
	Icon getCircularIcon() {
		// TODO: change icon paths to correct filenames once added to your project
		return new Icon(path:'provision-circular.svg', darkPath:'provision-circular-dark.svg')
	}

	/**
	 * Provides a Collection of OptionType inputs that need to be made available to various provisioning Wizards
	 * @return Collection of OptionTypes
	 */
	@Override
	Collection<OptionType> getOptionTypes() {
		def options = []
		options << new OptionType(
				name: 'skip agent install',
				code: 'provisionType.nutanixPrism.noAgent',
				category: 'provisionType.nutanixPrism',
				inputType: OptionType.InputType.CHECKBOX,
				fieldName: 'noAgent',
				fieldContext: 'config',
				fieldCode: 'gomorpheus.optiontype.SkipAgentInstall',
				fieldLabel: 'Skip Agent Install',
				fieldGroup:'Advanced Options',
				displayOrder: 4,
				required: false,
				enabled: true,
				editable:false,
				global:false,
				placeHolder:null,
				helpBlock:'Skipping Agent installation will result in a lack of logging and guest operating system statistics. Automation scripts may also be adversely affected.',
				defaultValue: false,
				custom:false,
				fieldClass:null
		)
		return options
	}

	/**
	 * Provides a Collection of OptionType inputs for configuring node types
	 * @since 0.9.0
	 * @return Collection of OptionTypes
	 */
	@Override
	Collection<OptionType> getNodeOptionTypes() {
		Collection<OptionType> nodeOptions = []
		return nodeOptions
	}

	/**
	 * Provides a Collection of StorageVolumeTypes that are available for root StorageVolumes
	 * @return Collection of StorageVolumeTypes
	 */
	@Override
	Collection<StorageVolumeType> getRootVolumeStorageTypes() {
		return this.getStorageVolumeTypes()
	}

	/**
	 * Provides a Collection of StorageVolumeTypes that are available for data StorageVolumes
	 * @return Collection of StorageVolumeTypes
	 */
	@Override
	Collection<StorageVolumeType> getDataVolumeStorageTypes() {
		return this.getStorageVolumeTypes()
	}

	Collection<StorageVolumeType> getStorageVolumeTypes() {
		Collection<StorageVolumeType> volumeTypes = []

		volumeTypes << new StorageVolumeType(
				name: "Proxmox VM Generic Volume Type",
				code: "proxmox.vm.generic.volume.type",
				externalId: "proxmox.vm.generic.volume.type",
				displayOrder: 0
		)

		return volumeTypes
	} 

	/**
	 * Provides a Collection of ${@link ServicePlan} related to this ProvisionProvider that can be seeded in.
	 * Some clouds do not use this as they may be synced in from the public cloud. This is more of a factor for
	 * On-Prem clouds that may wish to have some precanned plans provided for it.
	 * @return Collection of ServicePlan sizes that can be seeded in at plugin startup.
	 */
	@Override
	Collection<ServicePlan> getServicePlans() {
		Collection<ServicePlan> plans = []
		plans << new ServicePlan([code:'proxmox-ve-vm-512', name:'1 vCPU, 512MB Memory', description:'1 vCPU, 512MB Memory', sortOrder:0,
										 maxStorage:10l * 1024l * 1024l * 1024l, maxMemory: 1l * 512l * 1024l * 1024l, maxCores:1,
										 customMaxStorage:true, customMaxDataStorage:true, addVolumes:true])

		plans << new ServicePlan([code:'proxmox-ve-vm-1024', name:'1 vCPU, 1GB Memory', description:'1 vCPU, 1GB Memory', sortOrder:1,
										 maxStorage: 10l * 1024l * 1024l * 1024l, maxMemory: 1l * 1024l * 1024l * 1024l, maxCores:1,
										 customMaxStorage:true, customMaxDataStorage:true, addVolumes:true])

		plans << new ServicePlan([code:'proxmox-ve-vm-2048', name:'1 vCPU, 2GB Memory', description:'1 vCPU, 2GB Memory', sortOrder:2,
										 maxStorage: 20l * 1024l * 1024l * 1024l, maxMemory: 2l * 1024l * 1024l * 1024l, maxCores:1,
										 customMaxStorage:true, customMaxDataStorage:true, addVolumes:true])

		plans << new ServicePlan([code:'proxmox-ve-vm-4096', name:'1 vCPU, 4GB Memory', description:'1 vCPU, 4GB Memory', sortOrder:3,
										 maxStorage: 40l * 1024l * 1024l * 1024l, maxMemory: 4l * 1024l * 1024l * 1024l, maxCores:1,
										 customMaxStorage:true, customMaxDataStorage:true, addVolumes:true])

		plans << new ServicePlan([code:'proxmox-ve-vm-8192', name:'2 vCPU, 8GB Memory', description:'2 vCPU, 8GB Memory', sortOrder:4,
								  maxStorage: 80l * 1024l * 1024l * 1024l, maxMemory: 8l * 1024l * 1024l * 1024l, maxCores:2,
								  customMaxStorage:true, customMaxDataStorage:true, addVolumes:true])

		plans << new ServicePlan([code:'proxmox-ve-vm-8192', name:'2 vCPU, 8GB Memory', description:'2 vCPU, 8GB Memory', sortOrder:4,
								  maxStorage: 80l * 1024l * 1024l * 1024l, maxMemory: 8l * 1024l * 1024l * 1024l, maxCores:2,
								  customMaxStorage:true, customMaxDataStorage:true, addVolumes:true])

		plans << new ServicePlan([code:'proxmox-ve-vm-16384', name:'2 vCPU, 16GB Memory', description:'2 vCPU, 16GB Memory', sortOrder:5,
										 maxStorage: 160l * 1024l * 1024l * 1024l, maxMemory: 16l * 1024l * 1024l * 1024l, maxCores:2,
										 customMaxStorage:true, customMaxDataStorage:true, addVolumes:true])

		plans << new ServicePlan([code:'proxmox-ve-vm-24576', name:'4 vCPU, 24GB Memory', description:'4 vCPU, 24GB Memory', sortOrder:6,
										 maxStorage: 240l * 1024l * 1024l * 1024l, maxMemory: 24l * 1024l * 1024l * 1024l, maxCores:4,
										 customMaxStorage:true, customMaxDataStorage:true, addVolumes:true])

		plans << new ServicePlan([code:'proxmox-ve-vm-32768', name:'4 vCPU, 32GB Memory', description:'4 vCPU, 32GB Memory', sortOrder:7,
										 maxStorage: 320l * 1024l * 1024l * 1024l, maxMemory: 32l * 1024l * 1024l * 1024l, maxCores:4,
										 customMaxStorage:true, customMaxDataStorage:true, addVolumes:true])

		plans << new ServicePlan([code:'proxmox-ve-internal-custom', editable:false, name:'Proxmox Custom', description:'Proxmox Custom', sortOrder:0,
										 customMaxStorage:true, customMaxDataStorage:true, addVolumes:true, customCpu: true, customCores: true, customMaxMemory: true, deletable: false, provisionable: false,
										 maxStorage:0l, maxMemory: 0l,  maxCpu:0])
		return plans
	}


	/**
	 * Validates the provided provisioning options of a workload. A return of success = false will halt the
	 * creation and display errors
	 * @param opts options
	 * @return Response from API. Errors should be returned in the errors Map with the key being the field name and the error
	 * message as the value.
	 */
	@Override
	ServiceResponse validateWorkload(Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * This method is a key entry point in provisioning a workload. This could be a vm, a container, or something else.
	 * Information associated with the passed Workload object is used to kick off the workload provision request
	 * @param workload the Workload object we intend to provision along with some of the associated data needed to determine
	 *                 how best to provision the workload
	 * @param workloadRequest the RunWorkloadRequest object containing the various configurations that may be needed
	 *                        in running the Workload
	 * @param opts additional configuration options that may have been passed during provisioning
	 * @return Response from API
	 */
	@Override
	ServiceResponse<ProvisionResponse> runWorkload(Workload workload, WorkloadRequest workloadRequest, Map opts) {
		log.debug("In runWorkload...")
		log.info("Cloud-Init User-Data Opts: $workloadRequest.cloudConfigOpts")
		log.info("Cloud-Init User-Data User: $workloadRequest.cloudConfigUser")
		log.info("Cloud-Init User-Data Meta: $workloadRequest.cloudConfigMeta")
		log.info("Cloud-Init User-Data Network: $workloadRequest.cloudConfigNetwork")
		Thread.sleep(10000)
		log.info("Network details")

		context.async.process.startProcessStep(workloadRequest.process , new ProcessEvent(type: ProcessEvent.ProcessType.general), 'completed').blockingGet()

		ComputeServer server = workload.server
		Cloud cloud = server.cloud
		VirtualImage virtualImage = server.sourceImage
		Map authConfig = plugin.getAuthConfig(cloud)
		HttpApiClient client = new HttpApiClient()
		String nodeId = workload.server.getConfigProperty('proxmoxNode') ?: null
		DatastoreIdentity targetDS = server.getVolumes().first().datastore


		if (!targetDS) {
			targetDS = getDefaultDatastore(cloud.id)
		}

		ComputeServer hvNode = getHypervisorHostByExternalId(cloud.id, nodeId)
		if (!hvNode.sshHost || !hvNode.sshUsername || !hvNode.sshPassword) {
			context.async.process.startProcessStep(workloadRequest.process, new ProcessEvent(type: ProcessEvent.ProcessType.provisionDeploy), 'completed').blockingGet()
			return ServiceResponse.error("SSH credentials required on host for provisioning to work. Edit the hypervisor host properties under the cloud Hosts tab.")
		}

		String imageExternalId = getOrUploadImage(client, authConfig, cloud, virtualImage, hvNode, targetDS.name)

		context.async.process.startProcessStep(workloadRequest.process, new ProcessEvent(type: ProcessEvent.ProcessType.provisionDeploy), 'completed').blockingGet()

		log.info("Provisioning/cloning: ${workload.getInstance().name} from Image Id: $imageExternalId on node: $nodeId")
		ServiceResponse rtnClone = ProxmoxComputeUtil.cloneTemplate(client, authConfig, imageExternalId, workload.getInstance().name, nodeId, server.coresPerSocket, server.maxMemory)

		log.debug("VM Clone done. Results: $rtnClone")

		if (!rtnClone.success) {
			log.error("Provisioning/clone failed: $rtnClone.msg")
			return ServiceResponse.error("Provisioning failed: $rtnClone.msg")
		}/* else {
			ServiceResponse rtnResize = ProxmoxComputeUtil.resizeVMCompute(client, authConfig, nodeId, rtnClone.data.vmId, server.coresPerSocket, server.maxMemory)
			log.debug("VM Resize done. Results: $rtnResize")
			if (!rtnClone.success) {
				log.error("VM Sizing failed: $rtnResize.msg")
				return ServiceResponse.error("Provisioning failed: $rtnResize.msg")
			}
		}*/

		if(virtualImage?.isCloudInit && workloadRequest?.cloudConfigUser) {
			String cloudConfigUser = workloadRequest?.cloudConfigUser
			log.debug("Ensuring snippets directory on node: $nodeId")
			context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "mkdir -p /var/lib/vz/snippets", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
			log.debug("Creating cloud-init user-data file on hypervisor node: /var/lib/vz/snippets/${rtnClone.data.vmId}-cloud-init.yml")
			ProxmoxMiscUtil.sftpCreateFile(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "/var/lib/vz/snippets/${rtnClone.data.vmId}-cloud-init.yml", workloadRequest.cloudConfigUser, null)
			log.debug("Creating cloud-init vm disk: local-zfs:cloudinit")
			context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "qm set ${rtnClone.data.vmId} --ide2 local-zfs:cloudinit", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
			log.debug("Mounting cloud-init data to disk...")
			context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "qm set ${rtnClone.data.vmId} --cicustom \"user=local:snippets/${rtnClone.data.vmId}-cloud-init.yml\"", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
		}

		ProxmoxComputeUtil.startVM(client, authConfig, nodeId, rtnClone.data.vmId)

		return new ServiceResponse<ProvisionResponse>(
				true,
				"Provisioned",
				null,
				new ProvisionResponse(
						success: true,
						noAgent: false,
						skipNetworkWait: true,
						installAgent: true,
						externalId: "abc123",
						//publicIp: "10.10.10.10",
						privateIp: "0.0.0.0"
				)
		)
	}


	private DatastoreIdentity getDefaultDatastore(Long cloudId) {
		log.debug("getDefaultDatastoreName()...")
		//returns the largest non-local datastore
		Datastore rtn = null
		context.async.cloud.datastore.list(new DataQuery().withFilters([
				new DataFilter("refType", "ComputeZone"),
				new DataFilter("refId", cloudId)
		])).blockingForEach { ds ->
			if (rtn == null) {
				rtn = ds
			} else if (ds.name.contains("zfs") || (ds.getFreeSpace() > rtn.getFreeSpace())) {
				rtn = ds
			}
		}
		return rtn
	}

	private ComputeServer getHypervisorHostByExternalId(Long cloudId, String externalId) {
		log.info("Fetch Hypervisor Host by Cloud/External Id: $cloudId/$externalId")

		ComputeServer hvNode
		def hostIdentityProjection = context.async.computeServer.listIdentityProjections(cloudId, null).filter {
			ComputeServerIdentityProjection projection ->
				if (projection.externalId == externalId) {
					return true
				}
				false
		}.subscribe {
			log.info("Found Host IdentityProjection: $it.id")
			List<Long> idList = [it.id]
			hvNode = context.async.computeServer.listById(idList).blockingFirst()
			log.debug("Returning hvHost: $hvNode.sshHost")
		}

		return hvNode
	}



	private getOrUploadImage(HttpApiClient client, Map authConfig, Cloud cloud, VirtualImage virtualImage, ComputeServer hvNode, String targetDS) {
		def imageExternalId
		def lock
		def imagePathPrefix = "/var/opt/morpheus/morpheus-ui/vms/morpheus-images"
		def remoteImageDir = "/var/lib/vz/template/qemu"
		def lockKey = "proxmox.ve.imageupload.${cloud.regionCode}.${virtualImage?.id}".toString()

		try {
			//lock = context.acquireLock(lockKey, [timeout: 2l * 60l * 1000l, ttl: 2l * 60l * 1000l]).blockingGet()
			lock = context.acquireLock(lockKey, [timeout: 15000l, ttl: 15000l]).blockingGet()
			//hold up to a 1 hour lock for image upload
			if (virtualImage) {
				log.info("VIRTUAL IMAGE EXISTS")
				VirtualImageLocation virtualImageLocation
				try {
					log.debug("searching for virtualImageLocation: $virtualImage.id")
					//virtualImageLocation = context.async.virtualImage.location.findVirtualImageLocation(virtualImage.id, cloud.id, cloud.regionCode, null, false).blockingGet()
					virtualImageLocation = context.async.virtualImage.location.find(new DataQuery().withFilters([
							new DataFilter("refType", "ComputeZone"),
							new DataFilter("refId", cloud.id),
							new DataFilter("externalId", virtualImage.externalId)
					])).blockingGet()
					log.debug("Got VirtualImageLocation ($cloud.id, $virtualImage.externalId): $virtualImageLocation")

					if (!virtualImageLocation) {
						log.info("VIRTUAL IMAGE LOCATION NOT EXISTS")
						imageExternalId = null
					} else {
						log.info("VIRTUAL IMAGE LOCATION EXISTS")
						imageExternalId = virtualImageLocation.externalId
					}
				} catch (e) {
					log.error "Error in findVirtualImageLocation.. could be not found ${e}", e
				}
			}
			if (!imageExternalId) { //If its userUploaded and still needs uploaded to cloud
				// Create the image
				def cloudFiles = context.async.virtualImage.getVirtualImageFiles(virtualImage).blockingGet()
				log.debug("CloudFiles: $cloudFiles")
				def imageFile = cloudFiles?.find { cloudFile -> cloudFile.name.toLowerCase().endsWith(".qcow2") }
				log.debug("ImageFile: $imageFile")
				def contentLength = imageFile?.getContentLength()

				//def letProxmoxDownloadImage = imageFile?.getURL()?.toString()?.contains('morpheus-images')

				//log.debug("letProxmoxDownloadImage: $letProxmoxDownloadImage")

				//create qcow2 template directory on proxmox
				log.debug("Ensuring Image Directory on node: $hvNode.sshHost")
				def dirOut = context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "mkdir -p $remoteImageDir", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
				log.debug("Dir create SSH Task \"mkdir -p $remoteImageDir\" results: ${dirOut.toMap().toString()}")

				//sftp .qcow2 file to the directory on proxmox server
				log.debug("uploading Image $imagePathPrefix/$imageFile to $hvNode.sshHost:$remoteImageDir")
				ProxmoxMiscUtil.sftpUpload(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "$imagePathPrefix/$imageFile", remoteImageDir, null)

				//create blank vm template on proxmox
				ServiceResponse templateResp = ProxmoxComputeUtil.createImageTemplate(client, authConfig, virtualImage.name, hvNode.externalId, 1, 1024L)
				log.debug("Create Image response data $templateResp.data")
				imageExternalId = templateResp.data.templateId

				//import the disk file to the blank vm template
				String fileName = new File("$imageFile").getName()
				log.debug("Executing ImportDisk command on node: qm importdisk $templateResp.data.templateId $remoteImageDir/$fileName $targetDS")
				def diskCreateOut = context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "qm importdisk $templateResp.data.templateId $remoteImageDir/$fileName $targetDS", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
				log.debug("Disk ImportDisk SSH Task \"qm importdisk $templateResp.data.templateId $remoteImageDir/$fileName $targetDS\" results: ${diskCreateOut.toMap().toString()}")

				//Mount the disk
				log.debug("Executing DiskMount SSH Task \"qm set $templateResp.data.templateId --scsi0 $targetDS:vm-$templateResp.data.templateId-disk-0\"")
				def diskMountOut = context.executeSshCommand(hvNode.sshHost, 22, hvNode.sshUsername, hvNode.sshPassword, "qm set $templateResp.data.templateId --scsi0 $targetDS:vm-$templateResp.data.templateId-disk-0", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
				log.debug("Disk Mount SSH Task \"qm set $templateResp.data.templateId --scsi0 $targetDS:vm-$templateResp.data.templateId-disk-0\" results: ${diskMountOut.toMap().toString()}")

				virtualImage.externalId = imageExternalId
				log.debug("Updating virtual image $virtualImage.name with external ID $virtualImage.externalId")
				context.async.virtualImage.bulkSave([virtualImage]).blockingGet()
				VirtualImageLocation virtualImageLocation = new VirtualImageLocation([
						virtualImage: virtualImage,
						externalId  : imageExternalId,
						imageRegion : cloud.regionCode,
						code        : "proxmox.ve.image.${cloud.id}.$templateResp.data.templateId",
						internalId  : imageExternalId,
						refId		: cloud.id,
						refType		: 'ComputeZone',
				])
				context.async.virtualImage.location.create([virtualImageLocation], cloud).blockingGet()

			}
		} finally {
			context.releaseLock(lockKey, [lock:lock]).blockingGet()
		}
		return imageExternalId
	}


	/**
	 * This method is called after successful completion of runWorkload and provides an opportunity to perform some final
	 * actions during the provisioning process. For example, ejected CDs, cleanup actions, etc
	 * @param workload the Workload object that has been provisioned
	 * @return Response from the API
	 */
	@Override
	ServiceResponse finalizeWorkload(Workload workload) {
		return ServiceResponse.success()
	}

	/**
	 * Issues the remote calls necessary top stop a workload element from running.
	 * @param workload the Workload we want to shut down
	 * @return Response from API
	 */
	@Override
	ServiceResponse stopWorkload(Workload workload) {
		return ServiceResponse.success()
	}

	/**
	 * Issues the remote calls necessary to start a workload element for running.
	 * @param workload the Workload we want to start up.
	 * @return Response from API
	 */
	@Override
	ServiceResponse startWorkload(Workload workload) {
		return ServiceResponse.success()
	}

	/**
	 * Issues the remote calls to restart a workload element. In some cases this is just a simple alias call to do a stop/start,
	 * however, in some cases cloud providers provide a direct restart call which may be preferred for speed.
	 * @param workload the Workload we want to restart.
	 * @return Response from API
	 */
	@Override
	ServiceResponse restartWorkload(Workload workload) {
		// Generally a call to stopWorkLoad() and then startWorkload()
		return ServiceResponse.success()
	}

	/**
	 * This is the key method called to destroy / remove a workload. This should make the remote calls necessary to remove any assets
	 * associated with the workload.
	 * @param workload to remove
	 * @param opts map of options
	 * @return Response from API
	 */
	@Override
	ServiceResponse removeWorkload(Workload workload, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Method called after a successful call to runWorkload to obtain the details of the ComputeServer. Implementations
	 * should not return until the server is successfully created in the underlying cloud or the server fails to
	 * create.
	 * @param server to check status
	 * @return Response from API. The publicIp and privateIp set on the WorkloadResponse will be utilized to update the ComputeServer
	 */
	@Override
	ServiceResponse<ProvisionResponse> getServerDetails(ComputeServer server) {
		return new ServiceResponse<ProvisionResponse>(true, null, null, new ProvisionResponse(success:true))
	}

	/**
	 * Method called before runWorkload to allow implementers to create resources required before runWorkload is called
	 * @param workload that will be provisioned
	 * @param opts additional options
	 * @return Response from API
	 */
	@Override
	ServiceResponse createWorkloadResources(Workload workload, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Stop the server
	 * @param computeServer to stop
	 * @return Response from API
	 */
	@Override
	ServiceResponse stopServer(ComputeServer computeServer) {
		return ServiceResponse.success()
	}

	/**
	 * Start the server
	 * @param computeServer to start
	 * @return Response from API
	 */
	@Override
	ServiceResponse startServer(ComputeServer computeServer) {
		return ServiceResponse.success()
	}

	/**
	 * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
	 *
	 * @return an implementation of the MorpheusContext for running Future based rxJava queries
	 */
	@Override
	MorpheusContext getMorpheus() {
		return this.@context
	}

	/**
	 * Returns the instance of the Plugin class that this provider is loaded from
	 * @return Plugin class contains references to other providers
	 */
	@Override
	Plugin getPlugin() {
		return this.@plugin
	}

	/**
	 * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
	 * that is seeded or generated related to this provider will reference it by this code.
	 * @return short code string that should be unique across all other plugin implementations.
	 */
	@Override
	String getCode() {
		return PROVISION_PROVIDER_CODE
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'Proxmox VE Provisioning'
	}

	///MISSING LOGO issue
	////GOTCHA that needs to be fixed. The instanceType = instance-type.stackit in the scribe yml doesn't work
	@Override
	HostType getHostType() {
		HostType.vm
	}
}
