package org.flycloud.hadoop.yarn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.annotations.VisibleForTesting;

public class ApplicationMaster {

	static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

	// Configuration
	private Configuration conf;

	// Handle to communicate with the Resource Manager
	@SuppressWarnings("rawtypes")
	AMRMClientAsync amRMClient;

	// Handle to communicate with the Node Manager
	NMClientAsync nmClientAsync;
	// Listen to process the response from the Node Manager
	NMCallbackHandler containerListener;

	// Application Attempt Id ( combination of attemptId and fail count )
	private ApplicationAttemptId appAttemptID;

	// TODO
	// For status update for clients - yet to be implemented
	// Hostname of the container
	private String appMasterHostname = "";
	// Port on which the app master listens for status updates from clients
	private int appMasterRpcPort = -1;
	// Tracking url to which app master publishes info for clients to monitor
	private String appMasterTrackingUrl = "";

	int numTotalContainers = 1;
	int containerMemory = 10;
	int requestPriority;

	// Counter for completed containers ( complete denotes successful or failed
	// )
	AtomicInteger numCompletedContainers = new AtomicInteger();
	// Allocated container count so that we know how many containers has the RM
	// allocated to us
	AtomicInteger numAllocatedContainers = new AtomicInteger();
	// Count of failed containers
	AtomicInteger numFailedContainers = new AtomicInteger();
	// Count of containers already requested from the RM
	// Needed as once requested, we should not request for containers again.
	// Only request for more if the original requirement changes.
	AtomicInteger numRequestedContainers = new AtomicInteger();

	// Shell command to be executed
	String containerClass = "";
	// Args to be passed to the shell command
	String shellArgs = "";
	// Env variables to be setup for the shell command
	Map<String, String> shellEnv = new HashMap<String, String>();

	volatile boolean done;
	private volatile boolean success;

	ByteBuffer allTokens;

	// Launch threads
	List<Thread> launchThreads = new ArrayList<Thread>();

	/**
	 * @param args
	 *            Command line args
	 */
	public static void main(String[] args) {
		boolean result = false;
		try {

			ApplicationMaster appMaster = new ApplicationMaster();
			LOG.info("Initializing ApplicationMaster");
			boolean doRun = appMaster.init(args);
			if (!doRun) {
				System.exit(0);
			}
			result = appMaster.run();
		} catch (Throwable t) {
			LOG.fatal("Error running ApplicationMaster", t);
			System.exit(1);
		}
		if (result) {
			LOG.info("Application Master completed successfully. exiting");
			System.exit(0);
		} else {
			LOG.info("Application Master failed. exiting");
			System.exit(2);
		}
	}

	/**
	 * Dump out contents of $CWD and the environment to stdout for debugging
	 */
	private void dumpOutDebugInfo() {

		LOG.info("Dump debug output");
		Map<String, String> envs = System.getenv();
		for (Map.Entry<String, String> env : envs.entrySet()) {
			LOG.info("System env: key=" + env.getKey() + ", val="
					+ env.getValue());
			System.out.println("System env: key=" + env.getKey() + ", val="
					+ env.getValue());
		}

		String cmd = "ls -al";
		Runtime run = Runtime.getRuntime();
		Process pr = null;
		try {
			pr = run.exec(cmd);
			pr.waitFor();

			BufferedReader buf = new BufferedReader(new InputStreamReader(
					pr.getInputStream()));
			String line = "";
			while ((line = buf.readLine()) != null) {
				LOG.info("System CWD content: " + line);
				System.out.println("System CWD content: " + line);
			}
			buf.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public ApplicationMaster() {
		// Set up the configuration
		conf = new YarnConfiguration();
	}

	/**
	 * Parse command line options
	 * 
	 * @param args
	 *            Command line args
	 * @return Whether init successful and run should be invoked
	 * @throws ParseException
	 * @throws IOException
	 */
	public boolean init(String[] args) throws ParseException, IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path src = new Path("AppMaster.jar");
		Path dst = new Path(fs.getHomeDirectory(), "Node.jar");
		fs.copyFromLocalFile(false, true, src, dst);

		Options opts = new Options();
		opts.addOption("app_attempt_id", true,
				"App Attempt ID. Not to be used unless for testing purposes");
		opts.addOption("container_class", true,
				"Container class to be executed by the Application Master");
		opts.addOption("shell_env", true,
				"Environment for shell script. Specified as env_key=env_val pairs");
		opts.addOption("container_memory", true,
				"Amount of memory in MB to be requested to run the shell command");
		opts.addOption("num_containers", true,
				"No. of containers on which the shell command needs to be executed");
		opts.addOption("priority", true, "Application Priority. Default 0");
		opts.addOption("debug", false, "Dump out debug information");

		opts.addOption("help", false, "Print usage");
		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			printUsage(opts);
			throw new IllegalArgumentException(
					"No args specified for application master to initialize");
		}

		if (cliParser.hasOption("help")) {
			printUsage(opts);
			return false;
		}

		if (cliParser.hasOption("debug")) {
			dumpOutDebugInfo();
		}

		Map<String, String> envs = System.getenv();

		if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
			if (cliParser.hasOption("app_attempt_id")) {
				String appIdStr = cliParser
						.getOptionValue("app_attempt_id", "");
				appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
			} else {
				throw new IllegalArgumentException(
						"Application Attempt Id not set in the environment");
			}
		} else {
			ContainerId containerId = ConverterUtils.toContainerId(envs
					.get(Environment.CONTAINER_ID.name()));
			appAttemptID = containerId.getApplicationAttemptId();
		}

		if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
					+ " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HOST.name())) {
			throw new RuntimeException(Environment.NM_HOST.name()
					+ " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
			throw new RuntimeException(Environment.NM_HTTP_PORT
					+ " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_PORT.name())) {
			throw new RuntimeException(Environment.NM_PORT.name()
					+ " not set in the environment");
		}

		LOG.info("Application master for app" + ", appId="
				+ appAttemptID.getApplicationId().getId()
				+ ", clustertimestamp="
				+ appAttemptID.getApplicationId().getClusterTimestamp()
				+ ", attemptId=" + appAttemptID.getAttemptId());

		if (!cliParser.hasOption("container_class")) {
			throw new IllegalArgumentException(
					"No container class specified to be executed by application master");
		}
		containerClass = cliParser.getOptionValue("container_class");

		if (cliParser.hasOption("shell_args")) {
			shellArgs = cliParser.getOptionValue("shell_args");
		}
		if (cliParser.hasOption("shell_env")) {
			String shellEnvs[] = cliParser.getOptionValues("shell_env");
			for (String env : shellEnvs) {
				env = env.trim();
				int index = env.indexOf('=');
				if (index == -1) {
					shellEnv.put(env, "");
					continue;
				}
				String key = env.substring(0, index);
				String val = "";
				if (index < (env.length() - 1)) {
					val = env.substring(index + 1);
				}
				shellEnv.put(key, val);
			}
		}

		containerMemory = Integer.parseInt(cliParser.getOptionValue(
				"container_memory", "10"));
		numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
				"num_containers", "1"));
		if (numTotalContainers == 0) {
			throw new IllegalArgumentException(
					"Cannot run distributed shell with no containers");
		}
		requestPriority = Integer.parseInt(cliParser.getOptionValue("priority",
				"0"));

		return true;
	}

	/**
	 * Helper function to print usage
	 * 
	 * @param opts
	 *            Parsed command line options
	 */
	private void printUsage(Options opts) {
		new HelpFormatter().printHelp("ApplicationMaster", opts);
	}

	/**
	 * Main run function for the application master
	 * 
	 * @throws YarnException
	 * @throws IOException
	 */
	@SuppressWarnings({ "unchecked" })
	public boolean run() throws YarnException, IOException {
		LOG.info("Starting ApplicationMaster");

		Credentials credentials = UserGroupInformation.getCurrentUser()
				.getCredentials();
		DataOutputBuffer dob = new DataOutputBuffer();
		credentials.writeTokenStorageToStream(dob);
		// Now remove the AM->RM token so that containers cannot access it.
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
		while (iter.hasNext()) {
			Token<?> token = iter.next();
			if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
				iter.remove();
			}
		}
		allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

		AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler(
				this);
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		amRMClient.init(conf);
		amRMClient.start();

		containerListener = createNMCallbackHandler();
		nmClientAsync = new NMClientAsyncImpl(containerListener);
		nmClientAsync.init(conf);
		nmClientAsync.start();

		// Setup local RPC Server to accept status requests directly from
		// clients
		// TODO need to setup a protocol for client to be able to communicate to
		// the RPC server
		// TODO use the rpc port info to register with the RM for the client to
		// send requests to this app master

		// Register self with ResourceManager
		// This will start heartbeating to the RM
		appMasterHostname = NetUtils.getHostname();
		RegisterApplicationMasterResponse response = amRMClient
				.registerApplicationMaster(appMasterHostname, appMasterRpcPort,
						appMasterTrackingUrl);
		// Dump out information about cluster capability as seen by the
		// resource manager
		int maxMem = response.getMaximumResourceCapability().getMemory();
		LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

		// A resource ask cannot exceed the max.
		if (containerMemory > maxMem) {
			LOG.info("Container memory specified above max threshold of cluster."
					+ " Using max value."
					+ ", specified="
					+ containerMemory
					+ ", max=" + maxMem);
			containerMemory = maxMem;
		}

		// Setup ask for containers from RM
		// Send request for containers to RM
		// Until we get our fully allocated quota, we keep on polling RM for
		// containers
		// Keep looping until all the containers are launched and shell script
		// executed on them ( regardless of success/failure).
		for (int i = 0; i < numTotalContainers; ++i) {
			ContainerRequest containerAsk = setupContainerAskForRM();
			amRMClient.addContainerRequest(containerAsk);
		}
		numRequestedContainers.set(numTotalContainers);

		while (!done && (numCompletedContainers.get() != numTotalContainers)) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException ex) {
			}
		}
		finish();

		return success;
	}

	@VisibleForTesting
	NMCallbackHandler createNMCallbackHandler() {
		return new NMCallbackHandler(this);
	}

	private void finish() {
		// Join all launched threads
		// needed for when we time out
		// and we need to release containers
		for (Thread launchThread : launchThreads) {
			try {
				launchThread.join(10000);
			} catch (InterruptedException e) {
				LOG.info("Exception thrown in thread join: " + e.getMessage());
				e.printStackTrace();
			}
		}

		// When the application completes, it should stop all running containers
		LOG.info("Application completed. Stopping running containers");
		nmClientAsync.stop();

		// When the application completes, it should send a finish application
		// signal to the RM
		LOG.info("Application completed. Signalling finish to RM");

		FinalApplicationStatus appStatus;
		String appMessage = null;
		success = true;
		if (numFailedContainers.get() == 0
				&& numCompletedContainers.get() == numTotalContainers) {
			appStatus = FinalApplicationStatus.SUCCEEDED;
		} else {
			appStatus = FinalApplicationStatus.FAILED;
			appMessage = "Diagnostics." + ", total=" + numTotalContainers
					+ ", completed=" + numCompletedContainers.get()
					+ ", allocated=" + numAllocatedContainers.get()
					+ ", failed=" + numFailedContainers.get();
			success = false;
		}
		try {
			amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
		} catch (YarnException ex) {
			LOG.error("Failed to unregister application", ex);
		} catch (IOException e) {
			LOG.error("Failed to unregister application", e);
		}

		amRMClient.stop();
	}

	/**
	 * Setup the request that will be sent to the RM for the container ask.
	 * 
	 * @return the setup ResourceRequest to be sent to RM
	 */
	ContainerRequest setupContainerAskForRM() {
		// setup requirements for hosts
		// using * as any host will do for the distributed shell app
		// set the priority for the request
		Priority pri = Records.newRecord(Priority.class);
		// TODO - what is the range for priority? how to decide?
		pri.setPriority(requestPriority);

		// Set up resource type requirements
		// For now, only memory is supported so we set memory requirements
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(containerMemory);

		ContainerRequest request = new ContainerRequest(capability, null, null,
				pri);
		LOG.info("Requested container ask: " + request.toString());
		return request;
	}
}
