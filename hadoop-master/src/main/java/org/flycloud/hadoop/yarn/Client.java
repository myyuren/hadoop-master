package org.flycloud.hadoop.yarn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class Client {

	private YarnClient yarnClient;
	private Configuration conf;
	private ApplicationId appId;
	private FileSystem fs;
	private String pathSuffix;
	private final long clientStartTime = System.currentTimeMillis();

	private String appMasterJar = "example-1.0.1.jar";
	private String appMasterClass = "org.flycloud.hadoop.yarn.ApplicationMaster";
	private String appName = "client";
	private String amQueue = "default";
	private int amPriority = 0;
	private int amMemory = 10;
	private boolean debug = false;
	private String logFile = "log4j.properties";
	private long clientTimeout = 600000;

	private String containerClass = "org.flycloud.hadoop.data.HbaseCreator";
	private int containerPriority = 0;
	private int containerMemory = 10;
	private int containerCount = 3;
	private Map<String, String> containerEnv = new HashMap<String, String>();

	private static final Log LOG = LogFactory.getLog(Client.class);

	public Client(Configuration conf) {
		this.conf = conf;
	}

	public static void main(String[] args) throws IOException {
		boolean result = false;
		File workaround = new File(".");
		System.getProperties().put("hadoop.home.dir",
				workaround.getAbsolutePath());
		new File("./bin").mkdirs();
		new File("./bin/winutils.exe").createNewFile();
		try {
			long f = new Date().getTime();
			Configuration conf = new Configuration();
			conf.set("yarn.resourcemanager.address", "hdfs://hadoop1:18040");
			conf.set("fs.defaultFS", "hdfs://hadoop1:9000");
			Client client = new Client(conf);
			result = client.run();
			System.out.print("time: " + (new Date().getTime() - f));
		} catch (Throwable t) {
			System.exit(1);
		}
		if (result) {
			System.exit(0);
		}
		System.exit(2);
	}

	private boolean run() throws YarnException, IOException {
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();
		YarnClientApplication app = yarnClient.createApplication();
		ApplicationSubmissionContext ctx = app
				.getApplicationSubmissionContext();
		appId = ctx.getApplicationId();
		ctx.setApplicationName(appName);
		ctx.setResource(getCapability());
		ctx.setAMContainerSpec(getAmContainer());
		ctx.setPriority(getPriority());
		ctx.setQueue(amQueue);
		yarnClient.submitApplication(ctx);

		return monitor(appId);
	}

	private boolean monitor(ApplicationId appId) throws YarnException,
			IOException {

		while (true) {

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.debug("Thread sleep in monitoring loop interrupted");
			}

			// Get application report for the appId we are interested in
			ApplicationReport report = yarnClient.getApplicationReport(appId);

			LOG.info("Got application report from ASM for" + ", appId="
					+ appId.getId() + ", clientToAMToken="
					+ report.getClientToAMToken() + ", appDiagnostics="
					+ report.getDiagnostics() + ", appMasterHost="
					+ report.getHost() + ", appQueue=" + report.getQueue()
					+ ", appMasterRpcPort=" + report.getRpcPort()
					+ ", appStartTime=" + report.getStartTime()
					+ ", yarnAppState="
					+ report.getYarnApplicationState().toString()
					+ ", distributedFinalState="
					+ report.getFinalApplicationStatus().toString()
					+ ", appTrackingUrl=" + report.getTrackingUrl()
					+ ", appUser=" + report.getUser());

			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus dsStatus = report
					.getFinalApplicationStatus();
			if (YarnApplicationState.FINISHED == state) {
				if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
					LOG.info("Application has completed successfully. Breaking monitoring loop");
					return true;
				} else {
					LOG.info("Application did finished unsuccessfully."
							+ " YarnState=" + state.toString()
							+ ", DSFinalStatus=" + dsStatus.toString()
							+ ". Breaking monitoring loop");
					return false;
				}
			} else if (YarnApplicationState.KILLED == state
					|| YarnApplicationState.FAILED == state) {
				LOG.info("Application did not finish." + " YarnState="
						+ state.toString() + ", DSFinalStatus="
						+ dsStatus.toString() + ". Breaking monitoring loop");
				return false;
			}

			if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
				LOG.info("Reached client specified timeout for application. Killing application");
				forceKillApplication(appId);
				return false;
			}
		}

	}

	private void forceKillApplication(ApplicationId appId)
			throws YarnException, IOException {
		yarnClient.killApplication(appId);
	}

	private Resource getCapability() {
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(amMemory);
		return capability;
	}

	private ContainerLaunchContext getAmContainer() throws IOException {
		ContainerLaunchContext amContainer = Records
				.newRecord(ContainerLaunchContext.class);
		amContainer.setLocalResources(getLocalResources());
		amContainer.setEnvironment(getEnv());
		amContainer.setCommands(getCommands());
		fillTokens(amContainer);
		return amContainer;
	}

	private void fillTokens(ContainerLaunchContext amContainer)
			throws IOException {
		if (UserGroupInformation.isSecurityEnabled()) {
			Credentials credentials = new Credentials();
			String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
			if (tokenRenewer == null || tokenRenewer.length() == 0) {
				throw new IOException(
						"Can't get Master Kerberos principal for the RM to use as renewer");
			}
			DataOutputBuffer dob = new DataOutputBuffer();
			credentials.writeTokenStorageToStream(dob);
			ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0,
					dob.getLength());
			amContainer.setTokens(fsTokens);
		}
	}

	private Priority getPriority() {
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(amPriority);
		return pri;
	}

	private Map<String, LocalResource> getLocalResources() throws IOException {
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		localResources.put("AppMaster.jar", getAmJarRsrc());
		fillLog4jRsrc(localResources);
		return localResources;
	}

	private LocalResource getAmJarRsrc() throws IOException {
		fs = FileSystem.get(conf);
		Path src = new Path(appMasterJar);
		pathSuffix = appName + "/" + appId.getId() + "/AppMaster.jar";
		Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
		fs.copyFromLocalFile(false, true, src, dst);
		FileStatus destStatus = fs.getFileStatus(dst);
		LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
		amJarRsrc.setType(LocalResourceType.FILE);
		amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
		amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
		amJarRsrc.setTimestamp(destStatus.getModificationTime());
		amJarRsrc.setSize(destStatus.getLen());
		return amJarRsrc;
	}

	private void fillLog4jRsrc(Map<String, LocalResource> localResources)
			throws IOException {
		if (!logFile.isEmpty()) {
			Path log4jSrc = new Path(logFile);
			Path log4jDst = new Path(fs.getHomeDirectory(), "log4j.props");
			fs.copyFromLocalFile(false, true, log4jSrc, log4jDst);
			FileStatus log4jFileStatus = fs.getFileStatus(log4jDst);
			LocalResource log4jRsrc = Records.newRecord(LocalResource.class);
			log4jRsrc.setType(LocalResourceType.FILE);
			log4jRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
			log4jRsrc.setResource(ConverterUtils.getYarnUrlFromURI(log4jDst
					.toUri()));
			log4jRsrc.setTimestamp(log4jFileStatus.getModificationTime());
			log4jRsrc.setSize(log4jFileStatus.getLen());
			localResources.put("log4j.properties", log4jRsrc);
		}
	}

	private List<String> getCommands() {
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
		vargs.add("-Xmx" + amMemory + "m");
		vargs.add(appMasterClass);
		vargs.add("--container_class " + this.containerClass);
		vargs.add("--container_memory " + String.valueOf(containerMemory));
		vargs.add("--num_containers " + String.valueOf(containerCount));
		vargs.add("--priority " + String.valueOf(containerPriority));
		for (Map.Entry<String, String> entry : containerEnv.entrySet()) {
			vargs.add("--shell_env " + entry.getKey() + "=" + entry.getValue());
		}
		if (debug) {
			vargs.add("--debug");
		}

		vargs.add("1>" + "/tmp/appmaster-stdout.log");
		vargs.add("2>" + "/tmp/appmaster-stderr.log");

		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
			command.append(str).append(" ");
		}

		List<String> commands = new ArrayList<String>();
		LOG.info(command.toString());
		commands.add(command.toString());
		return commands;
	}

	private Map<String, String> getEnv() throws IOException {
		Map<String, String> env = new HashMap<String, String>();

		StringBuilder classPathEnv = new StringBuilder(
				Environment.CLASSPATH.$()).append(File.pathSeparatorChar)
				.append("./*");
		for (String c : conf.getStrings(
				YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			classPathEnv.append(File.pathSeparatorChar);
			classPathEnv.append(c.trim());
		}
		classPathEnv.append(File.pathSeparatorChar)
				.append("./log4j.properties");

		classPathEnv.append(File.pathSeparatorChar).append(
				"/opt/cloudera/parcels/CDH/lib/hadoop/client/*");

		classPathEnv.append(File.pathSeparatorChar).append(
				"/opt/cloudera/parcels/CDH/lib/hbase/lib/*");
		
		if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
			classPathEnv.append(':');
			classPathEnv.append(System.getProperty("java.class.path"));
		}

		env.put("CLASSPATH", classPathEnv.toString());
		return env;
	}

}
