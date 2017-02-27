/*
 * The Fascinator - JClouds BlobStore storage plugin
 * Copyright (C) 2009-2011 University of Southern Queensland
 * Copyright (C) 2011 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.googlecode.fascinator.storage.jclouds;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.domain.Location;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonSimpleConfig;

/**
 * A private utility class to wrap Apache Jclouds connectivity and save repeated
 * instantiations across the package. Many methods/properties here are default
 * scoped to package-private so Payloads, DigitalObjects and the top-level
 * Storage class all get access.
 *
 * @author Andrew Brazzatti
 */
public class BlobStoreClient {

	/** Default Container Name **/
	private static final String DEFAULT_CONTAINER_NAME = "fascinator";

	/** Logger */
	private static Logger log = LoggerFactory.getLogger(BlobStoreClient.class);

	/** System Config */
	private static JsonSimpleConfig systemConfig;

	/** BlobStore client */
	private static BlobStore blobStore;

	private static String provider;

	private static String credential;

	private static String identity;

	private static String containerName;

	private static String location;

	private static BlobStoreContext context;

	private static String fileSystemLocation;
	
	private static String gridFsConnectionString;

	private static Boolean supportsUserMetadata = true;

	private static Boolean supportsUserMetadataSetting;

	private static int connectCount;

	/**
	 * Public init method for File based configuration.
	 *
	 * @param jsonFile
	 *            The File containing JSON configuration
	 * @throws StorageException
	 *             if any errors occur
	 */
	static void init(File jsonFile) throws StorageException {
		try {
			systemConfig = new JsonSimpleConfig(jsonFile);
			init();
		} catch (IOException ioe) {
			throw new StorageException("Failed to read file configuration!", ioe);
		}
	}

	/**
	 * Public init method for String based configuration.
	 *
	 * @param jsonString
	 *            The String containing JSON configuration
	 * @throws StorageException
	 *             if any errors occur
	 */
	static void init(String jsonString) throws StorageException {
		try {
			systemConfig = new JsonSimpleConfig(jsonString);
			init();
		} catch (IOException ioe) {
			throw new StorageException("Failed to read string configuration!", ioe);
		}
	}

	/**
	 * Constructor
	 *
	 * @throws StorageException
	 *             if any errors occur
	 */
	private static void init() throws StorageException {
		// Don't instantiate twice
		if (blobStore != null) {
			return;
		}

		// Grab all our information from config
		provider = systemConfig.getString("swift", "storage", "blobstore", "provider");
		credential = systemConfig.getString("", "storage", "blobstore", "password");
		identity = systemConfig.getString("", "storage", "blobstore", "identity");
		containerName = systemConfig.getString(DEFAULT_CONTAINER_NAME, "storage", "blobstore", "containerName");
		location = systemConfig.getString(null, "storage", "blobstore", "location");
		fileSystemLocation = systemConfig.getString(null, "storage", "blobstore", "fileSystemLocation");
		gridFsConnectionString = systemConfig.getString(null, "storage", "blobstore", "gridFsConnectionString");
		supportsUserMetadataSetting = systemConfig.getBoolean(null, "storage", "blobstore", "supportsUserMetadata");

		blobStoreConnect();

	}

	/**
	 * Establish a connection to the BlobStore, then return the instantiated
	 * BlobStore client used to connect.
	 *
	 * @return BlobStore: The client used to connect to the API
	 * @throws StorageException
	 *             if there was an error
	 */
	private static BlobStore blobStoreConnect() throws StorageException {
		if (blobStore != null && connectCount < 100) {
			return blobStore;
		}
		connectCount = 0;
		ContextBuilder contextBuilder = ContextBuilder.newBuilder(provider);
		// If we're using filesystem, set local directory to write objects to
		if ("filesystem".equals(provider)) {
			if (supportsUserMetadataSetting != null) {
				supportsUserMetadata = supportsUserMetadataSetting;
			} else {
				File storageDir = new File(fileSystemLocation);
				if (!storageDir.exists()) {
					try {
						FileUtils.forceMkdir(storageDir);
						// Java doesn't support extended attributes in some file
						// systems like FAT32 and HFS. As JClouds use them to
						// store
						// user metadata we'll need to store them differently on
						// these file systems.
						if (!Files.getFileStore(storageDir.toPath())
								.supportsFileAttributeView(UserDefinedFileAttributeView.class)) {
							supportsUserMetadata = false;
						}
					} catch (IOException e) {
						throw new StorageException("Failed to create storage directory", e);
					}
				}
			}
			Properties properties = new Properties();
			properties.setProperty(FilesystemConstants.PROPERTY_BASEDIR, fileSystemLocation);
			contextBuilder.overrides(properties);
		} else if ("gridfs".equals(provider)) {
			Properties properties = new Properties();
			properties.setProperty(Constants.PROPERTY_ENDPOINT, "mongodb://my_mongo_server:27017/?maxPoolSize=50");
			contextBuilder.overrides(properties);

		}
		context = contextBuilder.credentials(identity, credential)
				.endpoint("https://keystone.rc.nectar.org.au:5000/v2.0").buildView(BlobStoreContext.class);

		blobStore = context.getBlobStore();

		Location loc = null;
		if (StringUtils.isNotEmpty(location)) {
			for (Location assignableLoc : blobStore.listAssignableLocations()) {
				if (assignableLoc.getId().equalsIgnoreCase(location)) {
					loc = assignableLoc;
					break;
				}

			}
			if (loc == null) {
				throw new StorageException(location + " location not found in Blobstore");
			}
		}
		blobStore.createContainerInLocation(loc, containerName);

		return blobStore;
	}

	/**
	 * Package-private 'getter' method for the base BlobStore Client.
	 *
	 * @return BlobStore The BlobStore Client Object
	 * @throws StorageException
	 *             if any errors occur
	 */
	static BlobStore getClient() throws StorageException {
		return blobStoreConnect();
	}

	/**
	 * A really simple wrapper on closable object to allow trivial close
	 * attempts when we are unsure if they are even open.
	 *
	 * @param toClose
	 *            A Closeable Object to try closing
	 */
	static void close() {
		context.close();
	}

	public static String getContainerName() {
		return containerName;
	}

	public static Boolean supportsUserMetadata() {
		return supportsUserMetadata;
	}

}
