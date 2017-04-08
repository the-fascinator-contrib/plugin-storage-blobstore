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
import java.util.HashSet;
import java.util.Set;

import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.PluginDescription;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonSimpleConfig;

/**
 * <p>
 * This plugin provides storage to cloud and local object stores using the
 * <a href="http://jclouds.apache.org//">Apache JClouds</a> Client.
 * </p>
 *
 * <h3>Configuration</h3>
 * <table border="1">
 * <tr>
 * <th>Option</th>
 * <th>Description</th>
 * <th>Required</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td>provider</td>
 * <td>The name of the provider of the blob store service. List of supported
 * providers available here
 * http://jclouds.apache.org/reference/providers/#blobstore</td>
 * <td><b>Yes</b></td>
 * <td>swift</td>
 * </tr>
 * <tr>
 * <tr>
 * <td>identity</td>
 * <td>User account with read/write access</td>
 * <td><b>No</b></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>password</td>
 * <td>Password for the above user account</td>
 * <td><b>No</b></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>containerName</td>
 * <td>The name of the container to place the objects</td>
 * <td>Yes</td>
 * <td>fascinator</td>
 * </tr>
 * <tr>
 * <td>location</td>
 * <td>Some blobstores allow you to specify a location, such as US-EAST, for
 * where this container will exist.</td>
 * <td>No</td>
 * <td>default location</td>
 * </tr>
 * <tr>
 * <td>fileSystemLocation</td>
 * <td>When using the file-system blob store, this parameter specifies the
 * directory path.</td>
 * <td>No</td>
 * <td>default location</td>
 * </tr>
 * <tr>
 * <td>supportsUserMetadata</td>
 * <td>When using the file-system blob store, for some formats blob store can
 * store additional metadata to a file using User Attributes. Not all formats
 * support this feature and by setting this parameter to false, additional
 * metadata will be stored in it's own file alongside the payload.</td>
 * <td>No</td>
 * <td>true</td>
 * </tr>
 * </table>
 *
 * <h3>Sample configuration</h3>
 *
 * <pre>
 *  {
 *    "storage": {
 *        type": "blobstore",
 *         "blobstore": {
 *        "provider": "filesystem",
 *        "identity": "",
 *        "password": "",
 *        "containerName": "mint",
 *        "fileSystemLocation": "${storage.home}",
 *        "supportsUserMetadata": "false"
 *      }
 *   }
 *  }
 * </pre>
 *
 * @author Andrew Brazzatti
 */
public class BlobStoreStorage implements Storage {

	/** Logger */
	private Logger log = LoggerFactory.getLogger(BlobStoreStorage.class);

	/** System Config */
	private JsonSimpleConfig systemConfig;

	private static String METADATA_PAYLOAD = "TF-OBJ-META";

	/**
	 * Return the ID of this plugin.
	 *
	 * @return String the plugin's ID.
	 */
	@Override
	public String getId() {
		return "blobstore";
	}

	/**
	 * Return the name of this plugin.
	 *
	 * @return String the plugin's name.
	 */
	@Override
	public String getName() {
		return "Blobstore Storage Plugin";
	}

	/**
	 * Public init method for File based configuration.
	 *
	 * @param jsonFile
	 *            The File containing JSON configuration
	 * @throws StorageException
	 *             if any errors occur
	 */
	@Override
	public void init(File jsonFile) throws StorageException {
		try {
			systemConfig = new JsonSimpleConfig(jsonFile);
			BlobStoreClient.init(jsonFile);
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
	@Override
	public void init(String jsonString) throws StorageException {
		try {
			systemConfig = new JsonSimpleConfig(jsonString);
			BlobStoreClient.init(jsonString);
			init();
		} catch (IOException ioe) {
			throw new StorageException("Failed to read string configuration!", ioe);
		}
	}

	/**
	 * Initialisation occurs here
	 *
	 * @throws StorageException
	 *             if any errors occur
	 */
	private void init() throws StorageException {
		// A quick connection test
		BlobStoreClient.getClient();
	}

	/**
	 * Initialisation occurs here
	 *
	 * @throws StorageException
	 *             if any errors occur
	 */
	@Override
	public void shutdown() throws StorageException {
		// Don't need to do anything on shutdown
	}

	/**
	 * Retrieve the details for this plugin
	 *
	 * @return PluginDescription a description of this plugin
	 */
	@Override
	public PluginDescription getPluginDetails() {
		return new PluginDescription(this);
	}

	/**
	 * Create a new object in storage. An object identifier may be provided, or
	 * a null value will try to have Fedora auto-generate the new OID.
	 *
	 * @param oid
	 *            the Object ID to use during creation, null is allowed
	 * @return DigitalObject the instantiated DigitalObject created
	 * @throws StorageException
	 *             if any errors occur
	 */
	@Override
	public synchronized DigitalObject createObject(String oid) throws StorageException {
		// log.debug("createObject({})", oid);
		if (oid == null) {
			throw new StorageException("Error; Null OID recieved");
		}

		// Can we see object?
		if (BlobStoreClient.getClient().directoryExists(BlobStoreClient.getContainerName(), oid)) {
			throw new StorageException("Error; object '" + oid + "' already exists in Blobstore");
		}

		BlobStoreClient.getClient().createDirectory(BlobStoreClient.getContainerName(), oid);

		// Instantiate and return
		return new BlobStoreDigitalObject(oid);
	}

	/**
	 * Get the indicated object from storage.
	 *
	 * @param oid
	 *            the Object ID to retrieve
	 * @return DigitalObject the instantiated DigitalObject requested
	 * @throws StorageException
	 *             if any errors occur
	 */
	@Override
	public DigitalObject getObject(String oid) throws StorageException {

		if (oid == null) {
			throw new StorageException("Error; Null OID received");
		}
		if (!BlobStoreClient.getClient().directoryExists(BlobStoreClient.getContainerName(), oid)) {
			throw new StorageException("Error; Object with OID does not exist in storage");
		}
		// Instantiate and return
		return new BlobStoreDigitalObject(oid);

	}

	/**
	 * Remove the indicated object from storage.
	 *
	 * @param oid
	 *            the Object ID to remove from storage
	 * @throws StorageException
	 *             if any errors occur
	 */
	@Override
	public synchronized void removeObject(String oid) throws StorageException {
		// log.debug("removeObject({})", oid);
		if (oid == null) {
			throw new StorageException("Error; Null OID recieved");
		}

		try {
			removeBlobStoreObject(oid);
		} catch (Exception e) {
			throw new StorageException("Unable to remove object", e);
		}
	}

	/**
	 * Perform the actual removal from Fedora
	 *
	 * @param fedoraPid
	 *            the Fedora PID to remove from storage
	 * @throws StorageException
	 *             if any errors occur
	 */
	private void removeBlobStoreObject(String oid) throws StorageException {
		if (BlobStoreClient.getClient().directoryExists(BlobStoreClient.getContainerName(), oid)) {
			BlobStoreClient.getClient().deleteDirectory(BlobStoreClient.getContainerName(), oid);
		} else {
			throw new StorageException("Object " + oid + " doesn't exist to be deleted");
		}
	}

	/**
	 * Return a list of Object IDs currently in storage.
	 *
	 * @return Set<String> A Set containing all the OIDs in storage.
	 */
	@Override
	public Set<String> getObjectIdList() {
		Set<String> objectIdList = new HashSet<String>();
		PageSet<? extends StorageMetadata> pageSet;
		try {
			pageSet = BlobStoreClient.getClient().list(BlobStoreClient.getContainerName());

			for (StorageMetadata storageMetadata : pageSet) {
				if (storageMetadata.getType() == StorageType.FOLDER
						|| storageMetadata.getType() == StorageType.RELATIVE_PATH) {
					objectIdList.add(storageMetadata.getName());
				}
			}

		} catch (StorageException e) {
			log.error("Error getting list of object ids", e);
		}

		return objectIdList;
	}

}
