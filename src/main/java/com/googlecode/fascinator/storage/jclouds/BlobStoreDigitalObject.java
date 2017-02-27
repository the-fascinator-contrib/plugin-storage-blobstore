/*
 * The Fascinator - JClouds BlobStore storage plugin
 * Copyright (C) 2016 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.PayloadType;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.storage.impl.GenericDigitalObject;

/**
 * Maps a Blob Store Blob in a container to a Fascinator digital object.
 *
 * @author Andrew Brazzatti
 */
public class BlobStoreDigitalObject extends GenericDigitalObject {

	private static String METADATA_PAYLOAD = "TF-OBJ-META";

	// private static String METADATA_LABEL = "The Fascinator Indexer Metadata";

	/** Logging */
	private Logger log = LoggerFactory.getLogger(BlobStoreDigitalObject.class);

	public BlobStoreDigitalObject(String oid) {
		super(oid);
		try {
			buildManifest();
		} catch (StorageException e) {
			new RuntimeException("Failed to build manifest", e);
		}
	}

	/**
	 * Created a stored payload in storage as a datastream of this Object. This
	 * is the only payload supported by this plugin.
	 *
	 * @param pid
	 *            the Payload ID to use
	 * @param in
	 *            an InputStream containing the data to store
	 * @return Payload the Payload Object
	 * @throws StorageException
	 *             if any errors occur
	 */
	@Override
	public synchronized Payload createStoredPayload(String pid, InputStream in) throws StorageException {
		// log.debug("createStoredPayload({},{})", getId(), pid);
		if (pid == null || in == null) {
			throw new StorageException("Error; Null parameter recieved");
		}

		Payload payload = createPayload(pid, in, false);
		return payload;

	}

	private Payload createPayload(String pid, InputStream in, boolean b) throws StorageException {
		Map<String, Payload> manifest = getManifest();
		if (manifest.containsKey(pid)) {
			throw new StorageException("ID '" + pid + "' already exists in manifest.");
		}

		// Payload creation
		BlobStorePayload payload = new BlobStorePayload(getId(), pid);
		if (METADATA_PAYLOAD.equals(pid)) {
			payload.setType(PayloadType.Annotation);
		} else if (getSourceId() == null) {
			payload.setType(PayloadType.Source);
			setSourceId(pid);
		}

		payload.writePayload(in);
		// re-get the payload from storage to populate store metadata
		manifest.put(pid, null);
		payload = (BlobStorePayload) getPayload(pid);
		manifest.put(pid, payload);
		updateObjectManifest();

		return payload;

	}

	/**
	 * Created a linked payload in storage as a datastream of this Object.
	 * Linked payloads are not truly supported by this plugin, and the provided
	 * File will instead be ingested into the BlobStore as stored payloads.
	 *
	 * @param pid
	 *            the Payload ID to use
	 * @param linkPath
	 *            a file path to the file to store
	 * @return Payload the Payload Object
	 * @throws StorageException
	 *             if any errors occur
	 */
	@Override
	public synchronized Payload createLinkedPayload(String pid, String linkPath) throws StorageException {
		log.warn("This storage plugin does not support linked payloads..." + " converting to stored.");

		try {
			FileInputStream in = new FileInputStream(linkPath);
			return createStoredPayload(pid, in);
		} catch (FileNotFoundException fnfe) {
			throw new StorageException(fnfe);
		}
	}

	/**
	 * Retrieve and instantiate the requested payload in this Object.
	 *
	 * @param pid
	 *            the Payload ID to retrieve
	 * @return Payload the Payload Object
	 * @throws StorageException
	 *             if any errors occur
	 */
	@Override
	public synchronized Payload getPayload(String pid) throws StorageException {
		// log.debug("getPayload({},{})", getId(), pid);
		if (pid == null) {
			throw new StorageException("Error; Null PID recieved");
		}

		// Confirm we actually have this payload first
		Map<String, Payload> manifest = getManifest();
		if (!manifest.containsKey(pid)) {
			throw new StorageException("pID '" + pid + "': was not found");

		}

		return new BlobStorePayload(getId(), pid);
	}

	/**
	 * Remove the requested payload from this Object.
	 *
	 * @param pid
	 *            the Payload ID to retrieve
	 * @throws StorageException
	 *             if any errors occur
	 */
	@Override
	public synchronized void removePayload(String pid) throws StorageException {
		if (pid == null) {
			throw new StorageException("Error; Null PID recieved");
		}

		// Confirm we actually have this payload first
		Map<String, Payload> manifest = getManifest();
		if (!manifest.containsKey(pid)) {
			throw new StorageException("pID '" + pid + "': was not found");

		}
		manifest.remove(pid);

		BlobStoreClient.getClient().removeBlob(BlobStoreClient.getContainerName(), getId() + "/" + pid);

	}

	/**
	 * Update a stored payload in storage for this Object.
	 *
	 * @param pid
	 *            the Payload ID to use
	 * @param in
	 *            an InputStream containing the data to store
	 * @return Payload the updated Payload Object
	 * @throws StorageException
	 *             if any errors occur
	 */
	@Override
	public synchronized Payload updatePayload(String pid, InputStream in) throws StorageException {
		// log.debug("updatePayload({},{})", getId(), pid);
		if (pid == null || in == null) {
			throw new StorageException("Error; Null parameter recieved");
		}

		// Double-check it actually exists before we try to modify it
		Map<String, Payload> manifest = getManifest();
		if (!manifest.containsKey(pid)) {
			throw new StorageException("pID '" + pid + "': was not found");
		}
		BlobStorePayload payload = new BlobStorePayload(getId(), pid);
		payload.writePayload(in);
		payload = (BlobStorePayload) getPayload(pid);
		manifest.put(pid, payload);
		return payload;
	}

	private void buildManifest() throws StorageException {
		Map<String, Payload> manifest = getManifest();
		BlobStore blobStore = BlobStoreClient.getClient();
		Blob manifestBlob = blobStore.getBlob(BlobStoreClient.getContainerName(), getId() + "/object-manifest");
		if (manifestBlob == null) {
			String manifestString = "{}";

			manifestBlob = blobStore.blobBuilder(getId() + "/object-manifest").build();
			manifestBlob.setPayload(manifestString);
			blobStore.putBlob(BlobStoreClient.getContainerName(), manifestBlob);
		} else {
			InputStreamReader isr;
			try {

				JsonSimple manifestObject = new JsonSimple(manifestBlob.getPayload().openStream());
				if (manifestObject.getArray("items") != null) {
					for (Object itemObject : manifestObject.getArray("items")) {
						JsonObject item = (JsonObject) itemObject;
						String name = (String) item.get("name");
						Payload payload = new BlobStorePayload(getId(), name);
						if (PayloadType.Source.equals(payload.getType())) {
							setSourceId(name);
						}
						manifest.put(name, payload);
					}
				}
			} catch (IOException e) {
				new StorageException("Failed to build manifest", e);
			}
		}
	}

	private void updateObjectManifest() throws StorageException {
		Map<String, Payload> manifest = getManifest();
		BlobStore blobStore = BlobStoreClient.getClient();
		JsonObject objectManifest = new JsonObject();
		JSONArray objectsArray = new JSONArray();
		// String manifestString = "";
		for (String manifestItem : manifest.keySet()) {

			if (!manifestItem.endsWith(".meta")) {
				JsonObject manifestItemObject = new JsonObject();
				manifestItemObject.put("name", manifestItem);
				if (manifestItem.equals(getSourceId())) {
					manifestItemObject.put("type", PayloadType.Source);
				} else {
					manifestItemObject.put("type", "other");
				}
				objectsArray.add(manifestItemObject);
			}
		}
		objectManifest.put("items", objectsArray);
		Blob manifestBlob = blobStore.blobBuilder(getId() + "/object-manifest").build();
		manifestBlob.setPayload(new JsonSimple(objectManifest).toString(true));
		blobStore.putBlob(BlobStoreClient.getContainerName(), manifestBlob);

	}

}
