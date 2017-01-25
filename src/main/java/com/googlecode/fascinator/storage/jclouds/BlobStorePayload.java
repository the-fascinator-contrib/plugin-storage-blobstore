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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kahadb.util.ByteArrayInputStream;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteSource;
import com.googlecode.fascinator.api.storage.PayloadType;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.MimeTypeUtil;
import com.googlecode.fascinator.common.storage.impl.GenericPayload;

/**
 * Maps a BlobStore Blob to a Fascinator payload.
 *
 * @author Andrew Brazzatti
 */
public class BlobStorePayload extends GenericPayload {
	private static final String CONTENT_TYPE_KEY = "contenttype";

	private static final String LABEL_KEY = "label";

	private static final String PAYLOAD_TYPE_KEY = "payloadtype";

	private static final String METADATA_SUFFIX = ".meta";

	/** Logging */
	private Logger log = LoggerFactory.getLogger(BlobStorePayload.class);

	private Blob blob;
	private String oid;

	private String location;

	public BlobStorePayload(String oid, String pid) throws StorageException {
		super(pid);
		this.oid = oid;
		location = oid + "/" + pid;
	}

	private void loadBlob() throws StorageException {
		if (BlobStoreClient.getClient().blobExists(BlobStoreClient.getContainerName(), location)) {
			blob = BlobStoreClient.getClient().getBlob(BlobStoreClient.getContainerName(), location);

			Map<String, String> userMetaData;
			try {
				userMetaData = getUserMetadata(blob);

				if (StringUtils.isNotEmpty(userMetaData.get(PAYLOAD_TYPE_KEY))) {
					setType(PayloadType.valueOf(userMetaData.get(PAYLOAD_TYPE_KEY)));
				}
				setLabel(userMetaData.get(LABEL_KEY));
				setContentType(userMetaData.get(CONTENT_TYPE_KEY));
			} catch (IOException e) {
				throw new StorageException("Failed to retrieve payload metadata", e);
			}
		} else {
			BlobStore blobStore = BlobStoreClient.getClient();
			blob = blobStore.blobBuilder(location).build();
		}
	}

	private Map<String, String> getUserMetadata(Blob blob2) throws StorageException, IOException {
		if (blob == null) {
			loadBlob();
		}
		if (BlobStoreClient.supportsUserMetadata()) {
			Map<String, String> properties = blob.getMetadata().getUserMetadata();

			return properties;
		} else {
			Blob metaBlob = BlobStoreClient.getClient().getBlob(BlobStoreClient.getContainerName(),
					blob.getMetadata().getName() + METADATA_SUFFIX);
			Properties properties = new Properties();
			properties.load(metaBlob.getPayload().openStream());

			return new HashMap<String, String>((Map) properties);
		}
	}

	/**
	 * Gets the input stream to access the content for this payload
	 *
	 * @return an input stream
	 * @throws IOException
	 *             if there was an error reading the stream
	 */
	@Override
	public InputStream open() throws StorageException {
		if (blob == null) {
			loadBlob();
		}
		try {
			return blob.getPayload().openStream();
		} catch (IOException ex) {
			log.error("Error accessing Blob store: ", ex);
			return null;
		}
	}

	/**
	 * the input stream for this payload
	 *
	 * @throws StorageException
	 *             if there was an error closing the stream
	 */
	@Override
	public void close() throws StorageException {
		if (blob != null) {
			try {
				writePayload(blob.getPayload().openStream(), false);
			} catch (Exception e) {
				throw new StorageException("Failed to close stream", e);
			}
			blob = BlobStoreClient.getClient().getBlob(BlobStoreClient.getContainerName(),
					blob.getMetadata().getName());
		}
	}

	/**
	 * Return the timestamp when the payload was last modified
	 *
	 * @returns Long: The last modified date of the payload, or NULL if unknown
	 */
	@Override
	public Long lastModified() {
		try {
			if (blob == null) {
				loadBlob();
			}
			if (blob.getMetadata().getLastModified() == null) {
				// Payload objects stored into the blob store may not have their
				// timestamp so we'll re-fetch it

				blob = BlobStoreClient.getClient().getBlob(BlobStoreClient.getContainerName(),
						blob.getMetadata().getName());

			}

			return blob.getMetadata().getLastModified().getTime();
		} catch (StorageException e) {
			throw new RuntimeException("Failed to get payload size", e);
		}
	}

	/**
	 * Return the size of the payload in bytes
	 *
	 * @returns Integer: The file size in bytes, or NULL if unknown
	 */
	@Override
	public Long size() {
		try {
			if (blob == null) {
				loadBlob();
			}
			if (blob.getPayload().getContentMetadata().getContentLength() == null) {
				// Payload objects stored into the blob store may not have their
				// content length so we need to re-fetch it

				blob = BlobStoreClient.getClient().getBlob(BlobStoreClient.getContainerName(),
						blob.getMetadata().getName());

			}
			return blob.getPayload().getContentMetadata().getContentLength();
		} catch (StorageException e) {
			throw new RuntimeException("Failed to get payload size", e);
		}
	}

	public void writePayload(InputStream in) throws StorageException {
		writePayload(in, true);
	}

	@Override
	public String getLabel() {
		try {
			if (blob == null) {
				loadBlob();
			}
		} catch (StorageException e) {
			throw new RuntimeException("Failed to get payload label", e);
		}
		return super.getLabel();
	}

	@Override
	public PayloadType getType() {
		try {
			if (blob == null) {
				loadBlob();
			}
		} catch (StorageException e) {
			throw new RuntimeException("Failed to get payload type", e);
		}
		return super.getType();
	}

	@Override
	public void setType(PayloadType type) {
		try {
			if (blob == null) {
				loadBlob();
			}
		} catch (StorageException e) {
			throw new RuntimeException("Failed to set payload type", e);
		}
		super.setType(type);
	}

	@Override
	public String getContentType() {
		try {
			if (blob == null) {
				loadBlob();
			}
		} catch (StorageException e) {
			throw new RuntimeException("Failed to get payload type", e);
		}
		return super.getContentType();
	}

	public void writePayload(InputStream in, boolean determineContentType) throws StorageException {
		if (getLabel() == null) {
			setLabel(getId());
		}
		if (getType() == null) {
			setType(PayloadType.Source);
		}
		byte[] bytes = null;
		if (determineContentType) {
			// We need to read the stream locally to determine the content type
			try {

				bytes = IOUtils.toByteArray(in);
				setContentType(MimeTypeUtil.getMimeType(bytes,  getId()));

			} catch (IOException e) {
				throw new StorageException("Failed to determine content type", e);
			}
		}
		String payloadPath = oid + "/" + getId();

		Map<String, String> userMetadata = new HashMap<String, String>();

		userMetadata.put("id", getId());
		userMetadata.put(PAYLOAD_TYPE_KEY, getType().toString());
		userMetadata.put(LABEL_KEY, getLabel());
		userMetadata.put("linked", String.valueOf(isLinked()));
		// Sometimes we just can't get it
		if (getContentType() != null) {
			userMetadata.put(CONTENT_TYPE_KEY, getContentType());
		} else {
			userMetadata.put(CONTENT_TYPE_KEY, MimeTypeUtil.DEFAULT_MIME_TYPE);
		}

		BlobStore blobStore = BlobStoreClient.getClient();

		if (bytes != null) {

			blob = blobStore.blobBuilder(payloadPath).userMetadata(userMetadata).build();
			blob.setPayload(ByteSource.wrap(bytes));
		} else {
			blob = blobStore.blobBuilder(payloadPath).userMetadata(userMetadata).build();
			blob.setPayload(in);
		}

		blobStore.putBlob(BlobStoreClient.getContainerName(), blob);
		if (!BlobStoreClient.supportsUserMetadata()) {
			writePayloadMetadata(userMetadata);
		}

	}

	private void writePayloadMetadata(Map<String, String> userMetadata) throws StorageException {
		Properties metadata = new Properties();
		for (String key : userMetadata.keySet()) {
			metadata.setProperty(key, userMetadata.get(key));
		}
		BlobStore blobStore = BlobStoreClient.getClient();
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {
			metadata.store(output, null);
		} catch (IOException e) {
			throw new StorageException("Failed to write payload metadata", e);
		}
		InputStream input = new ByteArrayInputStream(output.toByteArray());
		Blob metadataBlob = blobStore.blobBuilder(blob.getMetadata().getName() + METADATA_SUFFIX).build();
		metadataBlob.setPayload(input);
		blobStore.putBlob(BlobStoreClient.getContainerName(), metadataBlob);
	}

}
