/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cloud.azure.blobstore;

import com.microsoft.azure.storage.StorageException;
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;

import java.net.URISyntaxException;

import static org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage.CONTAINER;
import static org.elasticsearch.repositories.azure.AzureRepository.CONTAINER_DEFAULT;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository;

/**
 *
 */
public class AzureBlobStore extends AbstractComponent implements BlobStore {

    private final AzureStorageService client;

    private final String container;
    private final String repositoryName;
    private final boolean useSecondary;

    @Inject
    public AzureBlobStore(RepositoryName name, Settings settings, RepositorySettings repositorySettings,
                          AzureStorageService client) throws URISyntaxException, StorageException {
        super(settings);
        this.client = client;
        this.container = repositorySettings.settings().get("container", settings.get(CONTAINER, CONTAINER_DEFAULT));
        this.repositoryName = name.getName();
        this.useSecondary = repositorySettings.settings().getAsBoolean(Repository.USE_SECONDARY, false);
    }

    @Override
    public String toString() {
        return container;
    }

    public String container() {
        return container;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new AzureBlobContainer(repositoryName, path, this);
    }

    @Override
    public void delete(BlobPath path) {
        String keyPath = path.buildAsString("/");
        if (!keyPath.isEmpty()) {
            keyPath = keyPath + "/";
        }

        try {
            this.client.deleteFiles(container, keyPath, this.useSecondary);
        } catch (URISyntaxException | StorageException e) {
            logger.warn("can not remove [{}] in container {{}}: {}", keyPath, container, e.getMessage());
        }
    }

    @Override
    public void close() {
    }

    public boolean doesContainerExist(String container)
    {
        return this.client.doesContainerExist(container, this.useSecondary);
    }

    public void removeContainer(String container) throws URISyntaxException, StorageException
    {
        this.client.removeContainer(container, this.useSecondary);
    }

    public void createContainer(String container) throws URISyntaxException, StorageException
    {
        this.client.createContainer(container, this.useSecondary);
    }

    public void deleteFiles(String container, String path) throws URISyntaxException, StorageException
    {
        this.client.deleteFiles(container, path, this.useSecondary);
    }

    public boolean blobExists(String container, String blob) throws URISyntaxException, StorageException
    {
        return this.client.blobExists(container, blob, this.useSecondary);
    }

    public void deleteBlob(String container, String blob) throws URISyntaxException, StorageException
    {
        this.client.deleteBlob(container, blob, this.useSecondary);
    }

    public InputStream getInputStream(String container, String blob) throws URISyntaxException, StorageException
    {
        return this.client.getInputStream(container, blob, this.useSecondary);
    }

    public OutputStream getOutputStream(String container, String blob) throws URISyntaxException, StorageException
    {
        return this.client.getOutputStream(container, blob, this.useSecondary);
    }

    public ImmutableMap<String,BlobMetaData> listBlobsByPrefix(String container, String keyPath, String prefix) throws URISyntaxException, StorageException
    {
        return this.client.listBlobsByPrefix(container, keyPath, prefix, this.useSecondary);
    }
}
