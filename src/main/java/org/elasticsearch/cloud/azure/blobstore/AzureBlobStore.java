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
import com.microsoft.azure.storage.LocationMode;
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.common.base.Strings;
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

import java.net.URISyntaxException;

import static org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage.CONTAINER;
import static org.elasticsearch.repositories.azure.AzureRepository.CONTAINER_DEFAULT;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository;

/**
 *
 */
public class AzureBlobStore extends AbstractComponent implements BlobStore {

    private final AzureStorageService client;

    private final String accountName;
    private final LocationMode locMode;
    private final String container;
    private final String repositoryName;

    @Inject
    public AzureBlobStore(RepositoryName name, Settings settings, RepositorySettings repositorySettings,
                          AzureStorageService client) throws URISyntaxException, StorageException {
        super(settings);
        this.client = client;
        this.container = repositorySettings.settings().get("container", settings.get(CONTAINER, CONTAINER_DEFAULT));
        this.repositoryName = name.getName();

        this.accountName = repositorySettings.settings().get(Repository.ACCOUNT, null);
        // NOTE: null account means to use the first one specified in config

        String modeStr = repositorySettings.settings().get(Repository.LOCATION_MODE, null);
        if (modeStr == null) {
            this.locMode = LocationMode.PRIMARY_ONLY;
        } else {
            this.locMode = LocationMode.valueOf(modeStr.toUpperCase());
        }
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

        final String[] accounts = this.getAccounts();
        try {
            if (accounts.length == 0) {
                this.client.deleteFiles(null, this.locMode, container, keyPath);
            } else {
                for (String account : accounts) {
                    this.client.deleteFiles(account, this.locMode, container, keyPath);
                }
            }
        } catch (URISyntaxException | StorageException e) {
            logger.warn("can not remove [{}] in container {{}}: {}", keyPath, container, e.getMessage());
        }
    }

    @Override
    public void close() {
    }

    private String[] getAccounts() {
        if (Strings.isNullOrEmpty(this.accountName ))
            return new String[0];

        return this.accountName.split(",");
    }

    private String getAccount(String blob) {
        final String[] accounts = this.getAccounts();
        if (accounts.length == 0) {
            return null;
        }
        int hash = this.getAccountIndex(blob, accounts.length);
        return accounts[hash];
    }

    private int getAccountIndex(String blob, int numberOfAccounts) {
        int hash = this.hashCode(blob);
        int mod = hash % numberOfAccounts;
        return Math.abs(mod);
    }

    /**
     * Returns a hash code for this blob name.
     *
     * @return  a hash code value for this blob name.
     */
    public int hashCode(String blob) {
        if (Strings.isNullOrEmpty(blob))
            return 0;

        int hash = 0;
        final char chars[] = blob.toCharArray();
        for (char ch : chars) {
            hash += ch;
        }
        return hash;
    }

    public boolean doesContainerExist(String container) {
        final String[] accounts = this.getAccounts();
        if (accounts.length == 0) {
            if (!this.client.doesContainerExist(null, this.locMode, container)) {
                return false;
            }
        } else {
            for (String account : accounts) {
                if (!this.client.doesContainerExist(account, this.locMode, container)) {
                    return false;
                }
            }
        }
        return true;
    }

    public void removeContainer(String container) throws URISyntaxException, StorageException {
        final String[] accounts = this.getAccounts();
        if (accounts.length == 0) {
            this.client.removeContainer(null, this.locMode, container);
        } else {
            for (String account : accounts) {
                this.client.removeContainer(account, this.locMode, container);
            }
        }
    }

    public void createContainer(String container) throws URISyntaxException, StorageException {
        final String[] accounts = this.getAccounts();
        if (accounts.length == 0) {
            this.client.createContainer(null, this.locMode, container);
        } else {
            for (String account : accounts) {
                this.client.createContainer(account, this.locMode, container);
            }
        }
    }

    public void deleteFiles(String container, String path) throws URISyntaxException, StorageException {
        this.client.deleteFiles(this.accountName, this.locMode, container, path);
    }

    public boolean blobExists(String container, String blob) throws URISyntaxException, StorageException {
        String account = this.getAccount(blob);
        return this.client.blobExists(account, this.locMode, container, blob);
    }

    public void deleteBlob(String container, String blob) throws URISyntaxException, StorageException {
        String account = this.getAccount(blob);
        this.client.deleteBlob(account, this.locMode, container, blob);
    }

    public InputStream getInputStream(String container, String blob) throws URISyntaxException, StorageException {
        String account = this.getAccount(blob);
        return this.client.getInputStream(account, this.locMode, container, blob);
    }

    public OutputStream getOutputStream(String container, String blob) throws URISyntaxException, StorageException {
        String account = this.getAccount(blob);
        return this.client.getOutputStream(account, this.locMode, container, blob);
    }

    public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(String container, String keyPath, String prefix) throws URISyntaxException, StorageException {
        final String[] accounts = this.getAccounts();
        ImmutableMap.Builder<String, BlobMetaData> blobsBuilder = ImmutableMap.builder();

        if (accounts.length == 0) {
            ImmutableMap<String, BlobMetaData> blobs = this.client.listBlobsByPrefix(null, this.locMode, container, keyPath, prefix);
            blobsBuilder.putAll(blobs);
        } else {
            for (String account : accounts) {
                ImmutableMap<String, BlobMetaData> blobs = this.client.listBlobsByPrefix(account, this.locMode, container, keyPath, prefix);
                blobsBuilder.putAll(blobs);
            }
        }
        return blobsBuilder.build();
    }
}
