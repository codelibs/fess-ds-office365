/*
 * Copyright 2012-2018 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.office365;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.graph.logger.DefaultLogger;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class Office365DataStore extends AbstractDataStore {

    // parameters
    private static final String TENANT_PARAM = "tenant";
    private static final String CLIENT_ID_PARAM = "client_id";
    private static final String CLIENT_SECRET_PARAM = "client_secret";

    // scripts
    private static final String FILES = "files";
    private static final String FILES_NAME = "name";
    private static final String FILES_DESCRIPTION = "description";
    private static final String FILES_CONTENTS = "contents";
    private static final String FILES_MIMETYPE = "mimetype";
    private static final String FILES_CREATED = "created";
    private static final String FILES_LAST_MODIFIED = "last_modified";
    private static final String FILES_WEB_URL = "web_url";

    private static final Logger logger = LoggerFactory.getLogger(Office365DataStore.class);

    protected String getName() {
        return "Office365";
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final String tenant = paramMap.getOrDefault(TENANT_PARAM, "");
        final String clientId = paramMap.getOrDefault(CLIENT_ID_PARAM, "");
        final String clientSecret = paramMap.getOrDefault(CLIENT_SECRET_PARAM, "");

        if (tenant.isEmpty() || clientId.isEmpty() || clientSecret.isEmpty()) {
            logger.warn("parameter '" + //
                    TENANT_PARAM + "', '" + //
                    CLIENT_ID_PARAM + "', '" + //
                    CLIENT_SECRET_PARAM + "' is required");
            return;
        }

        final String accessToken;
        try {
            accessToken = getAccessToken(tenant, clientId, clientSecret);
        } catch (final MalformedURLException | ExecutionException | InterruptedException e) {
            logger.warn("failed to get access token.", e);
            return;
        }

        final IGraphServiceClient client = getClient(accessToken);

    }

    protected void processDriveItem(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final DriveItem item) {
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
        final Map<String, Object> filesMap = new HashMap<>();

        filesMap.put(FILES_NAME, item.name);
        filesMap.put(FILES_DESCRIPTION, item.description);
        filesMap.put(FILES_CONTENTS, getDriveItemContents(item));
        filesMap.put(FILES_MIMETYPE, item.file.mimeType);
        filesMap.put(FILES_CREATED, item.createdDateTime.getTime());
        filesMap.put(FILES_LAST_MODIFIED, item.lastModifiedDateTime.getTime());
        filesMap.put(FILES_WEB_URL, item.webUrl);
        resultMap.put(FILES, filesMap);

        try {
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }
            callback.store(paramMap, dataMap);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : " + dataMap, e);
        }
    }

    protected static String getDriveItemContents(final DriveItem item) {
        return "";
    }

    protected static String getAccessToken(final String tenant, final String clientId, final String clientSecret)
            throws MalformedURLException, ExecutionException, InterruptedException {
        final AuthenticationContext context =
                new AuthenticationContext("https://login.microsoftonline.com/" + tenant + "/", false, Executors.newFixedThreadPool(1));
        final AuthenticationResult result =
                context.acquireToken("https://graph.microsoft.com", new ClientCredential(clientId, clientSecret), null).get();
        return result.getAccessToken();
    }

    protected static IGraphServiceClient getClient(final String accessToken) {
        return GraphServiceClient.builder() //
                .authenticationProvider(request -> request.addHeader("Authorization", "Bearer " + accessToken)) //
                .logger(new DefaultLogger() {
                    @Override
                    public void logDebug(String message) {
                        logger.debug(message);
                    }

                    @Override
                    public void logError(String message, Throwable throwable) {
                        logger.error(message, throwable);
                    }
                }).buildClient();
    }

}
