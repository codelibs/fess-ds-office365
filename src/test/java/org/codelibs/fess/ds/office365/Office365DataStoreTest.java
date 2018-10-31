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

import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.File;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.User;
import com.microsoft.graph.options.Option;
import com.microsoft.graph.options.QueryOption;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.callback.IndexUpdateCallbackImpl;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastadi.ContainerTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Office365DataStoreTest extends ContainerTestCase {

    private static final Logger logger = LoggerFactory.getLogger(Office365DataStoreTest.class);

    private Office365DataStore dataStore;

    // for test
    private static final String tenant = "";
    private static final String clientId = "";
    private static final String clientSecret = "";

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final TikaExtractor tikaExtractor = new TikaExtractor();
        tikaExtractor.init();
        ComponentUtil.register(tikaExtractor, "tikaExtractor");
        dataStore = new Office365DataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void testProduction() throws Exception {
        // doProductionTest();
    }

    private void doProductionTest() throws Exception {
        doStoreDataTest();
        doGetAccessTokenTest();
        doGetClientTest();
        doAPITest();
    }

    private void doGetAccessTokenTest() throws Exception {
        Office365DataStore.getAccessToken(tenant, clientId, clientSecret);
    }

    private void doGetClientTest() throws Exception {
        final String accessToken = Office365DataStore.getAccessToken(tenant, clientId, clientSecret);
        Office365DataStore.getClient(accessToken);
    }

    private void doAPITest() throws Exception {
        final IGraphServiceClient client = Office365DataStore.getClient(Office365DataStore.getAccessToken(tenant, clientId, clientSecret));
        final List<Option> options = new ArrayList<>();
        options.add(new QueryOption("$select", "id,displayName"));
        client.users().buildRequest(options).get().getCurrentPage().forEach(u -> {
            final User user = client.users(u.id).buildRequest(Collections.singletonList(new QueryOption("$select", "mySite"))).get();
            if (user.mySite != null) {
                logger.debug("Files in " + u.displayName + "'s drive:");
                client.users(u.id).drive().root().children().buildRequest().get().getCurrentPage().forEach(item -> {
                    logger.debug(item.name);
                });
                logger.debug("----------");
            }
        });

        logger.debug("Files in " + client.drive().buildRequest().get().name + "'s drive:");
        client.drive().root().children().buildRequest().get().getCurrentPage().forEach(item -> {
            logger.debug(item.name);
        });
        logger.debug("----------");

        options.add(new QueryOption("$filter", "groupTypes/any(c:c eq 'Unified')"));
        client.groups().buildRequest(options).get().getCurrentPage().forEach(g -> {
            logger.debug("Files in " + g.displayName + "'s drive:");
            client.groups(g.id).drive().root().children().buildRequest().get().getCurrentPage().forEach(item -> {
                logger.debug(item.name);
            });
            logger.debug("----------");
        });
    }

    private void doStoreDataTest() {
        final DataConfig dataConfig = new DataConfig();
        final Map<String, String> paramMap = new HashMap<>();
        paramMap.put("tenant", tenant);
        paramMap.put("client_id", clientId);
        paramMap.put("client_secret", clientSecret);
        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();

        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        scriptMap.put(fessConfig.getIndexFieldTitle(), "files.name");
        scriptMap.put(fessConfig.getIndexFieldContent(), "files.description + \"\\n\"+ files.contents");
        scriptMap.put(fessConfig.getIndexFieldMimetype(), "files.mimetype");
        scriptMap.put(fessConfig.getIndexFieldCreated(), "files.created");
        scriptMap.put(fessConfig.getIndexFieldLastModified(), "files.last_modified");
        scriptMap.put(fessConfig.getIndexFieldContentLength(), "files.size");
        scriptMap.put(fessConfig.getIndexFieldUrl(), "files.web_url");

        dataStore.storeData(dataConfig, new IndexUpdateCallbackImpl() {
            @Override
            public void store(Map<String, String> paramMap, Map<String, Object> dataMap) {
                logger.debug(dataMap.toString());
            }
        }, paramMap, scriptMap, defaultDataMap);
    }

    public void testProcessDriveItem() throws Exception {
        final Map<String, String> paramMap = new HashMap<>();
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("name", "files.name");
        scriptMap.put("description", "files.description");
        scriptMap.put("mimetype", "files.mimetype");
        scriptMap.put("created", "files.created");
        scriptMap.put("last_modified", "files.last_modified");
        scriptMap.put("web_url", "files.web_url");
        final Map<String, Object> defaultDataMap = new HashMap<>();
        final DriveItem item = new DriveItem();
        item.name = "hoge";
        item.description = "hogehoge";
        item.file = new File();
        item.file.mimeType = "fuga";
        item.createdDateTime = Calendar.getInstance();
        item.lastModifiedDateTime = Calendar.getInstance();
        item.webUrl = "piyo";
        final CountDownLatch latch = new CountDownLatch(1);
        dataStore.processDriveItem(new IndexUpdateCallbackImpl() {
            @Override
            public void store(Map<String, String> paramMap, Map<String, Object> dataMap) {
                assertEquals(item.name, dataMap.get("name"));
                assertEquals(item.description, dataMap.get("description"));
                assertEquals(item.file.mimeType, dataMap.get("mimetype"));
                assertEquals(item.createdDateTime.getTime(), dataMap.get("created"));
                assertEquals(item.lastModifiedDateTime.getTime(), dataMap.get("last_modified"));
                assertEquals(item.webUrl, dataMap.get("web_url"));
                latch.countDown();
            }
        }, paramMap, scriptMap, defaultDataMap, null, null, item);
        latch.await();
    }

    public void testGetDriveItemContents() {
        final DriveItem item = new DriveItem();
        assertEquals("", Office365DataStore.getDriveItemContents(null, null, item));
    }

}
