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
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.callback.IndexUpdateCallbackImpl;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastadi.ContainerTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.codelibs.fess.ds.office365.Office365HelperTest.*;
import static org.codelibs.fess.ds.office365.OneDriveDataStore.getDriveItemContents;

public class OneDriveDataStoreTest extends ContainerTestCase {

    private static final Logger logger = LoggerFactory.getLogger(OneDriveDataStoreTest.class);

    private OneDriveDataStore dataStore;

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
        dataStore = new OneDriveDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void testStoreData() {
        // doStoreData();
    }

    private void doStoreData() {
        final TikaExtractor tikaExtractor = new TikaExtractor();
        tikaExtractor.init();
        ComponentUtil.register(tikaExtractor, "tikaExtractor");

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

        final IndexUpdateCallbackImpl callback = new IndexUpdateCallbackImpl() {
            @Override
            public void store(Map<String, String> paramMap, Map<String, Object> dataMap) {
                logger.debug(dataMap.toString());
            }
        };

        dataStore.storeData(dataConfig, callback, paramMap, scriptMap, defaultDataMap);
    }

    public void testProcessDriveItem() throws Exception {
        final Map<String, String> paramMap = new HashMap<>();
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("name", "files.name");
        scriptMap.put("description", "files.description");
        scriptMap.put("contents", "files.contents");
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
                assertEquals("", dataMap.get("contents"));
                assertEquals(item.file.mimeType, dataMap.get("mimetype"));
                assertEquals(item.createdDateTime.getTime(), dataMap.get("created"));
                assertEquals(item.lastModifiedDateTime.getTime(), dataMap.get("last_modified"));
                assertEquals(item.webUrl, dataMap.get("web_url"));
                latch.countDown();
            }
        }, paramMap, scriptMap, defaultDataMap, null, item);
        latch.await();
    }

    public void testGetDriveItemContents() {
        final DriveItem item = new DriveItem();
        assertEquals("", getDriveItemContents(null, item));
    }

}
