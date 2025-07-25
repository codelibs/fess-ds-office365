/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

public class TeamsDataStoreTest extends LastaFluteTestCase {

    private static final Logger logger = LogManager.getLogger(TeamsDataStoreTest.class);

    private TeamsDataStore dataStore;

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
        dataStore = new TeamsDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void testNormalizeTextContent() {
        assertEquals("", dataStore.normalizeTextContent(" "));
        assertEquals("", dataStore.normalizeTextContent(""));
        assertEquals("", dataStore.normalizeTextContent(null));
        assertEquals("test", dataStore.normalizeTextContent(" test "));
        assertEquals("test", dataStore.normalizeTextContent(" test <attachment></attachment>"));
    }
}
