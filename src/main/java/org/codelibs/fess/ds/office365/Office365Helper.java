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
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class Office365Helper {

    // parameters
    public static final String TENANT_PARAM = "tenant";
    public static final String CLIENT_ID_PARAM = "client_id";
    public static final String CLIENT_SECRET_PARAM = "client_secret";

    private static final Logger logger = LoggerFactory.getLogger(Office365Helper.class);

    private static IGraphServiceClient client;

    public static String getAccessToken(final String tenant, final String clientId, final String clientSecret)
            throws MalformedURLException, ExecutionException, InterruptedException {
        final AuthenticationContext context =
                new AuthenticationContext("https://login.microsoftonline.com/" + tenant + "/", false, Executors.newFixedThreadPool(1));
        final AuthenticationResult result =
                context.acquireToken("https://graph.microsoft.com", new ClientCredential(clientId, clientSecret), null).get();
        return result.getAccessToken();
    }

    public static IGraphServiceClient getClient(final String accessToken) {
        return getClient(accessToken, logger);
    }

    public static IGraphServiceClient getClient(final String accessToken, final Logger logger) {
        if (client != null) {
            return client;
        }
        return client = GraphServiceClient.builder() //
                .authenticationProvider(request -> request.addHeader("Authorization", "Bearer " + accessToken)) //
                .logger(new DefaultLogger() {
                    @Override
                    public void logDebug(String message) {
                        switch (getLoggingLevel()) {
                        case DEBUG:
                            logger.debug(message);
                        case ERROR:
                        }
                    }

                    @Override
                    public void logError(String message, Throwable throwable) {
                        switch (getLoggingLevel()) {
                        case DEBUG:
                        case ERROR:
                            logger.error(message, throwable);
                        }
                    }
                }).buildClient();
    }
}
