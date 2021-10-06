/*
 * Copyright 2012-2021 CodeLibs Project and the Others.
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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.microsoft.graph.authentication.TokenCredentialAuthProvider;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.logger.DefaultLogger;
import com.microsoft.graph.logger.LoggerLevel;
import com.microsoft.graph.models.Channel;
import com.microsoft.graph.models.Chat;
import com.microsoft.graph.models.ChatMessage;
import com.microsoft.graph.models.ConversationMember;
import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.OnenotePage;
import com.microsoft.graph.models.OnenoteSection;
import com.microsoft.graph.models.Site;
import com.microsoft.graph.models.User;
import com.microsoft.graph.options.Option;
import com.microsoft.graph.options.QueryOption;
import com.microsoft.graph.requests.ChannelCollectionPage;
import com.microsoft.graph.requests.ChatCollectionPage;
import com.microsoft.graph.requests.ChatMessageCollectionPage;
import com.microsoft.graph.requests.ConversationMemberCollectionPage;
import com.microsoft.graph.requests.DriveCollectionPage;
import com.microsoft.graph.requests.DriveItemCollectionPage;
import com.microsoft.graph.requests.DriveRequestBuilder;
import com.microsoft.graph.requests.GraphServiceClient;
import com.microsoft.graph.requests.GroupCollectionPage;
import com.microsoft.graph.requests.NotebookCollectionPage;
import com.microsoft.graph.requests.NotebookRequestBuilder;
import com.microsoft.graph.requests.OnenotePageCollectionPage;
import com.microsoft.graph.requests.OnenoteRequestBuilder;
import com.microsoft.graph.requests.OnenoteSectionCollectionPage;
import com.microsoft.graph.requests.OnenoteSectionRequestBuilder;
import com.microsoft.graph.requests.PermissionCollectionPage;
import com.microsoft.graph.requests.UserCollectionPage;
import com.microsoft.graph.serializer.AdditionalDataManager;

import okhttp3.Request;

public class Office365Client implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Office365Client.class);

    protected static final String TENANT_PARAM = "tenant";
    protected static final String CLIENT_ID_PARAM = "client_id";
    protected static final String CLIENT_SECRET_PARAM = "client_secret";
    protected static final String ACCESS_TIMEOUT = "access_timeout";
    protected static final String REFRESH_TOKEN_INTERVAL = "refresh_token_interval";
    protected static final String USER_TYPE_CACHE_SIZE = "user_type_cache_size";
    protected static final String GROUP_ID_CACHE_SIZE = "group_id_cache_size";

    protected static final String INVALID_AUTHENTICATION_TOKEN = "InvalidAuthenticationToken";

    protected GraphServiceClient<Request> client;
    protected Map<String, String> params;
    protected LoadingCache<String, UserType> userTypeCache;
    protected LoadingCache<String, String[]> groupIdCache;

    public Office365Client(final Map<String, String> params) {
        this.params = params;

        final String tenant = params.getOrDefault(TENANT_PARAM, StringUtil.EMPTY);
        final String clientId = params.getOrDefault(CLIENT_ID_PARAM, StringUtil.EMPTY);
        final String clientSecret = params.getOrDefault(CLIENT_SECRET_PARAM, StringUtil.EMPTY);
        if (tenant.isEmpty() || clientId.isEmpty() || clientSecret.isEmpty()) {
            throw new DataStoreException("parameter '" + //
                    TENANT_PARAM + "', '" + //
                    CLIENT_ID_PARAM + "', '" + //
                    CLIENT_SECRET_PARAM + "' is required");
        }
        final ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()//
                .clientId(clientId)//
                .clientSecret(clientSecret)//
                .tenantId(tenant)//
                .build();

        final TokenCredentialAuthProvider tokenCredAuthProvider = new TokenCredentialAuthProvider(clientSecretCredential);

        try {
            client = GraphServiceClient.builder() //
                    .authenticationProvider(tokenCredAuthProvider) //
                    .logger(new DefaultLogger() {
                        @Override
                        public void logDebug(final String message) {
                            if (LoggerLevel.DEBUG == getLoggingLevel()) {
                                logger.debug(message);
                            }
                        }

                        @Override
                        public void logError(final String message, final Throwable t) {
                            if (t instanceof GraphServiceException) {
                                if (((GraphServiceException) t).getResponseCode() == 404) {
                                    logger.debug("[Office365Client] " + message, t);
                                } else {
                                    logger.warn("[Office365Client] " + message, t);
                                }
                            } else {
                                logger.error("[Office365Client] " + message, t);
                            }
                        }
                    })//
                    .buildClient();
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create a client.", e);
        }

        userTypeCache = CacheBuilder.newBuilder().maximumSize(Integer.parseInt(params.getOrDefault(USER_TYPE_CACHE_SIZE, "10000")))
                .build(new CacheLoader<String, UserType>() {
                    @Override
                    public UserType load(final String key) {
                        try {
                            getUser(key, Collections.emptyList());
                            return UserType.USER;
                        } catch (final GraphServiceException e) {
                            if (e.getResponseCode() == 404) {
                                return UserType.GROUP;
                            }
                            logger.warn("Failed to detect an user type.", e);
                        } catch (final Exception e) {
                            logger.warn("Failed to get an user.", e);
                        }
                        return UserType.UNKNOWN;
                    }
                });

        groupIdCache = CacheBuilder.newBuilder().maximumSize(Integer.parseInt(params.getOrDefault(GROUP_ID_CACHE_SIZE, "10000")))
                .build(new CacheLoader<String, String[]>() {
                    @Override
                    public String[] load(final String email) {
                        final List<String> idList = new ArrayList<>();
                        getGroups(Collections.singletonList(new QueryOption("$filter", "mail eq '" + email + "'")), g -> idList.add(g.id));
                        return idList.toArray(new String[idList.size()]);
                    }
                });
    }

    @Override
    public void close() {
        userTypeCache.invalidateAll();
        groupIdCache.invalidateAll();
    }

    public enum UserType {
        USER, GROUP, UNKNOWN;
    }

    public UserType getUserType(final String id) {
        if (StringUtil.isBlank(id)) {
            return UserType.UNKNOWN;
        }
        try {
            return userTypeCache.get(id);
        } catch (final ExecutionException e) {
            logger.warn("Failed to get an user type.", e);
            return UserType.UNKNOWN;
        }
    }

    public InputStream getDriveContent(final Function<GraphServiceClient<Request>, DriveRequestBuilder> builder, final String id) {
        return builder.apply(client).items(id).content().buildRequest().get();
    }

    public PermissionCollectionPage getDrivePermissions(final Function<GraphServiceClient<Request>, DriveRequestBuilder> builder,
            final String id) {
        return builder.apply(client).items(id).permissions().buildRequest().get();
    }

    public DriveItemCollectionPage getDriveItemPage(final Function<GraphServiceClient<Request>, DriveRequestBuilder> builder,
            final String id) {
        if (id == null) {
            return builder.apply(client).root().children().buildRequest().get();
        }
        return builder.apply(client).items(id).children().buildRequest().get();
    }

    public User getUser(final String userId, final List<? extends Option> options) {
        return client.users(userId).buildRequest(options).get();
    }

    public void getUsers(final List<QueryOption> options, final Consumer<User> consumer) {
        UserCollectionPage page = client.users().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    public String[] getGroupIdsByEmail(final String email) {
        try {
            return groupIdCache.get(email);
        } catch (final ExecutionException e) {
            logger.warn("Failed to get group ids.", e);
            return StringUtil.EMPTY_STRINGS;
        }
    }

    public void getGroups(final List<QueryOption> options, final Consumer<Group> consumer) {
        GroupCollectionPage page = client.groups().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    public Group getGroupById(final String id) {
        final List<Group> groupList = new ArrayList<>();
        getGroups(Collections.singletonList(new QueryOption("$filter", "id eq '" + id + "'")), g -> groupList.add(g));
        if (logger.isDebugEnabled()) {
            groupList.forEach(ToStringBuilder::reflectionToString);
        }
        if (groupList.size() == 1) {
            return groupList.get(0);
        }
        return null;
    }

    public NotebookCollectionPage getNotebookPage(final Function<GraphServiceClient<Request>, OnenoteRequestBuilder> builder) {
        return builder.apply(client).notebooks().buildRequest().get();
    }

    protected List<OnenoteSection> getSections(final NotebookRequestBuilder builder) {
        OnenoteSectionCollectionPage page = builder.sections().buildRequest().get();
        final List<OnenoteSection> sections = new ArrayList<>(page.getCurrentPage());
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            sections.addAll(page.getCurrentPage());
        }
        return sections;
    }

    protected List<OnenotePage> getPages(final OnenoteSectionRequestBuilder builder) {
        OnenotePageCollectionPage page = builder.pages().buildRequest().get();
        final List<OnenotePage> pages = new ArrayList<>(page.getCurrentPage());
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            pages.addAll(page.getCurrentPage());
        }
        return pages;
    }

    protected String getSectionContents(final OnenoteRequestBuilder builder, final OnenoteSection section) {
        final StringBuilder sb = new StringBuilder();
        sb.append(section.displayName).append('\n');
        final List<OnenotePage> pages = getPages(builder.sections(section.id));
        Collections.reverse(pages);
        sb.append(pages.stream().map(page -> getPageContents(builder, page)).collect(Collectors.joining("\n")));
        return sb.toString();
    }

    protected String getPageContents(final OnenoteRequestBuilder builder, final OnenotePage page) {
        final StringBuilder sb = new StringBuilder();
        sb.append(page.title).append('\n');
        try (final InputStream in = builder.pages(page.id).content().buildRequest().get()) {
            final TikaExtractor extractor = ComponentUtil.getComponent("tikaExtractor");
            sb.append(extractor.getText(in, null).getContent());
        } catch (final IOException e) {
            logger.warn("Failed to get contents of Page: " + page.title, e);
        }
        return sb.toString();
    }

    public String getNotebookContent(final Function<GraphServiceClient<Request>, OnenoteRequestBuilder> builder, final String id) {
        final List<OnenoteSection> sections = getSections(builder.apply(client).notebooks(id));
        Collections.reverse(sections);
        return sections.stream().map(section -> getSectionContents(builder.apply(client), section)).collect(Collectors.joining("\n"));
    }

    public Site getSite(final String id) {
        return client.sites(StringUtil.isNotBlank(id) ? id : "root").buildRequest().get();
    }

    //    public SiteCollectionPage getSites() {
    //        return client.sites().buildRequest().get();
    //    }
    //
    //    public SiteCollectionPage getNextSitePage(final SiteCollectionPage page) {
    //        if (page.getNextPage() == null) {
    //            return null;
    //        }
    //        return page.getNextPage().buildRequest().get();
    //    }

    public Drive getDrive(final String driveId) {
        return client.drives(driveId).buildRequest().get();
    }

    // for testing
    protected void getDrives(final Consumer<Drive> consumer) {
        DriveCollectionPage page = client.drives().buildRequest().get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    public void geTeams(final List<QueryOption> options, final Consumer<Group> consumer) {
        GroupCollectionPage page = client.groups().buildRequest(options).get();
        final Consumer<Group> filter = g -> {
            final AdditionalDataManager additionalDataManager = g.additionalDataManager();
            if (additionalDataManager != null) {
                final JsonElement jsonElement = additionalDataManager.get("resourceProvisioningOptions");
                final JsonArray array = jsonElement.getAsJsonArray();
                for (int i = 0; i < array.size(); i++) {
                    if ("Team".equals(array.get(i).getAsString())) {
                        consumer.accept(g);
                        return;
                    }
                }
            }
        };
        page.getCurrentPage().forEach(filter);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(filter);
        }
    }

    public void getChannels(final List<QueryOption> options, final Consumer<Channel> consumer, final String teamId) {
        ChannelCollectionPage page = client.teams(teamId).channels().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    public Channel getChannelById(final String teamId, final String id) {
        final List<Channel> channelList = new ArrayList<>();
        getChannels(Collections.singletonList(new QueryOption("$filter", "id eq '" + id + "'")), g -> channelList.add(g), teamId);
        if (logger.isDebugEnabled()) {
            channelList.forEach(ToStringBuilder::reflectionToString);
        }
        if (channelList.size() == 1) {
            return channelList.get(0);
        }
        return null;
    }

    public void getTeamMessages(final List<QueryOption> options, final Consumer<ChatMessage> consumer, final String teamId,
            final String channelId) {
        ChatMessageCollectionPage page = client.teams(teamId).channels(channelId).messages().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    public void getTeamReplyMessages(final List<QueryOption> options, final Consumer<ChatMessage> consumer, final String teamId,
            final String channelId, final String messageId) {
        ChatMessageCollectionPage page = client.teams(teamId).channels(channelId).messages(messageId).replies().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    public void getChats(final List<QueryOption> options, final Consumer<Chat> consumer) {
        ChatCollectionPage page = client.chats().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    public void getChatMessages(final List<QueryOption> options, final Consumer<ChatMessage> consumer, final String chatId) {
        ChatMessageCollectionPage page = client.chats(chatId).messages().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    public void getChatReplyMessages(final List<QueryOption> options, final Consumer<ChatMessage> consumer, final String chatId,
            final String messageId) {
        ChatMessageCollectionPage page = client.chats(chatId).messages(messageId).replies().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    public Chat getChatById(final String id) {
        final List<Chat> chatList = new ArrayList<>();
        getChats(Collections.singletonList(new QueryOption("$filter", "id eq '" + id + "'")), g -> chatList.add(g));
        if (logger.isDebugEnabled()) {
            chatList.forEach(ToStringBuilder::reflectionToString);
        }
        if (chatList.size() == 1) {
            return chatList.get(0);
        }
        return null;
    }

    public void getChatMembers(final List<QueryOption> options, final Consumer<ConversationMember> consumer, final String chatId) {
        ConversationMemberCollectionPage page = client.chats(chatId).members().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }
}
