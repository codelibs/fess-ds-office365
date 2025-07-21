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
package org.codelibs.fess.ds.office365.client;

import java.io.Closeable;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;

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
import com.microsoft.graph.models.ChatMessageAttachment;
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

/**
 * This class provides a client for accessing Microsoft Office 365 services using the Microsoft Graph API.
 * It handles authentication, and provides methods for interacting with services like OneDrive, OneNote, and Teams.
 * This client is designed to be used within the Fess data store framework.
 */
public class Office365Client implements Closeable {

    private static final Logger logger = LogManager.getLogger(Office365Client.class);

    /** The parameter name for the Azure AD tenant ID. */
    protected static final String TENANT_PARAM = "tenant";
    /** The parameter name for the Azure AD client ID. */
    protected static final String CLIENT_ID_PARAM = "client_id";
    /** The parameter name for the Azure AD client secret. */
    protected static final String CLIENT_SECRET_PARAM = "client_secret";
    /** The parameter name for the access timeout. */
    protected static final String ACCESS_TIMEOUT = "access_timeout";
    /** The parameter name for the refresh token interval. */
    protected static final String REFRESH_TOKEN_INTERVAL = "refresh_token_interval";
    /** The parameter name for the user type cache size. */
    protected static final String USER_TYPE_CACHE_SIZE = "user_type_cache_size";
    /** The parameter name for the group ID cache size. */
    protected static final String GROUP_ID_CACHE_SIZE = "group_id_cache_size";
    /** The parameter name for the maximum content length. */
    protected static final String MAX_CONTENT_LENGTH = "max_content_length";

    /** Error code for an invalid authentication token. */
    protected static final String INVALID_AUTHENTICATION_TOKEN = "InvalidAuthenticationToken";

    /** The Microsoft Graph service client. */
    protected GraphServiceClient<Request> client;
    /** The data store parameters. */
    protected DataStoreParams params;
    /** A cache for user types. */
    protected LoadingCache<String, UserType> userTypeCache;
    /** A cache for group IDs. */
    protected LoadingCache<String, String[]> groupIdCache;

    /** The maximum content length for extracted text. */
    protected int maxContentLength = -1;

    /**
     * Constructs a new Office365Client with the specified data store parameters.
     *
     * @param params The data store parameters for configuration.
     */
    public Office365Client(final DataStoreParams params) {
        this.params = params;

        final String tenant = params.getAsString(TENANT_PARAM, StringUtil.EMPTY);
        final String clientId = params.getAsString(CLIENT_ID_PARAM, StringUtil.EMPTY);
        final String clientSecret = params.getAsString(CLIENT_SECRET_PARAM, StringUtil.EMPTY);
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

        try {
            maxContentLength = Integer.parseInt(params.getAsString(MAX_CONTENT_LENGTH, Integer.toString(maxContentLength)));
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse {}.", params.getAsString(MAX_CONTENT_LENGTH), e);
        }

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
                                    logger.debug("[Office365Client] {}", message, t);
                                } else {
                                    logger.warn("[Office365Client] {}", message, t);
                                }
                            } else {
                                logger.error("[Office365Client] {}", message, t);
                            }
                        }
                    })//
                    .buildClient();
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create a client.", e);
        }

        userTypeCache = CacheBuilder.newBuilder().maximumSize(Integer.parseInt(params.getAsString(USER_TYPE_CACHE_SIZE, "10000")))
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

        groupIdCache = CacheBuilder.newBuilder().maximumSize(Integer.parseInt(params.getAsString(GROUP_ID_CACHE_SIZE, "10000")))
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

    /**
     * An enumeration of user types in Office 365.
     */
    public enum UserType {
        /** Represents a regular user. */
        USER,
        /** Represents a group. */
        GROUP,
        /** Represents an unknown user type. */
        UNKNOWN;
    }

    /**
     * Retrieves the type of a user (user, group, or unknown) by their ID.
     *
     * @param id The ID of the user or group.
     * @return The UserType of the specified ID.
     */
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

    /**
     * Retrieves the content of a drive item as an InputStream.
     *
     * @param builder A function that builds a DriveRequestBuilder.
     * @param id The ID of the drive item.
     * @return An InputStream containing the content of the drive item.
     */
    public InputStream getDriveContent(final Function<GraphServiceClient<Request>, DriveRequestBuilder> builder, final String id) {
        return builder.apply(client).items(id).content().buildRequest().get();
    }

    /**
     * Retrieves the permissions for a drive item.
     *
     * @param builder A function that builds a DriveRequestBuilder.
     * @param id The ID of the drive item.
     * @return A PermissionCollectionPage containing the permissions.
     */
    public PermissionCollectionPage getDrivePermissions(final Function<GraphServiceClient<Request>, DriveRequestBuilder> builder,
            final String id) {
        return builder.apply(client).items(id).permissions().buildRequest().get();
    }

    /**
     * Retrieves a page of drive items within a drive.
     *
     * @param builder A function that builds a DriveRequestBuilder.
     * @param id The ID of the parent drive item, or null for the root.
     * @return A DriveItemCollectionPage containing the drive items.
     */
    public DriveItemCollectionPage getDriveItemPage(final Function<GraphServiceClient<Request>, DriveRequestBuilder> builder,
            final String id) {
        if (id == null) {
            return builder.apply(client).root().children().buildRequest().get();
        }
        return builder.apply(client).items(id).children().buildRequest().get();
    }

    /**
     * Retrieves a user by their ID.
     *
     * @param userId The ID of the user.
     * @param options A list of options for the request.
     * @return The User object.
     */
    public User getUser(final String userId, final List<? extends Option> options) {
        return client.users(userId).buildRequest(options).get();
    }

    /**
     * Retrieves a list of users, processing each user with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each User object.
     */
    public void getUsers(final List<QueryOption> options, final Consumer<User> consumer) {
        UserCollectionPage page = client.users().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    /**
     * Retrieves the group IDs associated with an email address.
     *
     * @param email The email address to search for.
     * @return An array of group IDs.
     */
    public String[] getGroupIdsByEmail(final String email) {
        try {
            return groupIdCache.get(email);
        } catch (final ExecutionException e) {
            logger.warn("Failed to get group ids.", e);
            return StringUtil.EMPTY_STRINGS;
        }
    }

    /**
     * Retrieves a list of groups, processing each group with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each Group object.
     */
    public void getGroups(final List<QueryOption> options, final Consumer<Group> consumer) {
        GroupCollectionPage page = client.groups().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    /**
     * Retrieves a group by its ID.
     *
     * @param id The ID of the group.
     * @return The Group object, or null if not found.
     */
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

    /**
     * Retrieves a page of notebooks.
     *
     * @param builder A function that builds an OnenoteRequestBuilder.
     * @return a NotebookCollectionPage containing the notebooks.
     */
    public NotebookCollectionPage getNotebookPage(final Function<GraphServiceClient<Request>, OnenoteRequestBuilder> builder) {
        return builder.apply(client).notebooks().buildRequest().get();
    }

    /**
     * Retrieves all sections within a notebook.
     *
     * @param builder The NotebookRequestBuilder for the notebook.
     * @return A list of OnenoteSection objects.
     */
    protected List<OnenoteSection> getSections(final NotebookRequestBuilder builder) {
        OnenoteSectionCollectionPage page = builder.sections().buildRequest().get();
        final List<OnenoteSection> sections = new ArrayList<>(page.getCurrentPage());
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            sections.addAll(page.getCurrentPage());
        }
        return sections;
    }

    /**
     * Retrieves all pages within a section.
     *
     * @param builder The OnenoteSectionRequestBuilder for the section.
     * @return A list of OnenotePage objects.
     */
    protected List<OnenotePage> getPages(final OnenoteSectionRequestBuilder builder) {
        OnenotePageCollectionPage page = builder.pages().buildRequest().get();
        final List<OnenotePage> pages = new ArrayList<>(page.getCurrentPage());
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            pages.addAll(page.getCurrentPage());
        }
        return pages;
    }

    /**
     * Retrieves the contents of a OneNote section as a single string.
     *
     * @param builder The OnenoteRequestBuilder for the OneNote.
     * @param section The OnenoteSection to retrieve contents from.
     * @return A string containing the concatenated contents of the section.
     */
    protected String getSectionContents(final OnenoteRequestBuilder builder, final OnenoteSection section) {
        final StringBuilder sb = new StringBuilder();
        sb.append(section.displayName).append('\n');
        final List<OnenotePage> pages = getPages(builder.sections(section.id));
        Collections.reverse(pages);
        sb.append(pages.stream().map(page -> getPageContents(builder, page)).collect(Collectors.joining("\n")));
        return sb.toString();
    }

    /**
     * Retrieves the contents of a OneNote page as a single string.
     *
     * @param builder The OnenoteRequestBuilder for the OneNote.
     * @param page The OnenotePage to retrieve contents from.
     * @return A string containing the contents of the page.
     */
    protected String getPageContents(final OnenoteRequestBuilder builder, final OnenotePage page) {
        final StringBuilder sb = new StringBuilder();
        sb.append(page.title).append('\n');
        try (final InputStream in = builder.pages(page.id).content().buildRequest().get()) {
            sb.append(ComponentUtil.getExtractorFactory().builder(in, Collections.emptyMap()).maxContentLength(maxContentLength).extract()
                    .getContent());
        } catch (final Exception e) {
            if (!ComponentUtil.getFessConfig().isCrawlerIgnoreContentException()) {
                throw new DataStoreCrawlingException(page.title, "Failed to get contents: " + page.id, e);
            }
            if (logger.isDebugEnabled()) {
                logger.warn("Failed to get contents of Page: {}", page.title, e);
            } else {
                logger.warn("Failed to get contents of Page: {}. {}", page.title, e.getMessage());
            }
        }
        return sb.toString();
    }

    /**
     * Retrieves the content of a notebook as a single string.
     *
     * @param builder A function that builds an OnenoteRequestBuilder.
     * @param id The ID of the notebook.
     * @return A string containing the concatenated contents of the notebook.
     */
    public String getNotebookContent(final Function<GraphServiceClient<Request>, OnenoteRequestBuilder> builder, final String id) {
        final List<OnenoteSection> sections = getSections(builder.apply(client).notebooks(id));
        Collections.reverse(sections);
        return sections.stream().map(section -> getSectionContents(builder.apply(client), section)).collect(Collectors.joining("\n"));
    }

    /**
     * Retrieves a site by its ID.
     *
     * @param id The ID of the site, or "root" for the root site.
     * @return The Site object.
     */
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

    /**
     * Retrieves a drive by its ID.
     *
     * @param driveId The ID of the drive.
     * @return The Drive object.
     */
    public Drive getDrive(final String driveId) {
        return client.drives(driveId).buildRequest().get();
    }

    /**
     * Retrieves all drives, processing each drive with the provided consumer.
     *
     * @param consumer A consumer to process each Drive object.
     */
    // for testing
    protected void getDrives(final Consumer<Drive> consumer) {
        DriveCollectionPage page = client.drives().buildRequest().get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    /**
     * Retrieves a list of Teams, processing each Team with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each Group object representing a Team.
     */
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

    /**
     * Retrieves a list of channels in a Team, processing each channel with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each Channel object.
     * @param teamId The ID of the Team.
     */
    public void getChannels(final List<QueryOption> options, final Consumer<Channel> consumer, final String teamId) {
        ChannelCollectionPage page = client.teams(teamId).channels().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    /**
     * Retrieves a channel by its ID.
     *
     * @param teamId The ID of the Team.
     * @param id The ID of the channel.
     * @return The Channel object, or null if not found.
     */
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

    /**
     * Retrieves a list of messages from a Team channel, processing each message with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each ChatMessage object.
     * @param teamId The ID of the Team.
     * @param channelId The ID of the channel.
     */
    public void getTeamMessages(final List<QueryOption> options, final Consumer<ChatMessage> consumer, final String teamId,
            final String channelId) {
        ChatMessageCollectionPage page = client.teams(teamId).channels(channelId).messages().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    /**
     * Retrieves a list of reply messages to a specific message in a Team channel,
     * processing each message with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each ChatMessage object.
     * @param teamId The ID of the Team.
     * @param channelId The ID of the channel.
     * @param messageId The ID of the message to retrieve replies for.
     */
    public void getTeamReplyMessages(final List<QueryOption> options, final Consumer<ChatMessage> consumer, final String teamId,
            final String channelId, final String messageId) {
        ChatMessageCollectionPage page = client.teams(teamId).channels(channelId).messages(messageId).replies().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    /**
     * Retrieves a list of members in a channel, processing each member with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each ConversationMember object.
     * @param teamId The ID of the Team.
     * @param channelId The ID of the channel.
     */
    public void getChannelMembers(final List<QueryOption> options, final Consumer<ConversationMember> consumer, final String teamId,
            final String channelId) {
        ConversationMemberCollectionPage page = client.teams(teamId).channels(channelId).members().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    /**
     * Retrieves a list of chats, processing each chat with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each Chat object.
     */
    public void getChats(final List<QueryOption> options, final Consumer<Chat> consumer) {
        ChatCollectionPage page = client.chats().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    /**
     * Retrieves a list of messages from a chat, processing each message with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each ChatMessage object.
     * @param chatId The ID of the chat.
     */
    public void getChatMessages(final List<QueryOption> options, final Consumer<ChatMessage> consumer, final String chatId) {
        ChatMessageCollectionPage page = client.chats(chatId).messages().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    /**
     * Retrieves a list of reply messages to a specific message in a chat,
     * processing each message with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each ChatMessage object.
     * @param chatId The ID of the chat.
     * @param messageId The ID of the message to retrieve replies for.
     */
    public void getChatReplyMessages(final List<QueryOption> options, final Consumer<ChatMessage> consumer, final String chatId,
            final String messageId) {
        ChatMessageCollectionPage page = client.chats(chatId).messages(messageId).replies().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    /**
     * Retrieves a chat by its ID.
     *
     * @param id The ID of the chat.
     * @return The Chat object, or null if not found.
     */
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

    /**
     * Retrieves a list of members in a chat, processing each member with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each ConversationMember object.
     * @param chatId The ID of the chat.
     */
    public void getChatMembers(final List<QueryOption> options, final Consumer<ConversationMember> consumer, final String chatId) {
        ConversationMemberCollectionPage page = client.chats(chatId).members().buildRequest(options).get();
        page.getCurrentPage().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    /**
     * Retrieves the content of a chat message attachment as a string.
     *
     * @param attachment The ChatMessageAttachment to retrieve content from.
     * @return A string containing the content of the attachment.
     */
    public String getAttachmentContent(final ChatMessageAttachment attachment) {
        if (attachment.content != null || StringUtil.isBlank(attachment.contentUrl)) {
            return StringUtil.EMPTY;
        }
        // https://learn.microsoft.com/en-us/answers/questions/1072289/download-directly-chat-attachment-using-contenturl
        final String id = "u!" + Base64.getUrlEncoder().encodeToString(attachment.contentUrl.getBytes(Constants.CHARSET_UTF_8))
                .replaceFirst("=+$", StringUtil.EMPTY).replace('/', '_').replace('+', '-');
        try (InputStream in = client.shares(id).driveItem().content().buildRequest().get()) {
            return ComponentUtil.getExtractorFactory().builder(in, null).filename(attachment.name).maxContentLength(maxContentLength)
                    .extract().getContent();
        } catch (final Exception e) {
            if (!ComponentUtil.getFessConfig().isCrawlerIgnoreContentException()) {
                throw new CrawlingAccessException(e);
            }
            if (logger.isDebugEnabled()) {
                logger.warn("Could not get a text.", e);
            } else {
                logger.warn("Could not get a text. {}", e.getMessage());
            }
            return StringUtil.EMPTY;
        }
    }
}
