Office365 Data Store for Fess [![Build Status](https://travis-ci.org/codelibs/fess-ds-office365.svg?branch=master)](https://travis-ci.org/codelibs/fess-ds-office365)
==========================

## Overview

Office365 Data Store is an extension for Fess Data Store Crawling.

## Download

See [Maven Repository](http://central.maven.org/maven2/org/codelibs/fess/fess-ds-office365/).

## Installation

1. Download fess-ds-office365-X.X.X.jar
2. Copy fess-ds-office365-X.X.X.jar to $FESS\_HOME/app/WEB-INF/lib or /usr/share/fess/app/WEB-INF/lib

## Getting Started

### Parameters

```
tenant=********-****-****-****-************
client_id=********-****-****-****-************
client_secret=***********************
```

### Scripts

#### OneDrive

```
title=files.name
content=files.description + "\n" + files.contents
mimetype=files.mimetype
created=files.created
last_modified=files.last_modified
url=files.web_url
role=files.roles
```

| Key | Value |
| --- | --- |
| files.name | The name of the file. |
| files.description | A short description of the file. |
| files.contents | The text contents of the file |
| files.mimetype | The MIME type of the file. |
| files.created | The time at which the file was created. |
| files.last_modified | The last time the file was modified by anyone. |
| files.web_url | A link for opening the file in an editor or viewer in a browser. |
| files.roles | A users/groups who can access the file. |

#### OneNote

```
title=notebooks.name
content=notebooks.contents
created=notebooks.created
last_modified=notebooks.last_modified
url=notebooks.web_url
role=notebooks.roles
```

| Key | Value |
| --- | --- |
| notebooks.name | The name of the notebook. |
| notebooks.contents | The text contents of the notebook |
| notebooks.created | The time at which the notebook was created. |
| notebooks.last_modified | The last time the notebook was modified by anyone. |
| notebooks.web_url | A link for opening the notebook in an editor in a browser. |
| notebooks.roles | A users/groups who can access the notebook. |