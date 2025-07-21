Office365 Data Store for Fess
[![Java CI with Maven](https://github.com/codelibs/fess-ds-office365/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-office365/actions/workflows/maven.yml)
==========================

## Overview

Office365 Data Store is an extension for Fess Data Store Crawling.

## Download

See [Maven Repository](https://repo1.maven.org/maven2/org/codelibs/fess/fess-ds-office365/).

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
title=file.name
content=file.description + "\n" + file.contents
mimetype=file.mimetype
created=file.created
last_modified=file.last_modified
url=file.web_url
role=file.roles
```

| Key | Value |
| --- | --- |
| file.name | The name of the file. |
| file.description | A short description of the file. |
| file.contents | The text contents of the file |
| file.mimetype | The MIME type of the file. |
| file.created | The time at which the file was created. |
| file.last_modified | The last time the file was modified by anyone. |
| file.web_url | A link for opening the file in an editor or viewer in a browser. |
| file.roles | A users/groups who can access the file. |

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
