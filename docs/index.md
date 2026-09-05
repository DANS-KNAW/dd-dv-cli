dd-dv-cli
=========

Dataverse command-line interface

SYNOPSIS
--------

```bash
# Banner management
dv banner-list
dv banner-add -m <message> [ -l <lang> ] [ --dismissible ]
dv banner-delete <id>

# Lock management
dv dataset-lock-add <pid> <lock_type>
dv dataset-lock-delete <pid> <lock_type>
dv dataset-lock-list <pid>

# Storage drivers and direct upload
dv dataset-delete-storage-driver <pid> <storageDriverLabel>
dv dataset-get-storage-driver <pid>
dv dataset-set-storage-driver <pid> <storageDriverLabel>
dv dataset-direct-upload [ --label <label> ] [ -d <directoryLabel> ] \
   [ --description <description> ] [ --resume ] [ --skip-checksum-on-resume ] \
   [ --keep-upload-state ] <pid> <file>
dv storage-drivers-list

# Solr index management
dv index-status
dv index-clear
dv index-all
dv index-dataset <datasetIdOrPid>
dv index-dataverse <dataverseId>
dv index-clear-timestamps

# Reports
dv datasets-get-published [ --after <after> ] [ --archived ] [ --unarchived ] \
   [ --updatecurrent ] [ -o <outputFile> [ -b <batchSize> ] ]
dv filemetas-get-published -o <outputFile> [ --dataset-pid ] [ --label ] \
   [ --directory-label ]
dv datasets-get-storage-size [ -b <base> ] [ --min-size <minSize> ] \ 
   [ --min-files <minFiles> ] [ --max-size <maxSize> ] [ --max-files <maxFiles> ] \
   [ -o <outputFile> ]
dv dataverses-collect-storage-usage [ -m <maxDepth> ] [ -o <outputFile> ] \
   [ -f { csv | json } ]

# Misc management
dv settings-get <key>
dv settings-put <key> <value>
dv settings-list
dv workflow-add -i <inputJson>
dv workflow-list
dv workflow-get <workflowId>
dv workflow-delete <workflowId>
dv workflow-list-defaults
dv workflow-get-default <triggerType>
dv workflow-set-default <triggerType> <workflowId>
dv workflow-delete-default <triggerType>
dv workflow-get-ip-whitelist
dv workflow-set-ip-whitelist <ipWhitelist>
dv workflow-delete-ip-whitelist
dv metadata-export-all [ -f | --force ] [ --older-than <date> ] [ --formats <formats> ]
dv metadata-export-clear-timestamps
dv metadata-export-dataset <datasetIdOrPid> [ --formats <formats> ]
dv notifications-truncate [ --user <userId> ] --keep <keep>
dv dataset-archive-version [ --skip-pids-from <skipPidsFrom> ] [ --force ] \
   [ --allow-rearchive-older-versions ] --report <reportBasename> \
   [ -w <waitBetweenItems> ] { -p <pid> -v <version> | -i <inputFile> }
dv dataset-update-registration-metadata <pid>
dv users-import -i <inputCsv> -k <builtinUsersKey> [ --dry-run ]

```

For more information on a subcommand use:

```bash
dv <subcommand> --help
```
