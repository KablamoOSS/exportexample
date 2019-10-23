# Driver for the athena wrapper

CLI driver to demonstrating how to make a query to aws athena using the shim.

Typical usage is something like:

```
# we assume you appropriate entries in ~/.aws/{config,credentials} to query athena.
export AWS_PROFILE=sekret_aws_profile
./cli -skip-header-row  somedatabase  'SELECT * FROM staging_table LIMIT 10'  s3://the-bill-gates-bucket/
```

# Misc notes about using athena using the awscli command

From a shell you can query athena to get 10 or so rows from a table like so:

```bash
aws athena start-query-execution --profile=sekret_aws_profile --query-string 'SELECT * from staging_table LIMIT 10' --query-execution-context Database=somedatabase  --result-configuration OutputLocation=s3://the-bill-gates-bucket/
```

Which gives you the query execution ID:

```json
{
    "QueryExecutionId": "11493eb5-cf58-4839-8e55-13eb152c3d70"
}
```

Which you can then poll for status:

```bash
aws athena get-query-execution --profile=sekret_aws_profile --query-execution-id 11493eb5-cf58-4839-8e55-13eb152c3d70
```

Providing status like so:

```json
{
    "QueryExecution": {
        "QueryExecutionId": "11493eb5-cf58-4839-8e55-13eb152c3d70",
        "Query": "SELECT * from staging_table LIMIT 10",
        "StatementType": "DML",
        "ResultConfiguration": {
            "OutputLocation": "s3://the-bill-gates-bucket/11493eb5-cf58-4839-8e55-13eb152c3d70.csv"
        },
        "QueryExecutionContext": {
            "Database": "somedatabase"
        },
        "Status": {
            "State": "SUCCEEDED",
            "SubmissionDateTime": 1569840037.677,
            "CompletionDateTime": 1569840040.216
        },
        "Statistics": {
            "EngineExecutionTimeInMillis": 2303,
            "DataScannedInBytes": 93419
        },
        "WorkGroup": "primary"
    }
}
```

When the state has SUCCEEDED you can then fetch the results:

```bash
aws athena get-query-results --profile=sekret_aws_profile --query-execution-id  69151b11-fc2c-4db8-98f6-94ee4913a86a
```

Which dumps out the results in the following format (note the first row contains the column names), so in
this case we get 11 rows:

```json
{
    "ResultSet": {
        "Rows": [
            {
                "Data": [
                    {
                        "VarCharValue": "building"
                    },
                    {
                        "VarCharValue": "index"
                    },
                    {
                        "VarCharValue": "system"
                    },
                    {
                        "VarCharValue": "key"
                    },
                    {
                        "VarCharValue": "level"
                    },
                    {
                        "VarCharValue": "source"
                    },
                    {
                        "VarCharValue": "timestamp"
                    },
                    {
                        "VarCharValue": "class"
                    },
                    {
                        "VarCharValue": "value"
                    },
                    {
                        "VarCharValue": "bookable"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "zone_Fd-jgI"
                    },
                    {
                        "VarCharValue": "64"
                    },
                    {
                        "VarCharValue": "sys_Fd-tO"
                    },
                    {
                        "VarCharValue": "incall"
                    },
                    {
                        "VarCharValue": "zone_Fd-Ip"
                    },
                    {
                        "VarCharValue": "alpha"
                    },
                    {
                        "VarCharValue": "1561725810000"
                    },
                    {
                        "VarCharValue": "VidConf"
                    },
                    {
                        "VarCharValue": "false"
                    },
                    {
                        "VarCharValue": "true"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "zone_Fd-CuL"
                    },
                    {
                        "VarCharValue": "39"
                    },
                    {
                        "VarCharValue": "sys_Fd-uK"
                    },
                    {
                        "VarCharValue": "incall"
                    },
                    {
                        "VarCharValue": "zone_Fd-JM"
                    },
                    {
                        "VarCharValue": "alpha"
                    },
                    {
                        "VarCharValue": "1542545410000"
                    },
                    {
                        "VarCharValue": "VidConf"
                    },
                    {
                        "VarCharValue": "false"
                    },
                    {
                        "VarCharValue": "false"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "zone_Fd-Wfx"
                    },
                    {
                        "VarCharValue": "97"
                    },
                    {
                        "VarCharValue": "sys_Fd-XL"
                    },
                    {
                        "VarCharValue": "incall"
                    },
                    {
                        "VarCharValue": "zone_Fd-kQ"
                    },
                    {
                        "VarCharValue": "alpha"
                    },
                    {
                        "VarCharValue": "1558610267000"
                    },
                    {
                        "VarCharValue": "VidConf"
                    },
                    {
                        "VarCharValue": "true"
                    },
                    {
                        "VarCharValue": "false"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "zone_Fd-lkq"
                    },
                    {
                        "VarCharValue": "21"
                    },
                    {
                        "VarCharValue": "sys_Fd-ZV"
                    },
                    {
                        "VarCharValue": "incall"
                    },
                    {
                        "VarCharValue": "zone_Fd-Qa"
                    },
                    {
                        "VarCharValue": "alpha"
                    },
                    {
                        "VarCharValue": "1564609633000"
                    },
                    {
                        "VarCharValue": "VidConf"
                    },
                    {
                        "VarCharValue": "true"
                    },
                    {
                        "VarCharValue": "true"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "zone_Fd-zeO"
                    },
                    {
                        "VarCharValue": "98"
                    },
                    {
                        "VarCharValue": "sys_Fd-Yv"
                    },
                    {
                        "VarCharValue": "incall"
                    },
                    {
                        "VarCharValue": "zone_Fd-ka"
                    },
                    {
                        "VarCharValue": "alpha"
                    },
                    {
                        "VarCharValue": "1552072606000"
                    },
                    {
                        "VarCharValue": "VidConf"
                    },
                    {
                        "VarCharValue": "true"
                    },
                    {
                        "VarCharValue": "true"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "zone_Fd-szY"
                    },
                    {
                        "VarCharValue": "56"
                    },
                    {
                        "VarCharValue": "sys_Fd-dS"
                    },
                    {
                        "VarCharValue": "incall"
                    },
                    {
                        "VarCharValue": "zone_Fd-Hz"
                    },
                    {
                        "VarCharValue": "alpha"
                    },
                    {
                        "VarCharValue": "1548347271000"
                    },
                    {
                        "VarCharValue": "VidConf"
                    },
                    {
                        "VarCharValue": "false"
                    },
                    {
                        "VarCharValue": "false"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "zone_Fd-nwF"
                    },
                    {
                        "VarCharValue": "55"
                    },
                    {
                        "VarCharValue": "sys_Fd-VI"
                    },
                    {
                        "VarCharValue": "incall"
                    },
                    {
                        "VarCharValue": "zone_Fd-Uc"
                    },
                    {
                        "VarCharValue": "alpha"
                    },
                    {
                        "VarCharValue": "1541055271000"
                    },
                    {
                        "VarCharValue": "VidConf"
                    },
                    {
                        "VarCharValue": "false"
                    },
                    {
                        "VarCharValue": "true"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "zone_Fd-Izh"
                    },
                    {
                        "VarCharValue": "18"
                    },
                    {
                        "VarCharValue": "sys_Fd-oh"
                    },
                    {
                        "VarCharValue": "incall"
                    },
                    {
                        "VarCharValue": "zone_Fd-Dh"
                    },
                    {
                        "VarCharValue": "alpha"
                    },
                    {
                        "VarCharValue": "1545171223000"
                    },
                    {
                        "VarCharValue": "VidConf"
                    },
                    {
                        "VarCharValue": "true"
                    },
                    {
                        "VarCharValue": "true"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "zone_Fd-acm"
                    },
                    {
                        "VarCharValue": "66"
                    },
                    {
                        "VarCharValue": "sys_Fd-AP"
                    },
                    {
                        "VarCharValue": "incall"
                    },
                    {
                        "VarCharValue": "zone_Fd-hx"
                    },
                    {
                        "VarCharValue": "alpha"
                    },
                    {
                        "VarCharValue": "1544555516000"
                    },
                    {
                        "VarCharValue": "VidConf"
                    },
                    {
                        "VarCharValue": "false"
                    },
                    {
                        "VarCharValue": "true"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "zone_Fd-vEU"
                    },
                    {
                        "VarCharValue": "37"
                    },
                    {
                        "VarCharValue": "sys_Fd-km"
                    },
                    {
                        "VarCharValue": "incall"
                    },
                    {
                        "VarCharValue": "zone_Fd-lp"
                    },
                    {
                        "VarCharValue": "alpha"
                    },
                    {
                        "VarCharValue": "1550155723000"
                    },
                    {
                        "VarCharValue": "VidConf"
                    },
                    {
                        "VarCharValue": "true"
                    },
                    {
                        "VarCharValue": "false"
                    }
                ]
            }
        ],
        "ResultSetMetadata": {
            "ColumnInfo": [
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "building",
                    "Label": "building",
                    "Type": "varchar",
                    "Precision": 2147483647,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                    "CaseSensitive": true
                },
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "index",
                    "Label": "index",
                    "Type": "integer",
                    "Precision": 10,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                    "CaseSensitive": false
                },
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "system",
                    "Label": "system",
                    "Type": "varchar",
                    "Precision": 2147483647,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                    "CaseSensitive": true
                },
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "key",
                    "Label": "key",
                    "Type": "varchar",
                    "Precision": 2147483647,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                    "CaseSensitive": true
                },
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "level",
                    "Label": "level",
                    "Type": "varchar",
                    "Precision": 2147483647,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                    "CaseSensitive": true
                },
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "source",
                    "Label": "source",
                    "Type": "varchar",
                    "Precision": 2147483647,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                    "CaseSensitive": true
                },
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "timestamp",
                    "Label": "timestamp",
                    "Type": "bigint",
                    "Precision": 19,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                    "CaseSensitive": false
                },
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "class",
                    "Label": "class",
                    "Type": "varchar",
                    "Precision": 2147483647,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                    "CaseSensitive": true
                },
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "value",
                    "Label": "value",
                    "Type": "boolean",
                    "Precision": 0,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                    "CaseSensitive": false
                },
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "bookable",
                    "Label": "bookable",
                    "Type": "boolean",
                    "Precision": 0,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                    "CaseSensitive": false
                }
            ]
        }
    },
    "UpdateCount": 0
}
```
