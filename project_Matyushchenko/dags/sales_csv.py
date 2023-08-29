sales_csv = {
    "autodetect": False,
    "schema": {
        "fields": [
            {
                "name": "CustomerId",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "PurchaseDate",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "Product",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "Price",
                "type": "STRING",
                "mode": "REQUIRED",
            },
        ]
    },
    "csvOptions": {
        "allowJaggedRows": False,
        "allowQuotedNewlines": False,
        "maxBadRecords": 0,
        "encoding": "UTF-8",
        "quote": "\"",
        "fieldDelimiter": ",",
        "skipLeadingRows": 1
    },
    "sourceFormat": "CSV",
    "sourceUris": [
        (
            "gs://{{ params.data_lake_raw_bucket }}"
            "/raw"
            "/sales"
            "/{{ dag_run.logical_date.strftime('%Y-%m-%-d') }}"
            "/*.csv"
        )
    ]
}
