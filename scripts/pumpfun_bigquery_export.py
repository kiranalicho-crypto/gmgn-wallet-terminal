def query_day(
    client: bigquery.Client,
    start_timestamp: datetime,
    end_timestamp: datetime,
) -> tuple[list[Any], int]:
    sql = """
    SELECT
      block_timestamp,
      block_slot,
      tx_signature,
      index,
      parent_index,
      accounts,
      data
    FROM
      `bigquery-public-data.crypto_solana_mainnet_us.Instructions`
    WHERE
      block_timestamp >= @start_timestamp
      AND block_timestamp < @end_timestamp
      AND program_id = @program_id
    ORDER BY
      block_timestamp,
      tx_signature,
      index
    """

    parameters = [
        bigquery.ScalarQueryParameter(
            "start_timestamp",
            "TIMESTAMP",
            start_timestamp,
        ),
        bigquery.ScalarQueryParameter(
            "end_timestamp",
            "TIMESTAMP",
            end_timestamp,
        ),
        bigquery.ScalarQueryParameter(
            "program_id",
            "STRING",
            PUMP_PROGRAM_ID,
        ),
    ]

    dry_run_config = bigquery.QueryJobConfig(
        query_parameters=parameters,
        dry_run=True,
        use_query_cache=False,
    )

    dry_run_job = client.query(
        sql,
        job_config=dry_run_config,
    )

    estimated_bytes = int(
        dry_run_job.total_bytes_processed or 0
    )

    run_config = bigquery.QueryJobConfig(
        query_parameters=parameters,
        use_query_cache=True,
    )

    rows = list(
        client.query(
            sql,
            job_config=run_config,
        ).result()
    )

    return rows, estimated_bytes
