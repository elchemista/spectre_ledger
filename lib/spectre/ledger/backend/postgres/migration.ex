defmodule Spectre.Ledger.Backend.Postgres.Migration do
  @moduledoc """
  Builds the PostgreSQL DDL owned by Spectre Ledger.

  Identifiers are accepted only through the same strict validator used by the
  runtime adapter and are quoted in every statement. The generated migration
  does not start or configure an Ecto Repo.
  """

  alias Spectre.Ledger.Backend.Postgres.Names

  @schema_version 1

  @doc "Returns the current PostgreSQL storage schema version."
  @spec schema_version() :: pos_integer()
  def schema_version, do: @schema_version

  @doc "Returns idempotent statements that install the Ledger tables."
  @spec up_sql(keyword()) :: {:ok, [String.t()]} | {:error, term()}
  def up_sql(opts \\ []) do
    with {:ok, names} <- Names.new(opts) do
      meta = Names.qualified(names, :meta)
      streams = Names.qualified(names, :streams)
      blobs = Names.qualified(names, :blobs)
      entries = Names.qualified(names, :entries)
      aliases = Names.qualified(names, :aliases)
      schema = Names.quote_identifier(names.schema)

      {:ok,
       [
         "CREATE SCHEMA IF NOT EXISTS #{schema}",
         """
         CREATE TABLE IF NOT EXISTS #{meta} (
           "key" text PRIMARY KEY,
           "value" text NOT NULL,
           "updated_at" timestamptz NOT NULL DEFAULT transaction_timestamp()
         )
         """,
         """
         INSERT INTO #{meta} ("key", "value")
         VALUES ('schema_version', '#{@schema_version}')
         ON CONFLICT ("key") DO NOTHING
         """,
         """
         DO $spectre_ledger$
         BEGIN
           IF (SELECT "value" FROM #{meta} WHERE "key" = 'schema_version') IS DISTINCT FROM '#{@schema_version}' THEN
             RAISE EXCEPTION 'incompatible Spectre Ledger schema version';
           END IF;
         END
         $spectre_ledger$
         """,
         """
         CREATE TABLE IF NOT EXISTS #{streams} (
           "namespace" varchar(128) NOT NULL,
           "stream_key" text NOT NULL,
           "head_revision" bigint NOT NULL DEFAULT 0 CHECK ("head_revision" >= 0),
           "head_entry_digest" char(64),
           "head_blob_digest" char(64),
           "inserted_at" timestamptz NOT NULL DEFAULT transaction_timestamp(),
           "updated_at" timestamptz NOT NULL DEFAULT transaction_timestamp(),
           PRIMARY KEY ("namespace", "stream_key"),
           CHECK (("head_entry_digest" IS NULL) = ("head_blob_digest" IS NULL))
         )
         """,
         """
         CREATE TABLE IF NOT EXISTS #{blobs} (
           "namespace" varchar(128) NOT NULL,
           "blob_digest" char(64) NOT NULL,
           "checkpoint_digest" char(64) NOT NULL,
           "checkpoint" bytea NOT NULL,
           "byte_size" bigint NOT NULL CHECK ("byte_size" > 0),
           "inserted_at" timestamptz NOT NULL DEFAULT transaction_timestamp(),
           PRIMARY KEY ("namespace", "blob_digest")
         )
         """,
         """
         CREATE TABLE IF NOT EXISTS #{entries} (
           "namespace" varchar(128) NOT NULL,
           "stream_key" text NOT NULL,
           "revision" bigint NOT NULL CHECK ("revision" >= 0),
           "expected_revision" bigint NOT NULL CHECK ("expected_revision" >= 0),
           "kind" text NOT NULL CHECK ("kind" IN ('checkpoint', 'migration')),
           "checkpoint_digest" char(64) NOT NULL,
           "blob_digest" char(64) NOT NULL,
           "previous_entry_digest" char(64),
           "source_entry_digest" char(64),
           "owner_fencing_token" bigint CHECK ("owner_fencing_token" >= 0),
           "entry_digest" char(64) NOT NULL,
           "inserted_at" timestamptz NOT NULL DEFAULT transaction_timestamp(),
           PRIMARY KEY ("namespace", "stream_key", "revision"),
           UNIQUE ("namespace", "entry_digest"),
           CHECK ("revision" > "expected_revision" OR
                  ("kind" = 'migration' AND "revision" = 0 AND "expected_revision" = 0)),
           FOREIGN KEY ("namespace", "stream_key")
             REFERENCES #{streams} ("namespace", "stream_key"),
           FOREIGN KEY ("namespace", "blob_digest")
             REFERENCES #{blobs} ("namespace", "blob_digest")
         )
         """,
         """
         CREATE TABLE IF NOT EXISTS #{aliases} (
           "namespace" varchar(128) NOT NULL,
           "alias_stream_key" text NOT NULL,
           "target_stream_key" text NOT NULL,
           "inserted_at" timestamptz NOT NULL DEFAULT transaction_timestamp(),
           PRIMARY KEY ("namespace", "alias_stream_key"),
           CHECK ("alias_stream_key" <> "target_stream_key"),
           FOREIGN KEY ("namespace", "target_stream_key")
             REFERENCES #{streams} ("namespace", "stream_key")
         )
         """
       ]}
    end
  end

  @doc "Returns statements that remove only the Ledger-owned tables."
  @spec down_sql(keyword()) :: {:ok, [String.t()]} | {:error, term()}
  def down_sql(opts \\ []) do
    with {:ok, names} <- Names.new(opts) do
      {:ok,
       [
         "DROP TABLE IF EXISTS #{Names.qualified(names, :aliases)}",
         "DROP TABLE IF EXISTS #{Names.qualified(names, :entries)}",
         "DROP TABLE IF EXISTS #{Names.qualified(names, :streams)}",
         "DROP TABLE IF EXISTS #{Names.qualified(names, :blobs)}",
         "DROP TABLE IF EXISTS #{Names.qualified(names, :meta)}"
       ]}
    end
  end
end
