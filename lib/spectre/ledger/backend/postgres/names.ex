defmodule Spectre.Ledger.Backend.Postgres.Names do
  @moduledoc false

  alias Spectre.Ledger.Config

  @default_schema "public"
  @default_table_prefix "spectre_ledger"
  @option_keys [:schema, :table_prefix]
  @identifier ~r/\A[a-z_][a-z0-9_]*\z/
  @suffixes %{
    aliases: "aliases",
    blobs: "blobs",
    entries: "entries",
    meta: "meta",
    streams: "streams"
  }

  @enforce_keys [:schema, :table_prefix, :tables]
  defstruct [:schema, :table_prefix, :tables]

  @type table :: :aliases | :blobs | :entries | :meta | :streams
  @type t :: %__MODULE__{
          schema: String.t(),
          table_prefix: String.t(),
          tables: %{required(table()) => String.t()}
        }

  @spec from_config(Config.t()) :: {:ok, t()} | {:error, term()}
  def from_config(%Config{} = config) do
    new(
      schema: Config.get_backend(config, :schema, @default_schema),
      table_prefix: Config.get_backend(config, :table_prefix, @default_table_prefix)
    )
  end

  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts \\ [])

  def new(opts) when is_list(opts) do
    with :ok <- validate_options(opts),
         schema = Keyword.get(opts, :schema, @default_schema),
         table_prefix = Keyword.get(opts, :table_prefix, @default_table_prefix),
         :ok <- validate_identifier(:schema, schema),
         :ok <- validate_identifier(:table_prefix, table_prefix),
         tables <- Map.new(@suffixes, fn {key, suffix} -> {key, "#{table_prefix}_#{suffix}"} end),
         :ok <- validate_tables(tables) do
      {:ok, %__MODULE__{schema: schema, table_prefix: table_prefix, tables: tables}}
    end
  end

  def new(_opts), do: {:error, :invalid_postgres_naming_options}

  @spec table(t(), table()) :: String.t()
  def table(%__MODULE__{tables: tables}, name), do: Map.fetch!(tables, name)

  @spec qualified(t(), table()) :: String.t()
  def qualified(%__MODULE__{} = names, name) do
    quote_identifier(names.schema) <> "." <> quote_identifier(table(names, name))
  end

  @spec quote_identifier(String.t()) :: String.t()
  def quote_identifier(identifier) when is_binary(identifier), do: ~s("#{identifier}")

  defp validate_options(opts) do
    if Keyword.keyword?(opts) do
      keys = Keyword.keys(opts)

      if length(keys) == length(Enum.uniq(keys)) and Enum.all?(keys, &(&1 in @option_keys)),
        do: :ok,
        else: {:error, :invalid_postgres_naming_options}
    else
      {:error, :invalid_postgres_naming_options}
    end
  end

  defp validate_tables(tables) do
    Enum.reduce_while(tables, :ok, fn {_key, identifier}, :ok ->
      case validate_identifier(:table_name, identifier) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_identifier(kind, identifier)
       when is_binary(identifier) and byte_size(identifier) in 1..63 do
    if Regex.match?(@identifier, identifier),
      do: :ok,
      else: {:error, {:invalid_postgres_identifier, kind}}
  end

  defp validate_identifier(kind, _identifier),
    do: {:error, {:invalid_postgres_identifier, kind}}
end
