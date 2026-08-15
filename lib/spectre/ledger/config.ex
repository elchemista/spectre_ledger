defmodule Spectre.Ledger.Config do
  @moduledoc """
  Validated runtime configuration for a Ledger backend.

  Runtime handles such as a Memory server or an Ecto Repo stay in
  `backend_opts`; they are never part of a Spectre Stack definition or a
  persisted Ledger entry. Checkpoint and receipt byte limits are independent;
  both default to 8,000,000 bytes.
  """

  @default_namespace "default"
  @default_max_checkpoint_bytes 8_000_000
  @default_max_receipt_bytes 8_000_000
  @common_keys [:backend, :namespace, :max_checkpoint_bytes, :max_receipt_bytes]

  @enforce_keys [
    :backend,
    :backend_opts,
    :namespace,
    :max_checkpoint_bytes,
    :max_receipt_bytes
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          backend: module(),
          backend_opts: keyword(),
          namespace: String.t(),
          max_checkpoint_bytes: pos_integer(),
          max_receipt_bytes: pos_integer()
        }

  @doc "Builds a runtime configuration without persisting runtime handles."
  @spec new(keyword() | t()) :: {:ok, t()} | {:error, term()}
  def new(%__MODULE__{} = config), do: validate(config)

  def new(opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         true <- unique_keys?(opts),
         {:ok, backend} <- backend(Keyword.get(opts, :backend, :memory)),
         {:ok, namespace} <- namespace(Keyword.get(opts, :namespace, @default_namespace)),
         {:ok, max_bytes} <-
           max_checkpoint_bytes(
             Keyword.get(opts, :max_checkpoint_bytes, @default_max_checkpoint_bytes)
           ),
         {:ok, max_receipt_bytes} <-
           max_receipt_bytes(Keyword.get(opts, :max_receipt_bytes, @default_max_receipt_bytes)) do
      config = %__MODULE__{
        backend: backend,
        backend_opts: Keyword.drop(opts, @common_keys),
        namespace: namespace,
        max_checkpoint_bytes: max_bytes,
        max_receipt_bytes: max_receipt_bytes
      }

      validate(config)
    else
      false -> {:error, :invalid_ledger_options}
      {:error, _reason} = error -> error
    end
  end

  def new(_opts), do: {:error, :invalid_ledger_options}

  @doc "Returns a backend option without exposing it in portable metadata."
  @spec fetch_backend(t(), atom()) :: {:ok, term()} | :error
  def fetch_backend(%__MODULE__{backend_opts: opts}, key), do: Keyword.fetch(opts, key)

  @doc "Returns a backend option or a default value."
  @spec get_backend(t(), atom(), term()) :: term()
  def get_backend(%__MODULE__{backend_opts: opts}, key, default \\ nil),
    do: Keyword.get(opts, key, default)

  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  defp validate(%__MODULE__{} = config) do
    cond do
      not (is_atom(config.backend) and not is_nil(config.backend)) ->
        {:error, :invalid_ledger_backend}

      not (is_list(config.backend_opts) and Keyword.keyword?(config.backend_opts)) ->
        {:error, :invalid_ledger_backend_options}

      true ->
        with {:ok, _namespace} <- namespace(config.namespace),
             {:ok, _max_bytes} <- max_checkpoint_bytes(config.max_checkpoint_bytes),
             {:ok, _max_receipt_bytes} <- max_receipt_bytes(config.max_receipt_bytes) do
          {:ok, config}
        end
    end
  end

  @spec backend(term()) :: {:ok, module()} | {:error, term()}
  defp backend(:memory), do: {:ok, Spectre.Ledger.Backend.Memory}
  defp backend(:postgres), do: {:ok, Spectre.Ledger.Backend.Postgres}
  defp backend(module) when is_atom(module) and not is_nil(module), do: {:ok, module}
  defp backend(_backend), do: {:error, :invalid_ledger_backend}

  @spec namespace(term()) :: {:ok, String.t()} | {:error, term()}
  defp namespace(value) when is_binary(value) and byte_size(value) in 1..128 do
    if String.valid?(value) and Regex.match?(~r/\A[A-Za-z0-9][A-Za-z0-9._:-]*\z/, value),
      do: {:ok, value},
      else: {:error, :invalid_ledger_namespace}
  end

  defp namespace(_value), do: {:error, :invalid_ledger_namespace}

  @spec max_checkpoint_bytes(term()) :: {:ok, pos_integer()} | {:error, term()}
  defp max_checkpoint_bytes(value) when is_integer(value) and value > 0,
    do: {:ok, value}

  defp max_checkpoint_bytes(_value), do: {:error, :invalid_max_checkpoint_bytes}

  @spec max_receipt_bytes(term()) :: {:ok, pos_integer()} | {:error, term()}
  defp max_receipt_bytes(value) when is_integer(value) and value > 0,
    do: {:ok, value}

  defp max_receipt_bytes(_value), do: {:error, :invalid_max_receipt_bytes}

  defp unique_keys?(opts) do
    keys = Keyword.keys(opts)
    length(keys) == length(Enum.uniq(keys))
  end
end
