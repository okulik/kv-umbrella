defmodule KVServer.Command do
  @doc ~S"""
  Parses the given `line` into a command.

  ## Examples

      iex> KVServer.Command.parse "CREATE shopping\r\n"
      {:ok, {:create, "shopping"}}

      iex> KVServer.Command.parse "CREATE  shopping  \r\n"
      {:ok, {:create, "shopping"}}

      iex> KVServer.Command.parse "PUT shopping milk 1\r\n"
      {:ok, {:put, "shopping", "milk", "1"}}

      iex> KVServer.Command.parse "GET shopping milk\r\n"
      {:ok, {:get, "shopping", "milk"}}

      iex> KVServer.Command.parse "DELETE shopping eggs\r\n"
      {:ok, {:delete, "shopping", "eggs"}}

  Unknown commands or commands with the wrong number of
  arguments return an error:

      iex> KVServer.Command.parse "UNKNOWN shopping eggs\r\n"
      {:error, :unknown_command}

      iex> KVServer.Command.parse "GET shopping\r\n"
      {:error, :unknown_command}
  """
  def parse(line) do
    case String.split(line) do
      ["CREATE", bucket]          -> {:ok,   {:create, bucket}}
      ["GET", bucket, key]        -> {:ok,   {:get, bucket, key}}
      ["PUT", bucket, key, value] -> {:ok,   {:put, bucket, key, value}}
      ["DELETE", bucket, key]     -> {:ok,   {:delete, bucket, key}}
      _                           -> {:error, :unknown_command}
    end
  end

  @doc """
  Runs the given command.
  """
  def run(command)

  def run({:create, bucket_name}) do
    KV.Registry.create(KV.Registry, bucket_name)
    {:ok, "OK\r\n"}
  end

  def run({:get, bucket_name, key}) do
    lookup(bucket_name, fn(bucket) ->
      value = KV.Bucket.get(bucket, key)
      {:ok, "#{value}\r\nOK\r\n"}
    end)
  end

  def run({:put, bucket_name, key, value}) do
    lookup(bucket_name, fn(bucket) ->
      KV.Bucket.put(bucket, key, value)
      {:ok, "OK\r\n"}
    end)
  end

  def run({:delete, bucket_name, key}) do
    lookup(bucket_name, fn(bucket) ->
      KV.Bucket.delete(bucket, key)
      {:ok, "OK\r\n"}
    end)
  end

  defp lookup(bucket_name, callback) do
    case KV.Registry.lookup(KV.Registry, bucket_name) do
      {:ok, bucket} -> callback.(bucket)
      :error -> {:error, :not_found}
    end
  end
end