defmodule KV.Registry do
  use GenServer

  @doc """
  Starts registry qith a provided event manger.
  """
  def start_link(table, event_manager, buckets, opts \\ []) do
    GenServer.start_link(__MODULE__, {table, event_manager, buckets}, opts)
  end

  @doc """
  Looks up for a bucket with a given name.
  """
  def lookup(table, name) do
    case :ets.lookup(table, name) do
      [{^name, bucket}] -> {:ok, bucket}
      [] -> :error
    end
  end

  @doc """
  Creates new bucket with a given name.
  """
  def create(server, name) do
    GenServer.call(server, {:create, name})
  end

  @doc """
  Stops the registry.
  """
  def stop(server) do
    GenServer.call(server, :stop)
  end


  @doc """
  GenServer callbacks.
  """

  def init({table, events, buckets}) do
    refs = :ets.foldl(fn({name, bucket}, acc) ->
      HashDict.put(acc, Process.monitor(bucket), name)
      end, HashDict.new, table)
    {:ok, %{names: table, refs: refs, events: events, buckets: buckets}}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call({:create, name}, _from, state) do
    case lookup(state.names, name) do
      {:ok, bucket} ->
        {:reply, bucket, state}
      :error ->
        {:ok, bucket} = KV.Bucket.Supervisor.start_bucket(state.buckets)
        
        # monitor the bucket process
        ref = Process.monitor(bucket)

        refs = HashDict.put(state.refs, ref, name)
        :ets.insert(state.names, {name, bucket})
        
        # push a notification to the event manager on bucket create
        GenEvent.sync_notify(state.events, {:create, name, bucket})
        
        {:reply, bucket, %{state | refs: refs}}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    {name, refs} = HashDict.pop(state.refs, ref)
    :ets.delete(state.names, name)

    # push a notification to the event manager on exit
    GenEvent.sync_notify(state.events, {:exit, name, pid})

    {:noreply, %{state | refs: refs}}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end
end