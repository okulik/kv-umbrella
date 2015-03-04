defmodule KVServer do
  use Application

  # See http://elixir-lang.org/docs/stable/elixir/Application.html
  # for more information on OTP Applications
  def start(_type, _args) do
    import Supervisor.Spec

    children = [
      supervisor(Task.Supervisor, [[name: KVServer.TaskSupervisor]]),
      worker(Task, [KVServer, :accept, [Application.get_env(:kv_server, :port)]])
    ]

    # See http://elixir-lang.org/docs/stable/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: KVServer.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @doc """
  Starts accepting connections on the given `port`.
  """
  def accept(port) do
    {:ok, server_socket} = :gen_tcp.listen(port, [:binary, packet: :line, active: false])
    IO.puts "accepting connections on port #{port}"
    loop_acceptor(server_socket)
  end

  def loop_acceptor(server_socket) do
    {:ok, client_socket} = :gen_tcp.accept(server_socket)
    Task.Supervisor.start_child(KVServer.TaskSupervisor, fn ->
      serve(client_socket)
    end)
    loop_acceptor(server_socket)
  end
  
  def serve(client_socket) do
    import Pipe

    message = pipe_matching x, {:ok, x},
      read_line(client_socket)
      |> KVServer.Command.parse()
      |> KVServer.Command.run()

    write_line(client_socket, message)
    serve(client_socket)
  end

  def read_line(client_socket) do
    :gen_tcp.recv(client_socket, 0)
  end

  def write_line(client_socket, line) do
    :gen_tcp.send(client_socket, format_message(line))
  end

  defp format_message({:ok, text}), do: text
  defp format_message({:error, :unknown_command}), do: "UNKNOWN COMMAND\r\n"
  defp format_message({:error, :not_found}), do: "NOT FOUND\r\n"
  defp format_message({:error, _}), do: "ERROR\r\n"
end
