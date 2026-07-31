defmodule PaseoRelay.ListenerTest do
  use ExUnit.Case, async: false

  import Bitwise

  setup do
    assert PaseoRelay.Metrics.value(:active_websockets) == 0

    on_exit(fn ->
      assert_eventually(fn -> PaseoRelay.Metrics.value(:active_websockets) == 0 end)
    end)

    :ok
  end

  test "the native Cowboy listener serves relay operations" do
    listener = {:listener_test, System.unique_integer([:positive])}

    start_supervised!(
      {PaseoRelay.Listener,
       ref: listener,
       config: PaseoRelay.Config.defaults(),
       ip: {127, 0, 0, 1},
       port: 0,
       acceptors: 1,
       max_connections: 10}
    )

    port = PaseoRelay.Listener.port(listener)
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    :ok = :gen_tcp.send(socket, "GET /health HTTP/1.1\r\nHost: relay.test\r\n\r\n")
    {:ok, response} = :gen_tcp.recv(socket, 0, 2_000)

    assert response =~ "HTTP/1.1 200 OK"
    assert response =~ ~s({"status":"ok"})
  end

  test "the active WebSocket ceiling rejects exactly at capacity and releases on close" do
    reference = {:listener_budget, System.unique_integer([:positive])}
    rejections = PaseoRelay.Metrics.value(:connection_rejections)

    start_supervised!(
      {PaseoRelay.Listener,
       ref: reference,
       config: PaseoRelay.Config.defaults(),
       ip: {127, 0, 0, 1},
       port: 0,
       acceptors: 2,
       max_connections: 100,
       max_websockets: 2}
    )

    port = PaseoRelay.Listener.port(reference)
    first = open_websocket(port, "budget-first")
    second = open_websocket(port, "budget-second")

    assert PaseoRelay.ConnectionBudget.active(reference) == 2

    rejected = connect_and_request_websocket(port, "budget-third")
    assert {:ok, response} = :gen_tcp.recv(rejected, 0, 2_000)
    assert response =~ "HTTP/1.1 503 Service Unavailable"
    assert response =~ "Relay connection capacity"
    assert PaseoRelay.Metrics.value(:connection_rejections) == rejections + 1
    :ok = :gen_tcp.close(rejected)

    :ok = :gen_tcp.close(first)
    assert_eventually(fn -> PaseoRelay.ConnectionBudget.active(reference) == 1 end)

    replacement = open_websocket(port, "budget-third")
    assert PaseoRelay.ConnectionBudget.active(reference) == 2

    :ok = :gen_tcp.close(second)
    :ok = :gen_tcp.close(replacement)
    assert_eventually(fn -> PaseoRelay.ConnectionBudget.active(reference) == 0 end)
  end

  test "active WebSockets fail closed when the connection budget restarts" do
    reference = {:listener_budget_restart, System.unique_integer([:positive])}
    active_websockets = PaseoRelay.Metrics.value(:active_websockets)

    start_supervised!(
      {PaseoRelay.Listener,
       ref: reference,
       config: PaseoRelay.Config.defaults(),
       ip: {127, 0, 0, 1},
       port: 0,
       acceptors: 1,
       max_connections: 100,
       max_websockets: 2}
    )

    port = PaseoRelay.Listener.port(reference)
    socket = open_websocket(port, "budget-restart-existing")
    assert PaseoRelay.ConnectionBudget.active(reference) == 1

    assert_eventually(fn ->
      PaseoRelay.Metrics.value(:active_websockets) == active_websockets + 1
    end)

    budget = Process.whereis(PaseoRelay.ConnectionBudget)
    monitor = Process.monitor(budget)
    Process.exit(budget, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^budget, :killed}, 2_000

    assert {:close, 1013, "Relay capacity unavailable"} = recv_until_close(socket)

    assert_eventually(fn ->
      replacement = Process.whereis(PaseoRelay.ConnectionBudget)
      is_pid(replacement) and replacement != budget
    end)

    replacement = open_websocket(port, "budget-restart-new")
    assert PaseoRelay.ConnectionBudget.active(reference) == 1
    :ok = :gen_tcp.close(replacement)
    assert_eventually(fn -> PaseoRelay.ConnectionBudget.active(reference) == 0 end)
  end

  test "the active WebSocket gauge reconciles when the heap fuse kills a socket" do
    reference = {:listener_heap_fuse, System.unique_integer([:positive])}
    config = %{PaseoRelay.Config.defaults() | websocket_max_heap_words: 65_536}

    start_supervised!(
      {PaseoRelay.Listener,
       ref: reference,
       config: config,
       ip: {127, 0, 0, 1},
       port: 0,
       acceptors: 1,
       max_connections: 100,
       max_websockets: 2}
    )

    socket = open_websocket(PaseoRelay.Listener.port(reference), "heap-fuse")
    assert_eventually(fn -> PaseoRelay.Metrics.value(:active_websockets) == 1 end)

    payload = :binary.copy(<<0x5A>>, 1024 * 1024)
    :ok = :gen_tcp.send(socket, :cow_ws.masked_frame({:binary, payload}, 0x11223344))

    assert_eventually(fn -> PaseoRelay.ConnectionBudget.active(reference) == 0 end)
    assert PaseoRelay.Metrics.value(:active_websockets) == 0
  end

  defp open_websocket(port, server_id) do
    socket = connect_and_request_websocket(port, server_id)
    assert {:ok, response} = :gen_tcp.recv(socket, 0, 2_000)
    assert response =~ "HTTP/1.1 101 Switching Protocols"
    socket
  end

  defp connect_and_request_websocket(port, server_id) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    key = Base.encode64(:crypto.strong_rand_bytes(16))

    request =
      "GET /ws?serverId=#{server_id}&role=server&v=2 HTTP/1.1\r\n" <>
        "Host: relay.test\r\n" <>
        "Upgrade: websocket\r\n" <>
        "Connection: Upgrade\r\n" <>
        "Sec-WebSocket-Version: 13\r\n" <>
        "Sec-WebSocket-Key: #{key}\r\n\r\n"

    :ok = :gen_tcp.send(socket, request)
    socket
  end

  defp recv_until_close(socket) do
    case recv_server_frame(socket) do
      {:close, _code, _reason} = close -> close
      _frame -> recv_until_close(socket)
    end
  end

  defp recv_server_frame(socket) do
    {:ok, <<first, second>>} = :gen_tcp.recv(socket, 2, 2_000)
    opcode = first &&& 0x0F
    length = second &&& 0x7F

    length =
      case length do
        126 ->
          {:ok, <<value::16>>} = :gen_tcp.recv(socket, 2, 2_000)
          value

        127 ->
          {:ok, <<value::64>>} = :gen_tcp.recv(socket, 8, 2_000)
          value

        value ->
          value
      end

    {:ok, payload} = :gen_tcp.recv(socket, length, 2_000)

    case {opcode, payload} do
      {0x8, <<code::16, reason::binary>>} -> {:close, code, reason}
      {0x1, payload} -> {:text, payload}
      {0x2, payload} -> {:binary, payload}
    end
  end

  defp assert_eventually(assertion, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    until_true(assertion, deadline)
  end

  defp until_true(assertion, deadline) do
    if assertion.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("condition did not become true before timeout")
      else
        receive do
        after
          10 -> until_true(assertion, deadline)
        end
      end
    end
  end
end
