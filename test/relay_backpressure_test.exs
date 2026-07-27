defmodule PaseoRelay.BackpressureTest do
  use ExUnit.Case, async: false
  import Bitwise

  @frame_bytes 4 * 1024 * 1024
  @pressure_frame_bytes 8 * 1024 * 1024
  @maximum_frame_payload_bytes PaseoRelay.Protocol.maximum_client_frame_payload_bytes()
  @maximum_message_payload_bytes PaseoRelay.Protocol.maximum_message_payload_bytes()

  setup do
    baseline = %{
      active_websockets: 0,
      backpressured_sources: 0,
      inflight_delivery_bytes: 0,
      ingress_reserved_bytes: 0
    }

    await_transient_gauges(baseline)
    on_exit(fn -> await_transient_gauges(baseline) end)

    :ok
  end

  defmodule RelayClient do
    use WebSockex

    def start_link(url, owner), do: WebSockex.start_link(url, __MODULE__, owner)

    def handle_connect(_connection, owner) do
      send(owner, {:relay_open, self()})
      {:ok, owner}
    end

    def handle_frame({kind, payload}, owner) do
      send(owner, {:relay_frame, self(), kind, payload})
      {:ok, owner}
    end

    def handle_disconnect(%{reason: reason}, owner) do
      send(owner, {:relay_closed, self(), reason})
      {:ok, owner}
    end
  end

  defmodule DigestClient do
    use WebSockex

    def start_link(url, owner), do: WebSockex.start_link(url, __MODULE__, owner)

    def handle_connect(_connection, owner) do
      send(owner, {:digest_open, self()})
      {:ok, owner}
    end

    def handle_frame({kind, payload}, owner) do
      send(
        owner,
        {:digest_frame, self(), kind, byte_size(payload), :crypto.hash(:sha256, payload)}
      )

      {:ok, owner}
    end
  end

  test "a client frame waits without buffering until daemon data attaches" do
    port = start_relay()
    source = raw_connect(port, "/ws?serverId=attach-#{port}&role=client&v=2&connectionId=c1")

    :ok = send_frame(source, :text, "first")
    await_metric(:backpressured_sources, &(&1 == 1))
    :ok = send_frame(source, :binary, "second")

    {:ok, destination} =
      connect(
        "ws://127.0.0.1:#{port}/ws?serverId=attach-#{port}&role=server&v=2&connectionId=c1",
        self()
      )

    assert_receive {:relay_open, ^destination}
    assert_receive {:relay_frame, ^destination, :text, "first"}
    assert_receive {:relay_frame, ^destination, :binary, "second"}
    await_metric(:backpressured_sources, &(&1 == 0))
  end

  @tag timeout: 20_000
  test "a passive destination bounds relay payloads and stalls the source TCP sender" do
    previous = Application.fetch_env!(:paseo_relay, :operations)

    Application.put_env(
      :paseo_relay,
      :operations,
      Keyword.put(previous, :delivery_timeout_ms, 500)
    )

    on_exit(fn -> Application.put_env(:paseo_relay, :operations, previous) end)

    port = start_relay(send_timeout: 1_000)
    active_baseline = PaseoRelay.Metrics.value(:active_websockets)
    slow_baseline = PaseoRelay.Metrics.value(:slow_consumer_disconnects)
    _destination = raw_connect(port, "/ws?serverId=pressure-#{port}&role=server")
    source = raw_connect(port, "/ws?serverId=pressure-#{port}&role=client")
    payload = :binary.copy(<<42>>, @frame_bytes)

    sender =
      Task.async(fn ->
        Enum.each(1..32, fn _ -> :ok = send_frame(source, :binary, payload) end)
      end)

    await_metric(:backpressured_sources, &(&1 >= 1))
    assert PaseoRelay.Metrics.value(:inflight_delivery_bytes) <= @frame_bytes
    assert nil == Task.yield(sender, 250)

    await_metric(:slow_consumer_disconnects, &(&1 == slow_baseline + 1))
    await_metric(:backpressured_sources, &(&1 == 0))
    await_metric(:inflight_delivery_bytes, &(&1 == 0))
    await_metric(:active_websockets, &(&1 == active_baseline))
    await_reserved(&(&1 == 0))

    _ = Task.yield(sender, 5_000) || Task.shutdown(sender, :brutal_kill)
  end

  @tag timeout: 20_000
  test "frame headers reserve a strict node budget before payload bytes are read" do
    previous = Application.fetch_env!(:paseo_relay, :operations)

    Application.put_env(
      :paseo_relay,
      :operations,
      Keyword.put(previous, :payload_timeout_ms, 200)
    )

    on_exit(fn -> Application.put_env(:paseo_relay, :operations, previous) end)

    port = start_relay()
    baseline = PaseoRelay.Metrics.value(:active_websockets)

    sockets =
      Enum.map(1..5, fn index ->
        raw_connect(port, "/ws?serverId=budget-#{port}-#{index}&role=server")
      end)

    await_metric(:active_websockets, &(&1 == baseline + 5))

    Enum.each(sockets, &send_frame_header(&1, :binary, @maximum_frame_payload_bytes))
    limit = 512 * 1024 * 1024
    await_reserved(&(&1 > 500 * 1024 * 1024))

    assert PaseoRelay.Delivery.Budget.reserved_bytes() <= limit
    assert PaseoRelay.Delivery.Budget.reserved_bytes() <= limit

    Enum.each(Enum.take(sockets, 4), fn socket ->
      assert {:close, 1013, "Payload completion timeout"} = recv_server_frame(socket)
    end)

    assert {:close, 1013, "Payload completion timeout"} = recv_server_frame(List.last(sockets))
    await_metric(:active_websockets, &(&1 == baseline))
    await_reserved(&(&1 == 0))
  end

  @tag timeout: 30_000
  test "a maximum legal unfragmented payload survives the heap fuse" do
    port = start_relay()
    server_id = "maximum-frame-#{port}"
    destination = digest_connect(v2_url(port, server_id, "server", "shared"))
    source = raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")
    payload = :binary.copy(<<0xA5>>, @maximum_frame_payload_bytes)
    digest = :crypto.hash(:sha256, payload)

    assert :ok = send_frame(source, :binary, payload)

    assert_receive {:digest_frame, ^destination, :binary, @maximum_frame_payload_bytes, ^digest},
                   15_000

    await_reserved(&(&1 == 0))
  end

  @tag timeout: 30_000
  test "a fragmented maximum-size message permits an interleaved control frame" do
    port = start_relay()
    server_id = "maximum-fragmented-#{port}"
    destination = digest_connect(v2_url(port, server_id, "server", "shared"))
    source = raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")
    half = :binary.copy(<<0x3C>>, div(@maximum_message_payload_bytes, 2))
    digest = :crypto.hash(:sha256, half <> half)

    assert :ok = send_raw_frame(source, 0x2, half, false)
    assert :ok = send_raw_frame(source, 0x9, "still-alive", true)
    assert {:pong, "still-alive"} = recv_server_frame(source)
    assert :ok = send_raw_frame(source, 0x0, half, true)

    assert_receive {:digest_frame, ^destination, :binary, @maximum_message_payload_bytes,
                    ^digest},
                   15_000

    await_reserved(&(&1 == 0))
  end

  @tag timeout: 20_000
  test "a complete non-final fragment expires its assembly and releases ingress" do
    previous = Application.fetch_env!(:paseo_relay, :operations)

    Application.put_env(
      :paseo_relay,
      :operations,
      Keyword.put(previous, :payload_timeout_ms, 2_000)
    )

    on_exit(fn -> Application.put_env(:paseo_relay, :operations, previous) end)

    port = start_relay()
    server_id = "stalled-fragment-#{port}"
    {:ok, destination} = connect(v2_url(port, server_id, "server", "shared"))
    assert_receive {:relay_open, ^destination}

    stalled =
      raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")

    baseline = PaseoRelay.Delivery.Budget.reserved_bytes()
    fragment = :binary.copy(<<0x7A>>, 8 * 1024 * 1024)
    assert :ok = send_raw_frame(stalled, 0x2, fragment, false)
    assert :ok = send_raw_frame(stalled, 0x9, "fragment-parsed", true)
    assert {:pong, "fragment-parsed"} = recv_server_frame(stalled)
    await_reserved(&(&1 == baseline + byte_size(fragment) * 4))

    assert {:close, 1013, "Payload completion timeout"} = recv_server_frame(stalled)
    await_reserved(&(&1 == baseline))

    legitimate_server_id = "after-stalled-fragment-#{port}"

    {:ok, legitimate_destination} =
      connect(v2_url(port, legitimate_server_id, "server", "shared"))

    assert_receive {:relay_open, ^legitimate_destination}

    legitimate =
      raw_connect(
        port,
        "/ws?serverId=#{legitimate_server_id}&role=client&v=2&connectionId=shared"
      )

    assert :ok = send_frame(legitimate, :text, "after-stalled-fragment")

    assert_receive {:relay_frame, ^legitimate_destination, :text, "after-stalled-fragment"}, 5_000
    await_reserved(&(&1 == baseline))
  end

  test "an oversized message is rejected before reserving ingress" do
    port = start_relay()
    source = raw_connect(port, "/ws?serverId=oversize-#{port}&role=server")

    assert :ok = send_frame_header(source, :binary, @maximum_frame_payload_bytes + 1)
    assert {:close, 1009, _reason} = recv_server_frame(source)
    await_reserved(&(&1 == 0))
  end

  @tag timeout: 30_000
  test "advertised payloads expire and release admission to queued legitimate traffic" do
    previous = Application.fetch_env!(:paseo_relay, :operations)

    Application.put_env(
      :paseo_relay,
      :operations,
      Keyword.put(previous, :payload_timeout_ms, 200)
    )

    on_exit(fn -> Application.put_env(:paseo_relay, :operations, previous) end)

    port = start_relay()
    server_id = "queued-legitimate-#{port}"
    destination = digest_connect(v2_url(port, server_id, "server", "shared"))
    source = raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")
    payload = :binary.copy(<<0x6D>>, @maximum_frame_payload_bytes)
    digest = :crypto.hash(:sha256, payload)

    stalled =
      Enum.map(1..4, fn index ->
        raw_connect(port, "/ws?serverId=stalled-#{port}-#{index}&role=server")
      end)

    Enum.each(stalled, fn socket ->
      :ok = send_frame_header(socket, :binary, @maximum_frame_payload_bytes)
    end)

    await_reserved(&(&1 == @maximum_frame_payload_bytes * 4 * 4))

    sender = Task.async(fn -> send_frame(source, :binary, payload) end)

    refute_receive {:digest_frame, ^destination, :binary, @maximum_frame_payload_bytes, ^digest},
                   50

    Enum.each(stalled, fn socket ->
      assert {:close, 1013, "Payload completion timeout"} = recv_server_frame(socket)
    end)

    assert :ok = Task.await(sender, 15_000)

    assert_receive {:digest_frame, ^destination, :binary, @maximum_frame_payload_bytes, ^digest},
                   15_000

    await_reserved(&(&1 == 0))
  end

  test "concurrent producers retain per-source FIFO order through one writer" do
    port = start_relay()
    server_id = "producers-#{port}"

    {:ok, destination} = connect(v2_url(port, server_id, "server", "shared"))
    assert_receive {:relay_open, ^destination}

    sources =
      Enum.map(1..5, fn _ ->
        {:ok, source} = connect(v2_url(port, server_id, "client", "shared"))
        assert_receive {:relay_open, ^source}
        source
      end)

    sources
    |> Enum.with_index(1)
    |> Task.async_stream(fn {source, source_id} ->
      Enum.each(1..20, fn sequence ->
        :ok = WebSockex.send_frame(source, {:text, "#{source_id}:#{sequence}"})
      end)
    end)
    |> Stream.run()

    received =
      Enum.map(1..100, fn _ ->
        assert_receive {:relay_frame, ^destination, :text, payload}, 5_000
        payload
      end)

    assert received
           |> Enum.map(&String.split(&1, ":"))
           |> Enum.group_by(&hd/1, &(List.last(&1) |> String.to_integer()))
           |> Map.values()
           |> Enum.all?(&(&1 == Enum.to_list(1..20)))

    Enum.each(sources, &GenServer.stop/1)
  end

  test "control notifications retain the forwarded metric contract" do
    port = start_relay()
    server_id = "control-metrics-#{port}"
    frames_baseline = PaseoRelay.Metrics.value(:frames_forwarded)
    bytes_baseline = PaseoRelay.Metrics.value(:bytes_forwarded)

    {:ok, control} = connect(v2_url(port, server_id, "server", ""))
    assert_receive {:relay_open, ^control}
    assert_receive {:relay_frame, ^control, :text, sync}

    {:ok, client} = connect(v2_url(port, server_id, "client", "shared"))
    assert_receive {:relay_open, ^client}
    assert_receive {:relay_frame, ^control, :text, connected}

    assert Jason.decode!(sync) == %{"connectionIds" => [], "type" => "sync"}
    assert Jason.decode!(connected) == %{"connectionId" => "shared", "type" => "connected"}
    await_metric(:frames_forwarded, &(&1 == frames_baseline + 2))

    expected_bytes = bytes_baseline + byte_size(sync) + byte_size(connected)
    await_metric(:bytes_forwarded, &(&1 == expected_bytes))
  end

  test "the node watermark explicitly closes the oldest blocked source" do
    previous = Application.fetch_env!(:paseo_relay, :operations)

    Application.put_env(
      :paseo_relay,
      :operations,
      Keyword.put(previous, :memory_watermark_bytes, 1)
    )

    on_exit(fn -> Application.put_env(:paseo_relay, :operations, previous) end)

    port = start_relay()
    baseline = PaseoRelay.Metrics.value(:active_websockets)
    {:ok, source} = connect(v2_url(port, "watermark-#{port}", "client", "missing"))
    assert_receive {:relay_open, ^source}
    await_metric(:active_websockets, &(&1 == baseline + 1))
    :ok = WebSockex.send_frame(source, {:binary, "blocked"})
    await_metric(:backpressured_sources, &(&1 == 1))
    Process.sleep(20)
    :ok = PaseoRelay.Delivery.Pressure.check_now()

    assert_receive {:relay_closed, ^source, {:remote, 1013, "Relay memory pressure"}}, 2_000
    await_metric(:active_websockets, &(&1 == baseline))
    await_metric(:backpressured_sources, &(&1 == 0))
    await_reserved(&(&1 == 0))
  end

  test "a missing daemon data route expires with an explicit retryable close" do
    previous = Application.fetch_env!(:paseo_relay, :operations)

    Application.put_env(
      :paseo_relay,
      :operations,
      Keyword.put(previous, :data_attach_timeout_ms, 100)
    )

    on_exit(fn -> Application.put_env(:paseo_relay, :operations, previous) end)

    port = start_relay()
    baseline = PaseoRelay.Metrics.value(:active_websockets)
    {:ok, source} = connect(v2_url(port, "attach-timeout-#{port}", "client", "missing"))
    assert_receive {:relay_open, ^source}
    :ok = WebSockex.send_frame(source, {:text, "cannot-buffer"})

    assert_receive {:relay_closed, ^source, {:remote, 1013, "Data route unavailable"}}, 2_000
    await_metric(:active_websockets, &(&1 == baseline))
    await_reserved(&(&1 == 0))
  end

  @tag timeout: 20_000
  test "destination death releases every blocked producer and reservation" do
    port = start_relay(send_timeout: 5_000)
    baseline = PaseoRelay.Metrics.value(:active_websockets)
    server_id = "destination-death-#{port}"

    destination =
      raw_connect(port, "/ws?serverId=#{server_id}&role=server&v=2&connectionId=shared")

    sources =
      Enum.map(1..5, fn _ ->
        {:ok, source} = connect(v2_url(port, server_id, "client", "shared"))
        assert_receive {:relay_open, ^source}
        source
      end)

    payload = :binary.copy(<<13>>, 16 * 1024 * 1024)
    Enum.each(sources, &WebSockex.send_frame(&1, {:binary, payload}))
    await_metric(:backpressured_sources, &(&1 >= 1))
    :gen_tcp.close(destination)

    Enum.each(sources, fn source ->
      assert_receive {:relay_closed, ^source, {:remote, code, reason}}, 5_000
      assert {code, reason} in [{1012, "Server disconnected"}, {1013, "Delivery unavailable"}]
    end)

    await_metric(:backpressured_sources, &(&1 == 0))
    await_metric(:inflight_delivery_bytes, &(&1 == 0))
    await_metric(:active_websockets, &(&1 == baseline))
    await_reserved(&(&1 == 0))
  end

  test "fanout metrics count every actual destination delivery" do
    port = start_relay()
    server_id = "fanout-metrics-#{port}"
    {:ok, first} = connect(v2_url(port, server_id, "client", "shared"))
    assert_receive {:relay_open, ^first}
    {:ok, second} = connect(v2_url(port, server_id, "client", "shared"))
    assert_receive {:relay_open, ^second}
    {:ok, daemon} = connect(v2_url(port, server_id, "server", "shared"))
    assert_receive {:relay_open, ^daemon}
    frames_baseline = PaseoRelay.Metrics.value(:frames_forwarded)
    bytes_baseline = PaseoRelay.Metrics.value(:bytes_forwarded)
    payload = "count-each-destination"

    :ok = WebSockex.send_frame(daemon, {:text, payload})
    assert_receive {:relay_frame, ^first, :text, ^payload}
    assert_receive {:relay_frame, ^second, :text, ^payload}
    await_metric(:frames_forwarded, &(&1 == frames_baseline + 2))
    await_metric(:bytes_forwarded, &(&1 == bytes_baseline + 2 * byte_size(payload)))
  end

  @tag timeout: 45_000
  test "a real unread fanout peer reaches its send deadline without delaying healthy order" do
    previous = Application.fetch_env!(:paseo_relay, :operations)

    Application.put_env(
      :paseo_relay,
      :operations,
      Keyword.put(previous, :delivery_timeout_ms, 500)
    )

    on_exit(fn -> Application.put_env(:paseo_relay, :operations, previous) end)

    # The Writer deadline fires first; the blocked real TCP send then reaches
    # its own transport deadline before the connection can finish cleanup.
    port = start_relay(send_timeout: 1_000)
    server_id = "fanout-#{port}"
    active_baseline = PaseoRelay.Metrics.value(:active_websockets)
    _slow = raw_connect(port, "/ws?serverId=#{server_id}&role=client&v=2&connectionId=shared")
    await_metric(:active_websockets, &(&1 == active_baseline + 1))
    {:ok, healthy} = connect(v2_url(port, server_id, "client", "shared"))
    assert_receive {:relay_open, ^healthy}
    daemon = raw_connect(port, "/ws?serverId=#{server_id}&role=server&v=2&connectionId=shared")
    await_metric(:active_websockets, &(&1 == active_baseline + 3))
    slow_baseline = PaseoRelay.Metrics.value(:slow_consumer_disconnects)
    send_ordered_pressure_frames(daemon, healthy, 1, 8)

    await_metric(:slow_consumer_disconnects, &(&1 == slow_baseline + 1))
    await_metric(:active_websockets, &(&1 == active_baseline + 2))

    :ok = send_frame(daemon, :text, "after-slow-client")
    assert_receive {:relay_frame, ^healthy, :text, "after-slow-client"}, 5_000
  end

  defp start_relay(options \\ []) do
    port = available_port()

    relay =
      start_supervised!(
        {Bandit,
         [
           plug: PaseoRelay.Router,
           port: port,
           thousand_island_options: [
             transport_options: [
               send_timeout: Keyword.get(options, :send_timeout, 30_000),
               send_timeout_close: true,
               recbuf: 64 * 1024,
               sndbuf: Keyword.get(options, :send_buffer, 1024)
             ]
           ],
           websocket_options: PaseoRelay.Protocol.websocket_options()
         ]}
      )

    track({:listener, relay})
    port
  end

  defp raw_connect(port, path) do
    {:ok, socket} =
      :gen_tcp.connect(~c"127.0.0.1", port, [
        :binary,
        active: false,
        nodelay: true,
        recbuf: 1024,
        send_timeout: 10_000
      ])

    key = Base.encode64(:crypto.strong_rand_bytes(16))

    request =
      "GET #{path} HTTP/1.1\r\n" <>
        "Host: relay.test\r\n" <>
        "Upgrade: websocket\r\n" <>
        "Connection: Upgrade\r\n" <>
        "Sec-WebSocket-Version: 13\r\n" <>
        "Sec-WebSocket-Key: #{key}\r\n\r\n"

    :ok = :gen_tcp.send(socket, request)
    {:ok, response} = recv_headers(socket, "")
    assert response =~ "HTTP/1.1 101 Switching Protocols"
    track({:socket, socket})
    socket
  end

  defp connect(url, owner \\ nil) do
    {:ok, client} = RelayClient.start_link(url, owner || self())
    Process.unlink(client)
    track({:client, client})
    {:ok, client}
  end

  defp digest_connect(url) do
    {:ok, client} = DigestClient.start_link(url, self())
    Process.unlink(client)
    track({:client, client})
    assert_receive {:digest_open, ^client}
    client
  end

  defp v2_url(port, server_id, role, connection_id) do
    "ws://127.0.0.1:#{port}/ws?serverId=#{server_id}&role=#{role}&v=2&connectionId=#{connection_id}"
  end

  defp recv_headers(socket, acc) do
    if String.contains?(acc, "\r\n\r\n") do
      {:ok, acc}
    else
      with {:ok, data} <- :gen_tcp.recv(socket, 0, 2_000) do
        recv_headers(socket, acc <> data)
      end
    end
  end

  defp send_frame(socket, opcode, payload) do
    opcode = if opcode == :text, do: 0x1, else: 0x2
    send_raw_frame(socket, opcode, payload, true)
  end

  defp send_ordered_pressure_frames(_source, _destination, sequence, last)
       when sequence > last,
       do: :ok

  defp send_ordered_pressure_frames(source, destination, sequence, last) do
    payload =
      <<sequence::32, :binary.copy(<<rem(sequence, 256)>>, @pressure_frame_bytes - 4)::binary>>

    assert :ok = send_frame(source, :binary, payload)
    assert_receive {:relay_frame, ^destination, :binary, ^payload}, 15_000
    await_metric(:backpressured_sources, &(&1 == 0))
    send_ordered_pressure_frames(source, destination, sequence + 1, last)
  end

  defp send_raw_frame(socket, opcode, payload, fin) do
    mask = 0x11223344
    masked = Bandit.PrimitiveOps.WebSocket.ws_mask(payload, mask)
    length = byte_size(payload)
    first = if(fin, do: 0x80, else: 0x00) ||| opcode

    header =
      cond do
        length <= 125 -> <<first, 0x80 ||| length>>
        length <= 65_535 -> <<first, 0x80 ||| 126, length::16>>
        true -> <<first, 0x80 ||| 127, length::64>>
      end

    :gen_tcp.send(socket, [header, <<mask::32>>, masked])
  end

  defp send_frame_header(socket, opcode, length) do
    opcode = if opcode == :text, do: 0x1, else: 0x2
    :gen_tcp.send(socket, <<0x80 ||| opcode, 0x80 ||| 127, length::64, 0x11223344::32>>)
  end

  defp recv_server_frame(socket) do
    {:ok, <<first, second>>} = :gen_tcp.recv(socket, 2, 5_000)
    opcode = first &&& 0x0F
    length = second &&& 0x7F

    length =
      case length do
        126 ->
          {:ok, <<value::16>>} = :gen_tcp.recv(socket, 2, 5_000)
          value

        127 ->
          {:ok, <<value::64>>} = :gen_tcp.recv(socket, 8, 5_000)
          value

        value ->
          value
      end

    {:ok, payload} = :gen_tcp.recv(socket, length, 5_000)

    case {opcode, payload} do
      {0x8, <<code::16, reason::binary>>} -> {:close, code, reason}
      {0xA, payload} -> {:pong, payload}
      {0x1, payload} -> {:text, payload}
      {0x2, payload} -> {:binary, payload}
    end
  end

  defp close_raw(socket) do
    :gen_tcp.close(socket)
    :ok
  end

  defp track(resource) do
    on_exit(fn -> stop_resource(resource) end)
    resource
  end

  defp stop_resource({:socket, socket}), do: close_raw(socket)

  defp stop_resource({_kind, pid}) do
    if Process.alive?(pid) do
      reference = Process.monitor(pid)

      try do
        GenServer.stop(pid, :normal, 5_000)
      catch
        :exit, _reason -> :ok
      end

      receive do
        {:DOWN, ^reference, :process, ^pid, _reason} -> :ok
      after
        5_000 -> flunk("resource #{inspect(pid)} did not stop synchronously")
      end
    end
  end

  defp transient_gauges do
    Map.new(
      [
        :active_websockets,
        :backpressured_sources,
        :inflight_delivery_bytes,
        :ingress_reserved_bytes
      ],
      fn name ->
        {name, PaseoRelay.Metrics.value(name)}
      end
    )
  end

  defp await_transient_gauges(expected) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_transient_gauges(expected, deadline)
  end

  defp await_transient_gauges(expected, deadline) do
    actual = transient_gauges()

    cond do
      actual == expected and
          PaseoRelay.Delivery.Budget.reserved_bytes() == expected.ingress_reserved_bytes ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("transient gauges remained #{inspect(actual)}; expected #{inspect(expected)}")

      true ->
        Process.sleep(10)
        await_transient_gauges(expected, deadline)
    end
  end

  defp await_metric(name, predicate) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_metric(name, predicate, deadline)
  end

  defp await_metric(name, predicate, deadline) do
    value = PaseoRelay.Metrics.value(name)

    cond do
      predicate.(value) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("#{name} remained #{value}")

      true ->
        Process.sleep(10)
        await_metric(name, predicate, deadline)
    end
  end

  defp await_reserved(predicate) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_reserved(predicate, deadline)
  end

  defp await_reserved(predicate, deadline) do
    value = PaseoRelay.Delivery.Budget.reserved_bytes()

    cond do
      predicate.(value) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("reserved bytes remained #{value}")

      true ->
        Process.sleep(10)
        await_reserved(predicate, deadline)
    end
  end

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
