defmodule PaseoRelay.Socket do
  @behaviour WebSock

  alias PaseoRelay.Delivery
  alias PaseoRelay.Delivery.Writer
  alias PaseoRelay.Ownership.Owner

  @impl true
  def init(%{connection: connection, owner: owner, reservation: reservation} = state) do
    with {:ok, writer} <- Writer.start_link(self()),
         :ok <- Owner.attach(owner, reservation, self(), connection, writer) do
      PaseoRelay.Metrics.inc(:active_websockets)
      owner_ref = Process.monitor(owner)

      {:ok,
       state
       |> Map.put(:writer, writer)
       |> Map.put(:owner_ref, owner_ref)
       |> Map.put(:metrics_active, true)}
    else
      _ -> {:stop, :normal, {1012, "Session expired"}, state}
    end
  end

  @impl true
  def handle_in(
        {payload, [opcode: opcode]},
        %{connection: %{version: 2, role: :server, connection_id: ""}} = state
      ) do
    PaseoRelay.Metrics.observe_frame(byte_size(payload))
    PaseoRelay.Delivery.Budget.release_all()
    handle_control_input(opcode, payload, state)
  end

  def handle_in({payload, [opcode: opcode]}, state) do
    PaseoRelay.Metrics.observe_frame(byte_size(payload))
    PaseoRelay.Metrics.inc(:backpressured_sources)
    PaseoRelay.Metrics.inc(:inflight_delivery_bytes, byte_size(payload))
    PaseoRelay.Delivery.Pressure.enter(self())
    source = self()
    task = Task.async(fn -> deliver_input(payload, opcode, state, source) end)

    delivery = %{
      ref: task.ref,
      pid: task.pid,
      bytes: byte_size(payload),
      started: System.monotonic_time()
    }

    {:suspend, Map.put(state, :delivery, delivery)}
  end

  @impl true
  def handle_control({payload, [opcode: _opcode]}, state) do
    PaseoRelay.Delivery.Budget.release(byte_size(payload))
    {:ok, state}
  end

  defp deliver_input(payload, opcode, state, source) do
    timeout = operation(:delivery_timeout_ms, 30_000)
    attach_timeout = operation(:data_attach_timeout_ms, 15_000)

    case Owner.destinations(state.owner, source, attach_timeout) do
      {:ok, destinations} ->
        forward(destinations, opcode, payload, timeout)

      {:error, :attach_timeout} ->
        {:error, :attach_timeout}

      {:error, _reason} ->
        {:error, :delivery_unavailable}
    end
  end

  @impl true
  def handle_info({:relay_frame, _writer, _reference, opcode, payload}, state) do
    {:push, {opcode, payload}, state}
  end

  def handle_info({:relay_write_barrier, writer, reference}, state) do
    Writer.acknowledge(writer, reference)
    {:ok, state}
  end

  def handle_info({:relay_control, payload}, state), do: {:push, {:text, payload}, state}

  def handle_info({reference, result}, %{delivery: %{ref: reference}} = state) do
    Process.demonitor(reference, [:flush])
    state = finish_delivery(state)

    case result do
      :ok -> {:resume, state}
      {:error, :attach_timeout} -> {:stop, :normal, {1013, "Data route unavailable"}, state}
      {:error, _reason} -> {:stop, :normal, {1013, "Delivery unavailable"}, state}
    end
  end

  def handle_info(
        {:DOWN, reference, :process, _pid, _reason},
        %{delivery: %{ref: reference}} = state
      ) do
    {:stop, :normal, {1013, "Delivery unavailable"}, finish_delivery(state)}
  end

  def handle_info(:relay_memory_pressure, %{delivery: delivery} = state) do
    stop_delivery_task(delivery)
    {:stop, :normal, {1013, "Relay memory pressure"}, finish_delivery(state)}
  end

  def handle_info({:DOWN, ref, :process, _owner, _reason}, %{owner_ref: ref} = state) do
    {:stop, :normal, {1012, "Session owner moved"}, cancel_delivery(state)}
  end

  def handle_info({:relay_close, code, reason}, state),
    do: {:stop, :normal, {code, reason}, cancel_delivery(state)}

  def handle_info(_message, state), do: {:ok, state}

  @impl true
  def terminate(_reason, %{owner: owner} = state) do
    if delivery = state[:delivery] do
      stop_delivery_task(delivery)
      PaseoRelay.Metrics.dec(:backpressured_sources)
      PaseoRelay.Metrics.dec(:inflight_delivery_bytes, delivery.bytes)
      PaseoRelay.Metrics.observe_delivery_wait(System.monotonic_time() - delivery.started)
    end

    PaseoRelay.Delivery.Budget.release_all()
    PaseoRelay.Delivery.Pressure.leave(self())
    Owner.detach(owner, self())
    if state[:metrics_active], do: PaseoRelay.Metrics.dec(:active_websockets)
  end

  defp handle_control_input(:text, payload, state) do
    with {:ok, %{"type" => "ping"}} <- Jason.decode(payload) do
      {:reply, :ok, {:text, Jason.encode!(%{type: "pong", ts: System.system_time(:millisecond)})},
       state}
    else
      _ -> {:ok, state}
    end
  end

  defp handle_control_input(_opcode, _payload, state), do: {:ok, state}

  defp forward([], _opcode, _payload, _timeout), do: :ok

  defp forward(destinations, opcode, payload, timeout) do
    case Delivery.deliver(destinations, opcode, payload, timeout) do
      :ok ->
        :ok

      error ->
        error
    end
  end

  defp operation(key, default) do
    :paseo_relay
    |> Application.get_env(:operations, [])
    |> Keyword.get(key, default)
  end

  defp finish_delivery(state) do
    delivery = state.delivery
    PaseoRelay.Delivery.Pressure.leave(self())
    PaseoRelay.Delivery.Budget.release_all()
    PaseoRelay.Metrics.dec(:backpressured_sources)
    PaseoRelay.Metrics.dec(:inflight_delivery_bytes, delivery.bytes)
    PaseoRelay.Metrics.observe_delivery_wait(System.monotonic_time() - delivery.started)
    Map.delete(state, :delivery)
  end

  defp cancel_delivery(%{delivery: delivery} = state) do
    stop_delivery_task(delivery)
    finish_delivery(state)
  end

  defp cancel_delivery(state), do: state

  defp stop_delivery_task(delivery) do
    Process.unlink(delivery.pid)
    Process.exit(delivery.pid, :kill)
    Process.demonitor(delivery.ref, [:flush])
  end
end
