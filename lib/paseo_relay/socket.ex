defmodule PaseoRelay.Socket do
  @moduledoc false

  @behaviour :cowboy_websocket

  alias PaseoRelay.Delivery
  alias PaseoRelay.Delivery.Budget
  alias PaseoRelay.Delivery.Writer
  alias PaseoRelay.Ownership.Owner

  @impl true
  def init(request, options) do
    with true <- :cowboy_websocket.is_upgrade_request(request),
         {:ok, connection} <- connection(request),
         decision <-
           PaseoRelay.Ownership.route(
             connection.server_id,
             options.ownership_target,
             options.config.minimum_cluster_size
           ),
         {:local, owner, reservation} <- decision,
         {:ok, admission} <- admit(options.connection_budget, owner, reservation) do
      state = %{
        admission: admission,
        config: options.config,
        connection: connection,
        owner: owner,
        reservation: reservation,
        pending: :queue.new()
      }

      {:cowboy_websocket, request, state, websocket_options()}
    else
      {:reroute, _target} = decision ->
        PaseoRelay.Metrics.inc(:reroute_responses)
        reply(request, 409, reroute_headers(decision, options.reroute_header), "")

      {:unavailable, reason} ->
        reply(request, 503, %{}, Atom.to_string(reason))

      {:error, :capacity} ->
        PaseoRelay.Metrics.inc(:connection_rejections)
        reply(request, 503, %{}, "Relay connection capacity")

      {:error, :configuration_mismatch} ->
        reply(request, 503, %{}, "Relay capacity configuration")

      {:error, :unavailable} ->
        reply(request, 503, %{}, "Relay capacity unavailable")

      false ->
        reply(request, 426, %{}, "Expected WebSocket upgrade")

      {:error, message} ->
        reply(request, 400, %{}, message)
    end
  end

  @impl true
  def websocket_init(state) do
    Process.flag(:message_queue_data, :off_heap)

    Process.flag(:max_heap_size, %{
      size: state.config.websocket_max_heap_words,
      include_shared_binaries: true,
      kill: true
    })

    with {:ok, connection_budget} <- PaseoRelay.ConnectionBudget.attach(state.admission),
         {:ok, ingress_budget} <- Budget.attach(),
         {:ok, pressure} <- PaseoRelay.Delivery.Pressure.attach(),
         {:ok, writer} <-
           Writer.start(
             self(),
             state.config.delivery_timeout_ms,
             state.config.control_queue_bytes
           ) do
      state =
        Map.put(state, :admission, %{
          token: state.admission,
          monitor: Process.monitor(connection_budget)
        })
        |> Map.put(:ingress_budget_ref, Process.monitor(ingress_budget))
        |> Map.put(:pressure_ref, Process.monitor(pressure))

      attach_writer(writer, state)
    else
      {:error, _reason} -> {[{:close, 1012, "Session expired"}], state}
    end
  end

  @impl true
  def websocket_handle(
        frame,
        %{connection: %{version: 2, role: :server, connection_id: ""}} = state
      ) do
    handle_control_input(frame, state)
  end

  def websocket_handle({opcode, payload}, state) when opcode in [:text, :binary] do
    PaseoRelay.Metrics.observe_frame(byte_size(payload))

    case PaseoRelay.Delivery.Budget.admit(byte_size(payload)) do
      :ok -> admit_input(opcode, payload, state)
      {:error, _reason} -> {[{:close, 1013, "Relay ingress capacity"}], state}
    end
  end

  def websocket_handle(_control, state), do: {[], state}

  @impl true
  def websocket_info({:relay_frame, _writer, _reference, opcode, payload}, state) do
    {[{opcode, payload}], state}
  end

  def websocket_info({:relay_write_barrier, writer, reference}, state) do
    Writer.acknowledge(writer, reference)
    {[], state}
  end

  def websocket_info({reference, result}, %{delivery: %{ref: reference}} = state) do
    Process.demonitor(reference, [:flush])
    state = finish_delivery(state)

    case result do
      :ok -> continue_or_resume(state)
      {:error, :attach_timeout} -> {[{:close, 1013, "Data route unavailable"}], state}
      {:error, _reason} -> {[{:close, 1013, "Delivery unavailable"}], state}
    end
  end

  def websocket_info(
        {:DOWN, reference, :process, _pid, _reason},
        %{delivery: %{ref: reference}} = state
      ) do
    {[{:close, 1013, "Delivery unavailable"}], finish_delivery(state)}
  end

  def websocket_info(:relay_memory_pressure, state),
    do: {[{:close, 1013, "Relay memory pressure"}], cancel_delivery(state)}

  def websocket_info({:DOWN, ref, :process, _owner, _reason}, %{owner_ref: ref} = state) do
    {[{:close, 1012, "Session owner moved"}], cancel_delivery(state)}
  end

  def websocket_info({:DOWN, ref, :process, _writer, _reason}, %{writer_ref: ref} = state) do
    {[{:close, 1013, "Delivery unavailable"}], cancel_delivery(state)}
  end

  def websocket_info(
        {:DOWN, ref, :process, _budget, _reason},
        %{admission: %{monitor: ref}} = state
      ) do
    {[{:close, 1013, "Relay capacity unavailable"}], cancel_delivery(state)}
  end

  def websocket_info(
        {:DOWN, ref, :process, _budget, _reason},
        %{ingress_budget_ref: ref} = state
      ) do
    {[{:close, 1013, "Relay ingress capacity unavailable"}], cancel_delivery(state)}
  end

  def websocket_info(
        {:DOWN, ref, :process, _pressure, _reason},
        %{pressure_ref: ref} = state
      ) do
    {[{:close, 1013, "Relay memory pressure unavailable"}], cancel_delivery(state)}
  end

  def websocket_info({:relay_close, code, reason}, state) do
    {[{:close, code, reason}], cancel_delivery(state)}
  end

  def websocket_info(_message, state), do: {[], state}

  @impl true
  def terminate(_reason, _request, %{owner: owner} = state) do
    if delivery = state[:delivery] do
      stop_delivery_task(delivery)
      complete_delivery_metrics(delivery)
    end

    PaseoRelay.Delivery.Budget.release_all()
    PaseoRelay.ConnectionBudget.release(admission_token(state.admission))
    PaseoRelay.Delivery.Pressure.leave(self())
    Owner.detach(owner, self())
  end

  def terminate(_reason, _request, _state), do: :ok

  defp admit_input(opcode, payload, %{delivery: _delivery} = state) do
    pending = :queue.in({opcode, payload}, state.pending)
    {[{:active, false}], %{state | pending: pending}}
  end

  defp admit_input(opcode, payload, state) do
    {[{:active, false}], start_delivery(opcode, payload, state)}
  end

  defp start_delivery(opcode, payload, state) do
    PaseoRelay.Metrics.inc(:backpressured_sources)
    PaseoRelay.Metrics.inc(:inflight_delivery_bytes, byte_size(payload))
    PaseoRelay.Delivery.Pressure.block(self())
    source = self()
    task = Task.async(fn -> deliver_input(payload, opcode, state, source) end)

    delivery = %{
      ref: task.ref,
      pid: task.pid,
      bytes: byte_size(payload),
      started: System.monotonic_time()
    }

    Map.put(state, :delivery, delivery)
  end

  defp continue_or_resume(state) do
    case :queue.out(state.pending) do
      {{:value, {opcode, payload}}, pending} ->
        state = %{state | pending: pending}
        {[], start_delivery(opcode, payload, state)}

      {:empty, _pending} ->
        {[{:active, true}], state}
    end
  end

  defp deliver_input(payload, opcode, state, source) do
    timeout = state.config.delivery_timeout_ms
    attach_timeout = state.config.data_attach_timeout_ms

    case Owner.destinations(state.owner, source, attach_timeout) do
      {:ok, destinations} -> Delivery.deliver(destinations, opcode, payload, timeout)
      {:error, :attach_timeout} -> {:error, :attach_timeout}
      {:error, _reason} -> {:error, :delivery_unavailable}
    end
  end

  defp handle_control_input({:text, payload}, state) do
    PaseoRelay.Metrics.observe_frame(byte_size(payload))

    with {:ok, %{"type" => "ping"}} <- Jason.decode(payload) do
      pong = Jason.encode!(%{type: "pong", ts: System.system_time(:millisecond)})

      case Owner.control(state.owner, self(), pong) do
        :ok -> {[], state}
        {:error, _reason} -> {[{:close, 1013, "Delivery unavailable"}], state}
      end
    else
      _ -> {[], state}
    end
  end

  defp handle_control_input(_frame, state), do: {[], state}

  defp finish_delivery(state) do
    delivery = state.delivery
    PaseoRelay.Delivery.Pressure.unblock(self())
    PaseoRelay.Delivery.Budget.release(delivery.bytes)
    complete_delivery_metrics(delivery)
    Map.delete(state, :delivery)
  end

  defp complete_delivery_metrics(delivery) do
    PaseoRelay.Metrics.dec(:backpressured_sources)
    PaseoRelay.Metrics.dec(:inflight_delivery_bytes, delivery.bytes)
    PaseoRelay.Metrics.observe_delivery_wait(System.monotonic_time() - delivery.started)
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

  defp connection(request) do
    query =
      request
      |> :cowboy_req.parse_qs()
      |> Map.new(fn
        {key, true} -> {key, ""}
        {key, value} -> {key, value}
      end)

    PaseoRelay.Connection.from_query(query)
  end

  defp admit({namespace, limit}, owner, reservation) do
    case PaseoRelay.ConnectionBudget.admit(namespace, limit) do
      {:ok, token} ->
        {:ok, token}

      {:error, _reason} = error ->
        Owner.cancel(owner, reservation)
        error
    end
  end

  defp admission_token(%{token: token}), do: token
  defp admission_token(token), do: token

  defp websocket_options do
    %{
      active_n: 1,
      compress: false,
      idle_timeout: :infinity,
      max_frame_size: PaseoRelay.Protocol.maximum_message_payload_bytes()
    }
  end

  defp reply(request, status, headers, body) do
    request = :cowboy_req.reply(status, headers, body, request)
    {:ok, request, nil}
  end

  defp reroute_headers(decision, header) do
    decision
    |> PaseoRelay.Reroute.headers(header)
    |> Map.new(fn {name, value} -> {to_string(name), value} end)
  end

  defp monitor(process) do
    if Process.alive?(process), do: {:ok, Process.monitor(process)}, else: {:error, :closed}
  end

  defp attach_writer(writer, state) do
    case Owner.attach(state.owner, state.reservation, self(), state.connection, writer) do
      :ok ->
        with {:ok, owner_ref} <- monitor(state.owner),
             {:ok, writer_ref} <- monitor(writer) do
          {[],
           state
           |> Map.put(:writer, writer)
           |> Map.put(:writer_ref, writer_ref)
           |> Map.put(:owner_ref, owner_ref)}
        else
          _reason ->
            Process.exit(writer, :normal)
            {[{:close, 1012, "Session expired"}], state}
        end

      :closed ->
        Process.exit(writer, :normal)
        {[{:close, 1012, "Session expired"}], state}
    end
  end
end
