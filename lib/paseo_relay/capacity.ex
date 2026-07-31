defmodule PaseoRelay.Capacity do
  @moduledoc false

  use GenServer

  @reservation_timeout_ms 5_000
  @check_interval_ms 1_000
  @pressure_recheck_ms 100
  @initial_max_shed_batch 64
  @max_shed_batch 1_024
  @zero_snapshot %{
    active_websockets: 0,
    ingress_reserved_bytes: 0,
    inflight_delivery_bytes: 0,
    backpressured_sources: 0
  }

  def start_link(config), do: GenServer.start_link(__MODULE__, config, name: __MODULE__)

  def admit_connection(namespace, limit) do
    call({:admit_connection, namespace, limit}, {:error, :unavailable})
  end

  def attach_connection(token),
    do: call({:attach_connection, token, self()}, {:error, :unavailable})

  def release_connection(token), do: call({:release_connection, token}, :ok)
  def active_connections(namespace), do: call({:active_connections, namespace}, 0)

  def admit_message(payload_bytes) do
    call({:admit_message, self(), payload_bytes}, {:error, :unavailable})
  end

  def start_delivery(token), do: call({:start_delivery, token}, {:error, :unavailable})
  def finish_message(token), do: call({:finish_message, token, true}, :ok)
  def cancel_message(token), do: call({:finish_message, token, false}, :ok)

  def value(name), do: call({:value, name}, 0)
  def snapshot, do: call(:snapshot, @zero_snapshot)
  def check_now, do: call(:check_now, {:error, :unavailable})
  def set_watermark(bytes), do: call({:set_watermark, bytes}, {:error, :unavailable})

  @impl true
  def init(config) do
    schedule_check()

    {:ok,
     %{
       ingress_limit: config.ingress_budget_bytes,
       ingress_weight: config.ingress_weight,
       watermark: config.memory_watermark_bytes,
       namespaces: %{},
       connections: %{},
       sockets: %{},
       monitors: %{},
       messages: %{},
       active: :gb_trees.empty(),
       blocked: :gb_trees.empty(),
       sequence: 0,
       active_websockets: 0,
       reserved_bytes: 0,
       inflight_bytes: 0,
       blocked_sources: 0,
       pressure: nil,
       pressure_recheck?: false
     }}
  end

  @impl true
  def handle_call({:admit_connection, namespace, limit}, _from, state) do
    namespace_state = Map.get(state.namespaces, namespace, %{limit: limit, active: 0})

    cond do
      state.pressure != nil ->
        {:reply, {:error, :pressure}, state}

      namespace_state.limit != limit ->
        {:reply, {:error, :configuration_mismatch}, state}

      namespace_state.active >= limit ->
        {:reply, {:error, :capacity}, state}

      true ->
        token = make_ref()
        timer = Process.send_after(self(), {:expire, token}, @reservation_timeout_ms)
        namespace_state = %{namespace_state | active: namespace_state.active + 1}
        connection = %{namespace: namespace, status: {:reservation, timer}}

        {:reply, {:ok, token},
         %{
           state
           | namespaces: Map.put(state.namespaces, namespace, namespace_state),
             connections: Map.put(state.connections, token, connection)
         }}
    end
  end

  def handle_call({:attach_connection, token, socket}, _from, state) do
    case state.connections[token] do
      %{status: {:reservation, timer}} = connection ->
        Process.cancel_timer(timer)
        monitor = Process.monitor(socket)
        {active_key, state} = next_key(state)

        socket_state = %{
          monitor: monitor,
          connection: token,
          messages: MapSet.new(),
          active_key: active_key,
          blocked_key: nil,
          shedding: false
        }

        connection = %{connection | status: {:active, socket}}

        {:reply, {:ok, self()},
         %{
           state
           | connections: Map.put(state.connections, token, connection),
             sockets: Map.put(state.sockets, socket, socket_state),
             monitors: Map.put(state.monitors, monitor, socket),
             active: :gb_trees.insert(active_key, socket, state.active),
             active_websockets: state.active_websockets + 1
         }}

      _missing_or_attached ->
        {:reply, {:error, :expired}, state}
    end
  end

  def handle_call({:release_connection, token}, _from, state) do
    {:reply, :ok, release_connection(state, token, true)}
  end

  def handle_call({:active_connections, namespace}, _from, state) do
    active = state.namespaces |> Map.get(namespace, %{active: 0}) |> Map.fetch!(:active)
    {:reply, active, state}
  end

  def handle_call({:admit_message, socket, payload_bytes}, _from, state) do
    weighted_bytes = payload_bytes * state.ingress_weight

    cond do
      not Map.has_key?(state.sockets, socket) ->
        {:reply, {:error, :closed}, state}

      state.sockets[socket].shedding ->
        {:reply, {:error, :closed}, state}

      state.pressure != nil ->
        {:reply, {:error, :pressure}, state}

      weighted_bytes > state.ingress_limit ->
        {:reply, {:error, :message_exceeds_budget}, state}

      state.reserved_bytes + weighted_bytes > state.ingress_limit ->
        {:reply, {:error, :budget_exhausted}, state}

      true ->
        token = make_ref()

        message = %{
          socket: socket,
          payload_bytes: payload_bytes,
          weighted_bytes: weighted_bytes,
          status: :reserved,
          started: nil
        }

        socket_state = state.sockets[socket]
        socket_state = %{socket_state | messages: MapSet.put(socket_state.messages, token)}

        {:reply, {:ok, token},
         %{
           state
           | messages: Map.put(state.messages, token, message),
             sockets: Map.put(state.sockets, socket, socket_state),
             reserved_bytes: state.reserved_bytes + weighted_bytes
         }}
    end
  end

  def handle_call({:start_delivery, token}, _from, state) do
    case state.messages[token] do
      %{status: :reserved, socket: socket} = message ->
        socket_state = state.sockets[socket]

        if socket_state.shedding do
          {:reply, {:error, :closed}, state}
        else
          {blocked_key, state} = ensure_blocked(state, socket, socket_state)
          message = %{message | status: :delivering, started: System.monotonic_time()}
          socket_state = %{state.sockets[socket] | blocked_key: blocked_key}

          {:reply, :ok,
           %{
             state
             | messages: Map.put(state.messages, token, message),
               sockets: Map.put(state.sockets, socket, socket_state),
               inflight_bytes: state.inflight_bytes + message.payload_bytes
           }}
        end

      _missing_or_started ->
        {:reply, {:error, :expired}, state}
    end
  end

  def handle_call({:finish_message, token, observe_wait?}, _from, state) do
    {:reply, :ok, remove_message(state, token, observe_wait?)}
  end

  def handle_call({:value, :active_websockets}, _from, state),
    do: {:reply, state.active_websockets, state}

  def handle_call({:value, :ingress_reserved_bytes}, _from, state),
    do: {:reply, state.reserved_bytes, state}

  def handle_call({:value, :inflight_delivery_bytes}, _from, state),
    do: {:reply, state.inflight_bytes, state}

  def handle_call({:value, :backpressured_sources}, _from, state),
    do: {:reply, state.blocked_sources, state}

  def handle_call(:snapshot, _from, state) do
    snapshot = %{
      active_websockets: state.active_websockets,
      ingress_reserved_bytes: state.reserved_bytes,
      inflight_delivery_bytes: state.inflight_bytes,
      backpressured_sources: state.blocked_sources
    }

    {:reply, snapshot, state}
  end

  def handle_call(:check_now, _from, state), do: {:reply, :ok, shed_if_needed(state)}

  def handle_call({:set_watermark, 0}, _from, state),
    do: {:reply, :ok, %{state | watermark: 0, pressure: nil}}

  def handle_call({:set_watermark, bytes}, _from, state),
    do: {:reply, :ok, %{state | watermark: bytes}}

  @impl true
  def handle_info({:expire, token}, state) do
    case state.connections[token] do
      %{status: {:reservation, _timer}} ->
        {:noreply, release_connection(state, token, false)}

      _missing_or_attached ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, socket, _reason}, state) do
    if state.monitors[monitor] == socket,
      do: {:noreply, remove_socket(state, socket, false)},
      else: {:noreply, state}
  end

  def handle_info(:check, state) do
    schedule_check()
    {:noreply, shed_if_needed(state)}
  end

  def handle_info(:pressure_recheck, state) do
    {:noreply, state |> Map.put(:pressure_recheck?, false) |> shed_if_needed()}
  end

  defp release_connection(state, token, demonitor?) do
    case state.connections[token] do
      nil ->
        state

      %{status: {:reservation, timer}} ->
        Process.cancel_timer(timer)
        remove_connection_record(state, token)

      %{status: {:active, socket}} ->
        remove_socket(state, socket, demonitor?)
    end
  end

  defp remove_socket(state, socket, demonitor?) do
    case state.sockets[socket] do
      nil ->
        state

      socket_state ->
        if demonitor?, do: Process.demonitor(socket_state.monitor, [:flush])

        state =
          Enum.reduce(socket_state.messages, state, fn token, current ->
            remove_message(current, token, false)
          end)

        socket_state = state.sockets[socket] || socket_state
        token = socket_state.connection
        monitor = socket_state.monitor

        state = %{
          state
          | sockets: Map.delete(state.sockets, socket),
            monitors: Map.delete(state.monitors, monitor),
            active: delete_key(state.active, socket_state.active_key),
            blocked: delete_key(state.blocked, socket_state.blocked_key),
            active_websockets: state.active_websockets - 1
        }

        remove_connection_record(state, token)
    end
  end

  defp remove_connection_record(state, token) do
    case Map.pop(state.connections, token) do
      {nil, _connections} ->
        state

      {%{namespace: namespace}, connections} ->
        namespace_state = Map.fetch!(state.namespaces, namespace)
        active = namespace_state.active - 1

        namespaces =
          if active == 0,
            do: Map.delete(state.namespaces, namespace),
            else: Map.put(state.namespaces, namespace, %{namespace_state | active: active})

        %{state | namespaces: namespaces, connections: connections}
    end
  end

  defp remove_message(state, token, observe_wait?) do
    case Map.pop(state.messages, token) do
      {nil, _messages} ->
        state

      {message, messages} ->
        if observe_wait? and message.status == :delivering do
          PaseoRelay.Metrics.observe_delivery_wait(System.monotonic_time() - message.started)
        end

        state = %{
          state
          | messages: messages,
            reserved_bytes: state.reserved_bytes - message.weighted_bytes,
            inflight_bytes:
              state.inflight_bytes -
                if(message.status == :delivering, do: message.payload_bytes, else: 0)
        }

        case state.sockets[message.socket] do
          nil ->
            state

          socket_state ->
            socket_state = %{
              socket_state
              | messages: MapSet.delete(socket_state.messages, token)
            }

            if message.status == :delivering and
                 not delivering_for_socket?(state, socket_state.messages) do
              %{
                state
                | sockets:
                    Map.put(state.sockets, message.socket, %{socket_state | blocked_key: nil}),
                  blocked: delete_key(state.blocked, socket_state.blocked_key),
                  blocked_sources: state.blocked_sources - 1
              }
            else
              %{state | sockets: Map.put(state.sockets, message.socket, socket_state)}
            end
        end
    end
  end

  defp delivering_for_socket?(state, tokens) do
    Enum.any?(tokens, fn token -> match?(%{status: :delivering}, state.messages[token]) end)
  end

  defp ensure_blocked(state, socket, %{blocked_key: nil}) do
    {key, state} = next_key(state)

    {key,
     %{
       state
       | blocked: :gb_trees.insert(key, socket, state.blocked),
         blocked_sources: state.blocked_sources + 1
     }}
  end

  defp ensure_blocked(state, _socket, %{blocked_key: key}), do: {key, state}

  defp shed_if_needed(state) do
    memory = :erlang.memory(:total)
    recovery = recovery_threshold(state.watermark)

    cond do
      state.watermark == 0 ->
        %{state | pressure: nil}

      state.pressure != nil and memory <= recovery ->
        %{state | pressure: nil}

      memory >= state.watermark or state.pressure != nil ->
        batch_size = pressure_batch(state.pressure, memory, recovery, state.watermark)
        {state, victims} = shed_candidates(state, batch_size, 0)

        state
        |> Map.put(:pressure, %{memory: memory, victims: victims, batch: batch_size})
        |> schedule_pressure_recheck()

      true ->
        state
    end
  end

  defp pressure_batch(nil, memory, _recovery, watermark) do
    maximum_message = PaseoRelay.Protocol.maximum_message_payload_bytes()

    memory
    |> Kernel.-(watermark)
    |> Kernel.+(maximum_message - 1)
    |> div(maximum_message)
    |> max(1)
    |> min(@initial_max_shed_batch)
  end

  defp pressure_batch(previous, memory, recovery, _watermark) do
    relief = previous.memory - memory

    if relief > 0 and previous.victims > 0 do
      bytes_per_victim = max(div(relief, previous.victims), 1)

      memory
      |> Kernel.-(recovery)
      |> Kernel.+(bytes_per_victim - 1)
      |> div(bytes_per_victim)
      |> max(1)
      |> min(@max_shed_batch)
    else
      previous.batch
      |> Kernel.*(2)
      |> max(1)
      |> min(@max_shed_batch)
    end
  end

  defp recovery_threshold(0), do: 0

  defp recovery_threshold(watermark) do
    max(watermark - PaseoRelay.Protocol.maximum_message_payload_bytes(), 0)
  end

  defp shed_candidates(state, 0, victims), do: {state, victims}

  defp shed_candidates(state, remaining, victims) do
    case next_candidate(state) do
      {:ok, socket, state} ->
        send(socket, :relay_memory_pressure)
        PaseoRelay.Metrics.inc(:memory_pressure_disconnects)
        shed_candidates(state, remaining - 1, victims + 1)

      :empty ->
        {state, victims}
    end
  end

  defp schedule_pressure_recheck(%{pressure_recheck?: true} = state), do: state

  defp schedule_pressure_recheck(state) do
    if :gb_trees.is_empty(state.active) and :gb_trees.is_empty(state.blocked) do
      state
    else
      Process.send_after(self(), :pressure_recheck, @pressure_recheck_ms)
      %{state | pressure_recheck?: true}
    end
  end

  defp next_candidate(state) do
    cond do
      not :gb_trees.is_empty(state.blocked) -> pop_oldest(state.blocked, :blocked, state)
      not :gb_trees.is_empty(state.active) -> pop_newest(state.active, state)
      true -> :empty
    end
  end

  defp pop_oldest(tree, _kind, state) do
    {_key, socket} = :gb_trees.smallest(tree)
    socket_state = state.sockets[socket]

    state = %{
      state
      | active: delete_key(state.active, socket_state.active_key),
        blocked: delete_key(state.blocked, socket_state.blocked_key),
        sockets:
          Map.put(state.sockets, socket, %{
            socket_state
            | active_key: nil,
              blocked_key: nil,
              shedding: true
          })
    }

    {:ok, socket, state}
  end

  defp pop_newest(tree, state) do
    {_key, socket} = :gb_trees.largest(tree)
    socket_state = state.sockets[socket]

    state = %{
      state
      | active: delete_key(state.active, socket_state.active_key),
        sockets:
          Map.put(state.sockets, socket, %{
            socket_state
            | active_key: nil,
              shedding: true
          })
    }

    {:ok, socket, state}
  end

  defp delete_key(tree, nil), do: tree
  defp delete_key(tree, key), do: :gb_trees.delete_any(key, tree)

  defp next_key(state) do
    sequence = state.sequence + 1
    {{System.monotonic_time(), sequence}, %{state | sequence: sequence}}
  end

  defp call(message, fallback) do
    GenServer.call(__MODULE__, message)
  catch
    :exit, _reason -> fallback
  end

  defp schedule_check, do: Process.send_after(self(), :check, @check_interval_ms)
end
