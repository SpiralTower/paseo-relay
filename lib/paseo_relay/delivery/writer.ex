defmodule PaseoRelay.Delivery.Writer do
  @moduledoc false

  use GenServer

  @type token :: reference()
  def start(destination, delivery_timeout_ms, control_queue_bytes),
    do: GenServer.start(__MODULE__, {destination, delivery_timeout_ms, control_queue_bytes})

  def reserve(writer, byte_count, deadline) do
    GenServer.call(writer, {:reserve, byte_count, deadline}, :infinity)
  catch
    :exit, _reason -> {:error, :destination_closed}
  end

  def write(writer, token, opcode, payload) do
    GenServer.call(writer, {:write, token, opcode, payload}, :infinity)
  catch
    :exit, _reason -> {:error, :destination_closed}
  end

  def acknowledge(writer, reference), do: send(writer, {:written, reference})

  def control(writer, payload) do
    GenServer.call(writer, {:control, payload}, :infinity)
  catch
    :exit, _reason -> {:error, :destination_closed}
  end

  def close(writer, code, reason), do: GenServer.cast(writer, {:close, code, reason})

  @impl true
  def init({destination, delivery_timeout_ms, control_queue_bytes}) do
    Process.flag(:message_queue_data, :off_heap)

    {:ok,
     %{
       destination: destination,
       destination_ref: Process.monitor(destination),
       delivery_timeout_ms: delivery_timeout_ms,
       control_queue_bytes: control_queue_bytes,
       active: nil,
       queued: :queue.new(),
       queued_control_bytes: 0
     }}
  end

  @impl true
  def handle_call({:reserve, byte_count, deadline}, from, %{active: nil} = state) do
    case remaining(deadline) do
      0 -> {:reply, {:error, :timeout}, state}
      timeout -> grant(from, byte_count, timeout, state)
    end
  end

  def handle_call({:reserve, byte_count, deadline}, from, state) do
    entry = %{kind: :payload, from: from, bytes: byte_count, deadline: deadline}
    {:noreply, %{state | queued: :queue.in(entry, state.queued)}}
  end

  def handle_call({:write, token, opcode, payload}, from, %{active: %{token: token}} = state) do
    reference = make_ref()
    PaseoRelay.Metrics.inc(:frames_forwarded)
    PaseoRelay.Metrics.inc(:bytes_forwarded, byte_size(payload))
    send(state.destination, {:relay_frame, self(), reference, opcode, payload})
    send(state.destination, {:relay_write_barrier, self(), reference})
    active = %{state.active | write: from, write_reference: reference}
    {:noreply, %{state | active: active}}
  end

  def handle_call({:write, _token, _opcode, _payload}, _from, state) do
    {:reply, {:error, :invalid_reservation}, state}
  end

  def handle_call({:control, payload}, _from, state) do
    deadline = System.monotonic_time(:millisecond) + state.delivery_timeout_ms
    enqueue_control(payload, deadline, state)
  end

  @impl true
  def handle_cast({:close, code, reason}, state) do
    send(state.destination, {:relay_close, code, reason})
    {:stop, :normal, reject_all(state, {:error, :destination_closed})}
  end

  defp enqueue_control(payload, deadline, %{active: nil} = state) do
    {:reply, :ok, start_control(payload, deadline, state)}
  end

  defp enqueue_control(payload, deadline, state) do
    bytes = byte_size(payload)

    if state.queued_control_bytes + bytes <= state.control_queue_bytes do
      entry = %{kind: :control, payload: payload, bytes: bytes, deadline: deadline}

      {:reply, :ok,
       %{
         state
         | queued: :queue.in(entry, state.queued),
           queued_control_bytes: state.queued_control_bytes + bytes
       }}
    else
      PaseoRelay.Metrics.inc(:slow_consumer_disconnects)
      send(state.destination, {:relay_close, 1013, "Slow consumer"})
      {:stop, :normal, {:error, :timeout}, reject_all(state, {:error, :timeout})}
    end
  end

  @impl true
  def handle_info({:written, reference}, %{active: %{write_reference: reference}} = state) do
    {:noreply, complete_active(state, :ok)}
  end

  def handle_info({:reservation_timeout, token}, %{active: %{token: token}} = state) do
    PaseoRelay.Metrics.inc(:delivery_timeouts)
    PaseoRelay.Metrics.inc(:slow_consumer_disconnects)
    send(state.destination, {:relay_close, 1013, "Slow consumer"})
    {:stop, :normal, reject_all(state, {:error, :timeout})}
  end

  def handle_info(
        {:DOWN, reference, :process, destination, _reason},
        %{destination: destination, destination_ref: reference} = state
      ) do
    state = reject_all(state, {:error, :destination_closed})
    {:stop, :normal, state}
  end

  def handle_info(
        {:DOWN, reference, :process, _pid, _reason},
        %{active: %{source_ref: reference}} = state
      ) do
    {:noreply, complete_active(state, {:error, :source_closed})}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp grant(from, byte_count, timeout, state) do
    token = make_ref()
    source = elem(from, 0)
    timer = Process.send_after(self(), {:reservation_timeout, token}, timeout)

    active = %{
      kind: :payload,
      token: token,
      bytes: byte_count,
      source_ref: Process.monitor(source),
      timer: timer,
      write: nil,
      write_reference: nil
    }

    GenServer.reply(from, {:ok, token})
    {:noreply, %{state | active: active}}
  end

  defp complete_active(%{active: active} = state, result) do
    Process.cancel_timer(active.timer)
    if active.source_ref, do: Process.demonitor(active.source_ref, [:flush])
    if active.write, do: GenServer.reply(active.write, result)
    state |> Map.put(:active, nil) |> grant_next()
  end

  defp grant_next(state) do
    case :queue.out(state.queued) do
      {:empty, _queue} ->
        state

      {{:value, %{kind: :payload} = entry}, queued} ->
        state = %{state | queued: queued}

        case remaining(entry.deadline) do
          0 ->
            GenServer.reply(entry.from, {:error, :timeout})
            grant_next(state)

          timeout ->
            {:noreply, state} = grant(entry.from, entry.bytes, timeout, state)
            state
        end

      {{:value, %{kind: :control} = entry}, queued} ->
        state = %{
          state
          | queued: queued,
            queued_control_bytes: state.queued_control_bytes - entry.bytes
        }

        entry.payload
        |> start_control(entry.deadline, state)
        |> grant_next_if_expired_control()
    end
  end

  defp reject_all(state, result) do
    if state.active do
      Process.cancel_timer(state.active.timer)
      if state.active.source_ref, do: Process.demonitor(state.active.source_ref, [:flush])
      if state.active.write, do: GenServer.reply(state.active.write, result)
    end

    state.queued
    |> :queue.to_list()
    |> Enum.each(fn
      %{kind: :payload, from: from} -> GenServer.reply(from, result)
      %{kind: :control} -> :ok
    end)

    %{state | active: nil, queued: :queue.new(), queued_control_bytes: 0}
  end

  defp start_control(payload, deadline, state) do
    case remaining(deadline) do
      0 ->
        state

      timeout ->
        reference = make_ref()
        timer = Process.send_after(self(), {:reservation_timeout, reference}, timeout)
        PaseoRelay.Metrics.inc(:frames_forwarded)
        PaseoRelay.Metrics.inc(:bytes_forwarded, byte_size(payload))
        send(state.destination, {:relay_frame, self(), reference, :text, payload})
        send(state.destination, {:relay_write_barrier, self(), reference})

        active = %{
          kind: :control,
          token: reference,
          bytes: byte_size(payload),
          source_ref: nil,
          timer: timer,
          write: nil,
          write_reference: reference
        }

        %{state | active: active}
    end
  end

  defp grant_next_if_expired_control(%{active: nil} = state), do: grant_next(state)
  defp grant_next_if_expired_control(state), do: state

  defp remaining(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end
end
