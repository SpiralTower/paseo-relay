defmodule PaseoRelay.Delivery.Writer do
  @moduledoc false

  use GenServer

  @type token :: reference()

  def start_link(destination), do: GenServer.start_link(__MODULE__, destination)

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

  @impl true
  def init(destination) do
    Process.flag(:message_queue_data, :off_heap)

    {:ok,
     %{
       destination: destination,
       destination_ref: Process.monitor(destination),
       active: nil,
       queued: :queue.new()
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
    entry = %{from: from, bytes: byte_count, deadline: deadline}
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
    Process.demonitor(active.source_ref, [:flush])
    if active.write, do: GenServer.reply(active.write, result)
    state |> Map.put(:active, nil) |> grant_next()
  end

  defp grant_next(state) do
    case :queue.out(state.queued) do
      {:empty, _queue} ->
        state

      {{:value, entry}, queued} ->
        state = %{state | queued: queued}

        case remaining(entry.deadline) do
          0 ->
            GenServer.reply(entry.from, {:error, :timeout})
            grant_next(state)

          timeout ->
            {:noreply, state} = grant(entry.from, entry.bytes, timeout, state)
            state
        end
    end
  end

  defp reject_all(state, result) do
    if state.active do
      Process.cancel_timer(state.active.timer)
      Process.demonitor(state.active.source_ref, [:flush])
      if state.active.write, do: GenServer.reply(state.active.write, result)
    end

    Enum.each(:queue.to_list(state.queued), &GenServer.reply(&1.from, result))
    %{state | active: nil, queued: :queue.new()}
  end

  defp remaining(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end
end
