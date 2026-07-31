defmodule PaseoRelay.Delivery.Pressure do
  @moduledoc false

  use GenServer

  @check_interval 1_000

  def start_link(watermark), do: GenServer.start_link(__MODULE__, watermark, name: __MODULE__)

  def attach do
    GenServer.call(__MODULE__, {:attach, self()})
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  def block(socket), do: GenServer.cast(__MODULE__, {:block, socket})
  def unblock(socket), do: GenServer.cast(__MODULE__, {:unblock, socket})
  def leave(socket), do: GenServer.cast(__MODULE__, {:leave, socket})
  def check_now, do: GenServer.call(__MODULE__, :check)

  @impl true
  def init(watermark) do
    schedule_check()

    {:ok,
     %{
       active: :gb_trees.empty(),
       blocked: :gb_trees.empty(),
       sockets: %{},
       sequence: 0,
       watermark: watermark
     }}
  end

  @impl true
  def handle_call({:attach, socket}, _from, state) do
    if Map.has_key?(state.sockets, socket) do
      {:reply, {:ok, self()}, state}
    else
      {key, state} = next_key(state)
      reference = Process.monitor(socket)
      socket_state = %{reference: reference, active_key: key, blocked_key: nil}

      {:reply, {:ok, self()},
       %{
         state
         | active: :gb_trees.insert(key, socket, state.active),
           sockets: Map.put(state.sockets, socket, socket_state)
       }}
    end
  end

  def handle_call(:check, _from, state) do
    state = shed_if_needed(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:block, socket}, state) do
    case state.sockets[socket] do
      %{blocked_key: nil} = socket_state ->
        {key, state} = next_key(state)
        socket_state = %{socket_state | blocked_key: key}

        {:noreply,
         %{
           state
           | blocked: :gb_trees.insert(key, socket, state.blocked),
             sockets: Map.put(state.sockets, socket, socket_state)
         }}

      _missing_or_blocked ->
        {:noreply, state}
    end
  end

  def handle_cast({:unblock, socket}, state) do
    case state.sockets[socket] do
      %{blocked_key: key} = socket_state when not is_nil(key) ->
        {:noreply,
         %{
           state
           | blocked: :gb_trees.delete_any(key, state.blocked),
             sockets: Map.put(state.sockets, socket, %{socket_state | blocked_key: nil})
         }}

      _missing_or_unblocked ->
        {:noreply, state}
    end
  end

  def handle_cast({:leave, socket}, state), do: {:noreply, remove(state, socket)}

  @impl true
  def handle_info(:check, state) do
    schedule_check()
    {:noreply, shed_if_needed(state)}
  end

  def handle_info({:DOWN, reference, :process, socket, _reason}, state) do
    if get_in(state.sockets, [socket, :reference]) == reference,
      do: {:noreply, remove(state, socket)},
      else: {:noreply, state}
  end

  defp shed_if_needed(state) do
    if state.watermark > 0 and :erlang.memory(:total) >= state.watermark do
      case next_candidate(state) do
        {:ok, socket, state} ->
          send(socket, :relay_memory_pressure)
          PaseoRelay.Metrics.inc(:memory_pressure_disconnects)
          state

        :empty ->
          state
      end
    else
      state
    end
  end

  defp next_candidate(state) do
    cond do
      not :gb_trees.is_empty(state.blocked) -> pop_oldest(state.blocked, state)
      not :gb_trees.is_empty(state.active) -> pop_oldest(state.active, state)
      true -> :empty
    end
  end

  defp pop_oldest(tree, state) do
    {_key, socket} = :gb_trees.smallest(tree)
    {:ok, socket, remove(state, socket)}
  end

  defp remove(state, socket) do
    case Map.pop(state.sockets, socket) do
      {nil, _sockets} ->
        state

      {%{reference: reference, active_key: active_key, blocked_key: blocked_key}, sockets} ->
        Process.demonitor(reference, [:flush])

        %{
          state
          | active: :gb_trees.delete_any(active_key, state.active),
            blocked: delete_if_present(state.blocked, blocked_key),
            sockets: sockets
        }
    end
  end

  defp delete_if_present(tree, nil), do: tree
  defp delete_if_present(tree, key), do: :gb_trees.delete_any(key, tree)

  defp next_key(state) do
    sequence = state.sequence + 1
    {{System.monotonic_time(), sequence}, %{state | sequence: sequence}}
  end

  defp schedule_check, do: Process.send_after(self(), :check, @check_interval)
end
