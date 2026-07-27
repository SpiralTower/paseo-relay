defmodule PaseoRelay.Delivery.Pressure do
  @moduledoc false

  use GenServer

  @check_interval 1_000
  def start_link(_options), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  def enter(socket), do: GenServer.cast(__MODULE__, {:enter, socket})
  def leave(socket), do: GenServer.cast(__MODULE__, {:leave, socket})
  def check_now, do: GenServer.call(__MODULE__, :check)

  @impl true
  def init(:ok) do
    schedule_check()
    {:ok, %{oldest: :gb_trees.empty(), monitors: %{}, sequence: 0}}
  end

  @impl true
  def handle_call(:check, _from, state) do
    state = shed_if_needed(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:enter, socket}, state) do
    if Map.has_key?(state.monitors, socket) do
      {:noreply, state}
    else
      reference = Process.monitor(socket)
      sequence = state.sequence + 1
      key = {System.monotonic_time(), sequence}

      {:noreply,
       %{
         state
         | oldest: :gb_trees.insert(key, socket, state.oldest),
           monitors: Map.put(state.monitors, socket, %{reference: reference, key: key}),
           sequence: sequence
       }}
    end
  end

  def handle_cast({:leave, socket}, state), do: {:noreply, remove(state, socket)}

  @impl true
  def handle_info(:check, state) do
    schedule_check()
    {:noreply, shed_if_needed(state)}
  end

  def handle_info({:DOWN, reference, :process, socket, _reason}, state) do
    if get_in(state.monitors, [socket, :reference]) == reference,
      do: {:noreply, remove(state, socket)},
      else: {:noreply, state}
  end

  defp shed_if_needed(state) do
    watermark = watermark()

    if watermark > 0 and :erlang.memory(:total) >= watermark do
      case next_live(state) do
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

  defp next_live(state) do
    if :gb_trees.is_empty(state.oldest) do
      :empty
    else
      {_key, socket} = :gb_trees.smallest(state.oldest)
      {:ok, socket, remove(state, socket)}
    end
  end

  defp remove(state, socket) do
    case state.monitors[socket] do
      nil ->
        state

      %{reference: reference, key: key} ->
        Process.demonitor(reference, [:flush])

        %{
          state
          | oldest: :gb_trees.delete_any(key, state.oldest),
            monitors: Map.delete(state.monitors, socket)
        }
    end
  end

  defp watermark do
    :paseo_relay
    |> Application.get_env(:operations, [])
    |> Keyword.get(:memory_watermark_bytes, 0)
  end

  defp schedule_check, do: Process.send_after(self(), :check, @check_interval)
end
