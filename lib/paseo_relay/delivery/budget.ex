defmodule PaseoRelay.Delivery.Budget do
  @moduledoc false

  use GenServer

  @default_limit 512 * 1024 * 1024
  @default_weight 4

  def start_link(options), do: GenServer.start_link(__MODULE__, options, name: __MODULE__)

  def attach do
    GenServer.call(__MODULE__, :attach)
  catch
    :exit, _reason -> {:error, :budget_unavailable}
  end

  def admit(payload_bytes) do
    GenServer.call(__MODULE__, {:admit, self(), payload_bytes})
  catch
    :exit, _reason -> {:error, :budget_unavailable}
  end

  def release(payload_bytes), do: GenServer.cast(__MODULE__, {:release, self(), payload_bytes})
  def release_all, do: GenServer.cast(__MODULE__, {:release_all, self()})
  def reserved_bytes, do: GenServer.call(__MODULE__, :reserved_bytes)

  @impl true
  def init(options) do
    {:ok,
     %{
       limit: Keyword.get(options, :limit, @default_limit),
       weight: Keyword.get(options, :weight, @default_weight),
       reserved: 0,
       holders: %{},
       monitors: %{}
     }}
  end

  @impl true
  def handle_call(:attach, _from, state), do: {:reply, {:ok, self()}, state}

  def handle_call({:admit, pid, payload_bytes}, _from, state) do
    weighted_bytes = payload_bytes * state.weight

    cond do
      weighted_bytes > state.limit ->
        {:reply, {:error, :message_exceeds_budget}, state}

      state.reserved + weighted_bytes <= state.limit ->
        {:reply, :ok, grant(state, pid, weighted_bytes)}

      true ->
        {:reply, {:error, :budget_exhausted}, state}
    end
  end

  def handle_call(:reserved_bytes, _from, state), do: {:reply, state.reserved, state}

  @impl true
  def handle_cast({:release, pid, payload_bytes}, state) do
    {:noreply, release(state, pid, payload_bytes * state.weight)}
  end

  def handle_cast({:release_all, pid}, state) do
    {:noreply, release(state, pid, :all)}
  end

  @impl true
  def handle_info({:DOWN, reference, :process, pid, _reason}, state) do
    if state.monitors[pid] == reference do
      {:noreply, release(state, pid, :all)}
    else
      {:noreply, state}
    end
  end

  defp grant(state, pid, bytes) do
    state = ensure_monitor(state, pid)
    holders = Map.update(state.holders, pid, bytes, &(&1 + bytes))
    PaseoRelay.Metrics.inc(:ingress_reserved_bytes, bytes)
    %{state | reserved: state.reserved + bytes, holders: holders}
  end

  defp release(state, pid, amount) do
    held = Map.get(state.holders, pid, 0)
    released = if amount == :all, do: held, else: min(held, amount)
    remaining = held - released

    {holders, monitors} =
      if remaining == 0 do
        if reference = state.monitors[pid], do: Process.demonitor(reference, [:flush])
        {Map.delete(state.holders, pid), Map.delete(state.monitors, pid)}
      else
        {Map.put(state.holders, pid, remaining), state.monitors}
      end

    PaseoRelay.Metrics.dec(:ingress_reserved_bytes, released)
    %{state | reserved: state.reserved - released, holders: holders, monitors: monitors}
  end

  defp ensure_monitor(state, pid) do
    monitors = Map.put_new_lazy(state.monitors, pid, fn -> Process.monitor(pid) end)
    %{state | monitors: monitors}
  end
end
