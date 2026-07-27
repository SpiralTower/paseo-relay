defmodule PaseoRelay.Delivery.Budget do
  @moduledoc false

  use GenServer

  @default_limit 512 * 1024 * 1024
  @default_weight 4

  def start_link(options), do: GenServer.start_link(__MODULE__, options, name: __MODULE__)

  def reserve(payload_bytes) do
    GenServer.call(__MODULE__, {:reserve, self(), payload_bytes}, :infinity)
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
       monitors: %{},
       queued: :queue.new()
     }}
  end

  @impl true
  def handle_call({:reserve, pid, payload_bytes}, _from, state) do
    weighted_bytes = payload_bytes * state.weight

    cond do
      weighted_bytes > state.limit ->
        {:reply, {:error, :frame_exceeds_budget}, state}

      :queue.is_empty(state.queued) and state.reserved + weighted_bytes <= state.limit ->
        reference = make_ref()
        {:reply, {:ok, reference}, grant(state, pid, weighted_bytes)}

      true ->
        reference = make_ref()
        entry = %{pid: pid, bytes: weighted_bytes, reference: reference}

        {:reply, {:suspend, reference},
         %{ensure_monitor(state, pid) | queued: :queue.in(entry, state.queued)}}
    end
  end

  def handle_call(:reserved_bytes, _from, state), do: {:reply, state.reserved, state}

  @impl true
  def handle_cast({:release, pid, payload_bytes}, state) do
    {:noreply, state |> release(pid, payload_bytes * state.weight) |> admit_waiters()}
  end

  def handle_cast({:release_all, pid}, state) do
    {:noreply, state |> release(pid, :all) |> admit_waiters()}
  end

  @impl true
  def handle_info({:DOWN, reference, :process, pid, _reason}, state) do
    if state.monitors[pid] == reference do
      queued =
        state.queued
        |> :queue.to_list()
        |> Enum.reject(&(&1.pid == pid))
        |> :queue.from_list()

      {:noreply,
       state
       |> Map.put(:queued, queued)
       |> release(pid, :all)
       |> admit_waiters()}
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

  defp admit_waiters(state) do
    case :queue.out(state.queued) do
      {{:value, entry}, queued} when state.reserved + entry.bytes <= state.limit ->
        state = %{state | queued: queued} |> grant(entry.pid, entry.bytes)
        send(entry.pid, {:bandit_payload_admitted, entry.reference})
        admit_waiters(state)

      _ ->
        state
    end
  end

  defp ensure_monitor(state, pid) do
    monitors = Map.put_new_lazy(state.monitors, pid, fn -> Process.monitor(pid) end)
    %{state | monitors: monitors}
  end
end
