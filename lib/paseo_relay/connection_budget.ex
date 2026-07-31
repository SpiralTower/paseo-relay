defmodule PaseoRelay.ConnectionBudget do
  @moduledoc false

  use GenServer

  @reservation_timeout_ms 5_000

  def start_link(_options), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  def admit(namespace, limit) do
    GenServer.call(__MODULE__, {:admit, namespace, limit})
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  def attach(token) do
    GenServer.call(__MODULE__, {:attach, token, self()})
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  def release(token) do
    GenServer.call(__MODULE__, {:release, token})
  catch
    :exit, _reason -> :ok
  end

  def active(namespace), do: GenServer.call(__MODULE__, {:active, namespace})

  def active_websockets do
    GenServer.call(__MODULE__, :active_websockets)
  catch
    :exit, _reason -> 0
  end

  @impl true
  def init(:ok), do: {:ok, %{namespaces: %{}, tokens: %{}, monitors: %{}}}

  @impl true
  def handle_call({:admit, namespace, limit}, _from, state) do
    namespace_state = Map.get(state.namespaces, namespace, %{limit: limit, active: 0})

    cond do
      namespace_state.limit != limit ->
        {:reply, {:error, :configuration_mismatch}, state}

      namespace_state.active >= limit ->
        {:reply, {:error, :capacity}, state}

      true ->
        token = make_ref()
        timer = Process.send_after(self(), {:expire, token}, @reservation_timeout_ms)
        namespace_state = %{namespace_state | active: namespace_state.active + 1}

        {:reply, {:ok, token},
         %{
           state
           | namespaces: Map.put(state.namespaces, namespace, namespace_state),
             tokens: Map.put(state.tokens, token, {namespace, {:reservation, timer}})
         }}
    end
  end

  def handle_call({:attach, token, socket}, _from, state) do
    case state.tokens[token] do
      {namespace, {:reservation, timer}} ->
        Process.cancel_timer(timer)
        monitor = Process.monitor(socket)

        {:reply, {:ok, self()},
         %{
           state
           | tokens: Map.put(state.tokens, token, {namespace, {:active, monitor}}),
             monitors: Map.put(state.monitors, monitor, token)
         }}

      _missing_or_attached ->
        {:reply, {:error, :expired}, state}
    end
  end

  def handle_call({:release, token}, _from, state) do
    {:reply, :ok, release(state, token, true)}
  end

  def handle_call({:active, namespace}, _from, state) do
    active = state.namespaces |> Map.get(namespace, %{active: 0}) |> Map.fetch!(:active)
    {:reply, active, state}
  end

  def handle_call(:active_websockets, _from, state) do
    active =
      Enum.count(state.tokens, fn {_token, {_namespace, status}} ->
        match?({:active, _}, status)
      end)

    {:reply, active, state}
  end

  @impl true
  def handle_info({:expire, token}, state), do: {:noreply, release(state, token, false)}

  def handle_info({:DOWN, monitor, :process, _socket, _reason}, state) do
    case state.monitors[monitor] do
      nil -> {:noreply, state}
      token -> {:noreply, release(state, token, false)}
    end
  end

  defp release(state, token, demonitor?) do
    case Map.pop(state.tokens, token) do
      {nil, _tokens} ->
        state

      {{namespace, status}, tokens} ->
        cleanup_status(status, demonitor?)
        namespace_state = Map.fetch!(state.namespaces, namespace)
        active = namespace_state.active - 1

        namespaces =
          if active == 0 do
            Map.delete(state.namespaces, namespace)
          else
            Map.put(state.namespaces, namespace, %{namespace_state | active: active})
          end

        %{
          state
          | namespaces: namespaces,
            tokens: tokens,
            monitors: drop_monitor(state.monitors, status)
        }
    end
  end

  defp cleanup_status({:reservation, timer}, _demonitor?), do: Process.cancel_timer(timer)

  defp cleanup_status({:active, monitor}, demonitor?) do
    if demonitor?, do: Process.demonitor(monitor, [:flush])
  end

  defp drop_monitor(monitors, {:reservation, _timer}), do: monitors
  defp drop_monitor(monitors, {:active, monitor}), do: Map.delete(monitors, monitor)
end
