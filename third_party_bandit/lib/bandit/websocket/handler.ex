defmodule Bandit.WebSocket.Handler do
  @moduledoc false
  # A WebSocket handler conforming to RFC6455, structured as a ThousandIsland.Handler

  use ThousandIsland.Handler

  alias Bandit.Extractor
  alias Bandit.WebSocket.{Connection, Frame}

  @impl ThousandIsland.Handler
  def handle_connection(socket, state) do
    {websock, websock_opts, connection_opts} = state.upgrade_opts

    connection_opts
    |> Keyword.take([:fullsweep_after, :max_heap_size])
    |> Enum.each(fn {key, value} -> :erlang.process_flag(key, value) end)

    connection_opts = Keyword.merge(state.opts.websocket, connection_opts)

    primitive_ops_module =
      Keyword.get(state.opts.websocket, :primitive_ops_module, Bandit.PrimitiveOps.WebSocket)

    state =
      state
      |> Map.take([:handler_module])
      |> Map.put(:extractor, Extractor.new(Frame, primitive_ops_module, connection_opts))

    case Connection.init(websock, websock_opts, connection_opts, socket) do
      {:continue, connection} ->
        case Keyword.get(connection_opts, :timeout) do
          nil -> {:continue, Map.put(state, :connection, connection)}
          timeout -> {:continue, Map.put(state, :connection, connection), {:persistent, timeout}}
        end

      {:error, reason, connection} ->
        {:error, reason, Map.put(state, :connection, connection)}
    end
  end

  @impl ThousandIsland.Handler
  def handle_data(data, socket, state) do
    state.extractor
    |> Extractor.push_data(data)
    |> pop_frame(socket, state)
  end

  defp pop_frame(extractor, socket, state) do
    {extractor, result} = Extractor.pop_frame(extractor)
    {completed_admission, extractor} = Extractor.take_completed_payload_admission(extractor)

    state =
      ensure_payload_timeout(
        Extractor.payload_admission(extractor) || completed_admission,
        state
      )

    case result do
      {:ok, frame} ->
        case Connection.handle_frame(frame, socket, state.connection) do
          {:continue, connection} ->
            state = finish_payload_timeout(%{state | connection: connection})
            pop_frame(extractor, socket, %{state | extractor: extractor})

          {:suspend, connection} ->
            {:suspend,
             state
             |> Map.put(:extractor, extractor)
             |> Map.put(:connection, connection)
             |> finish_payload_timeout()
             |> Map.put(:input_suspended, true)}

          {:close, connection} ->
            {:close,
             state
             |> Map.put(:extractor, extractor)
             |> Map.put(:connection, connection)
             |> cancel_payload_timeout()}

          {:error, reason, connection} ->
            {:error, reason,
             state
             |> Map.put(:extractor, extractor)
             |> Map.put(:connection, connection)
             |> cancel_payload_timeout()}
        end

      {:error, reason} ->
        error = {:deserializing, reason}
        {:error, _reason, connection} = Connection.handle_error(error, socket, state.connection)
        Bandit.Logger.maybe_log_websocket_protocol_error(error, connection)

        {:error, {:shutdown, error},
         state
         |> Map.put(:extractor, extractor)
         |> Map.put(:connection, connection)
         |> cancel_payload_timeout()}

      {:suspend, reference} ->
        {:suspend,
         state
         |> Map.put(:extractor, extractor)
         |> Map.put(:payload_admission, reference)
         |> Map.put(:input_suspended, true)}

      :more ->
        {:continue, %{state | extractor: extractor}}
    end
  end

  @impl ThousandIsland.Handler
  def handle_close(socket, %{connection: connection}),
    do: Connection.handle_close(socket, connection)

  def handle_close(_socket, _state), do: :ok

  @impl ThousandIsland.Handler
  def handle_shutdown(socket, state), do: Connection.handle_shutdown(socket, state.connection)

  @impl ThousandIsland.Handler
  def handle_error(reason, socket, state),
    do: Connection.handle_error(reason, socket, state.connection)

  @impl ThousandIsland.Handler
  def handle_timeout(socket, state), do: Connection.handle_timeout(socket, state.connection)

  def handle_info({:plug_conn, :sent}, {socket, state}), do: {:noreply, {socket, state}}

  def handle_info(
        {:bandit_payload_admitted, reference},
        {socket, %{payload_admission: reference} = state}
      ) do
    extractor = Extractor.resume_payload(state.extractor, reference)

    state =
      state
      |> Map.put(:extractor, extractor)
      |> Map.delete(:payload_admission)
      |> Map.delete(:input_suspended)

    extractor
    |> pop_frame(socket, state)
    |> ThousandIsland.Handler.handle_continuation(socket)
  end

  def handle_info(
        {:bandit_payload_timeout, reference},
        {socket, %{payload_timeout: %{admission: reference}} = state}
      ) do
    case Connection.handle_payload_timeout(socket, state.connection) do
      {:error, _reason, connection} ->
        {:stop, :normal,
         {socket, state |> Map.put(:connection, connection) |> Map.delete(:payload_timeout)}}
    end
  end

  def handle_info(msg, {socket, state}) do
    case Connection.handle_info(msg, socket, state.connection) do
      {:continue, connection_state} ->
        state = %{state | connection: connection_state}

        if state[:input_suspended] && connection_state.state == :closing do
          _ = ThousandIsland.Socket.setopts(socket, active: :once)
          {:noreply, {socket, Map.delete(state, :input_suspended)}}
        else
          {:noreply, {socket, state}}
        end

      {:resume, connection_state} ->
        state = state |> Map.put(:connection, connection_state) |> Map.delete(:input_suspended)

        state.extractor
        |> pop_frame(socket, state)
        |> ThousandIsland.Handler.handle_continuation(socket)

      {:error, reason, connection_state} ->
        {:stop, reason, {socket, %{state | connection: connection_state}}}
    end
  end

  defp ensure_payload_timeout(nil, state), do: state
  defp ensure_payload_timeout(_reference, %{payload_timeout: _timeout} = state), do: state

  defp ensure_payload_timeout(reference, state) do
    timeout_ms = Keyword.get(state.connection.opts, :payload_timeout_ms, 30_000)
    timer = Process.send_after(self(), {:bandit_payload_timeout, reference}, timeout_ms)
    Map.put(state, :payload_timeout, %{admission: reference, timer: timer})
  end

  defp finish_payload_timeout(%{connection: %{state: :open, fragment_frame: fragment}} = state)
       when not is_nil(fragment),
       do: state

  defp finish_payload_timeout(state), do: cancel_payload_timeout(state)

  defp cancel_payload_timeout(%{payload_timeout: timeout} = state) do
    cancel_payload_timeout_timer(timeout)
    Map.delete(state, :payload_timeout)
  end

  defp cancel_payload_timeout(state), do: state

  defp cancel_payload_timeout_timer(%{admission: reference, timer: timer}) do
    Process.cancel_timer(timer)

    receive do
      {:bandit_payload_timeout, ^reference} -> :ok
    after
      0 -> :ok
    end
  end
end
