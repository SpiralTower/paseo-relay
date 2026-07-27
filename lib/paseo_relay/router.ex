defmodule PaseoRelay.Router do
  use Plug.Router

  plug(:fetch_query_params)
  plug(:match)
  plug(:dispatch)

  get "/ws" do
    with :ok <- require_websocket_upgrade(conn),
         {:ok, connection} <- PaseoRelay.Connection.from_query(conn.query_params),
         decision <- PaseoRelay.Ownership.route(connection.server_id, target()),
         {:local, owner, reservation} <- decision do
      options =
        PaseoRelay.Protocol.websocket_options(
          timeout: :infinity,
          before_payload: &PaseoRelay.Delivery.Budget.reserve/1,
          payload_timeout_ms: operation(:payload_timeout_ms, 30_000),
          max_heap_size: %{
            size: operation(:websocket_max_heap_words, 32 * 1024 * 1024),
            include_shared_binaries: true,
            kill: true
          }
        )

      conn
      |> WebSockAdapter.upgrade(
        PaseoRelay.Socket,
        %{connection: connection, owner: owner, reservation: reservation},
        options
      )
      |> halt()
    else
      {:reroute, _target} = decision ->
        PaseoRelay.Metrics.inc(:reroute_responses)
        PaseoRelay.Reroute.response(conn, decision)

      {:unavailable, reason} ->
        send_resp(conn, 503, Atom.to_string(reason))

      {:upgrade_required, message} ->
        send_resp(conn, 426, message)

      {:error, message} ->
        send_resp(conn, 400, message)
    end
  end

  match _ do
    PaseoRelay.Operations.call(conn, [])
  end

  defp target, do: Application.fetch_env!(:paseo_relay, :ownership_target)

  defp operation(key, default) do
    :paseo_relay
    |> Application.get_env(:operations, [])
    |> Keyword.get(key, default)
  end

  defp require_websocket_upgrade(conn) do
    case WebSockAdapter.UpgradeValidation.validate_upgrade(conn) do
      :ok -> :ok
      {:error, _reason} -> {:upgrade_required, "Expected WebSocket upgrade"}
    end
  end
end
