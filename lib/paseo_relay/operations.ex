defmodule PaseoRelay.Operations do
  @moduledoc """
  Platform-neutral HTTP operations contract.

  `GET /health` reports liveness, `GET /ready` reports whether new relay work
  may be admitted, and `GET /metrics` exposes a small Prometheus-compatible
  surface. A drain is activated through `PaseoRelay.Drain.begin/0`; it never
  depends on a deployment provider's control plane.
  """
  @behaviour :cowboy_handler

  @impl true
  def init(request, config) do
    {status, content_type, body} = response(:cowboy_req.path(request), config)

    request =
      :cowboy_req.reply(
        status,
        %{"content-type" => content_type},
        body,
        request
      )

    {:ok, request, nil}
  end

  def response(path), do: response(path, configured_runtime())

  def response("/health", _config), do: {200, "application/json", ~s({"status":"ok"})}

  def response("/ready", config) do
    if ready?(config) do
      {200, "application/json", ~s({"status":"ready"})}
    else
      {503, "application/json", ~s({"status":"unready"})}
    end
  end

  def response("/metrics", config) do
    body =
      [
        "# HELP paseo_relay_ready Whether this node admits new relay work.",
        "# TYPE paseo_relay_ready gauge",
        "paseo_relay_ready #{if(ready?(config), do: 1, else: 0)}",
        "# HELP paseo_relay_draining Whether this node is draining.",
        "# TYPE paseo_relay_draining gauge",
        "paseo_relay_draining #{if(draining?(), do: 1, else: 0)}",
        PaseoRelay.Metrics.render()
      ]
      |> Enum.join("\n")
      |> Kernel.<>("\n")

    {200, "text/plain; version=0.0.4", body}
  end

  def response(_path, _config), do: {404, "text/plain", "not found\n"}

  defp draining?, do: PaseoRelay.Drain.draining?()

  defp ready?(config),
    do: not draining?() and PaseoRelay.Ownership.ready?(config.minimum_cluster_size)

  defp configured_runtime do
    :paseo_relay
    |> Application.get_env(:runtime, PaseoRelay.Config.defaults())
    |> PaseoRelay.Config.normalize()
  end
end
