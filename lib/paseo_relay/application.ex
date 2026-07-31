defmodule PaseoRelay.Application do
  use Application

  @impl true
  def start(_type, _args) do
    config =
      PaseoRelay.Config.normalize(
        Application.get_env(:paseo_relay, :runtime, PaseoRelay.Config.defaults())
      )

    children = [
      {DNSCluster, query: config.cluster_query || :ignore},
      {PaseoRelay.Drain, config.drain},
      PaseoRelay.Metrics,
      PaseoRelay.ConnectionBudget,
      {PaseoRelay.Delivery.Pressure, config.memory_watermark_bytes},
      {PaseoRelay.Delivery.Budget,
       limit: config.ingress_budget_bytes, weight: config.ingress_weight},
      {PaseoRelay.Listener, ref: PaseoRelay.Listener, config: config}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: PaseoRelay.Supervisor)
  end
end
