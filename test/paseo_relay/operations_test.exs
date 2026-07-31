defmodule PaseoRelay.OperationsTest do
  use ExUnit.Case, async: false

  alias PaseoRelay.Operations

  setup do
    PaseoRelay.Drain.cancel()
    on_exit(&PaseoRelay.Drain.cancel/0)
    :ok
  end

  test "health is live while readiness refuses new work during a drain" do
    PaseoRelay.Drain.begin()
    live = Operations.response("/health")
    draining = Operations.response("/ready")

    assert {elem(live, 0), elem(live, 2)} == {200, ~s({"status":"ok"})}
    assert {elem(draining, 0), elem(draining, 2)} == {503, ~s({"status":"unready"})}
  end

  test "metrics expose a stable Prometheus surface before relay wiring exists" do
    {status, _content_type, metrics} = Operations.response("/metrics")

    assert status == 200
    assert metrics =~ "# TYPE paseo_relay_ready gauge"
    assert metrics =~ "# TYPE paseo_relay_draining gauge"
    assert metrics =~ "# TYPE paseo_relay_active_websockets gauge"
    assert metrics =~ "# TYPE paseo_relay_active_sessions gauge"
    assert metrics =~ "# TYPE paseo_relay_reroute_responses_total counter"
    assert metrics =~ "# TYPE paseo_relay_frames_forwarded_total counter"
    assert metrics =~ "# TYPE paseo_relay_bytes_forwarded_total counter"
    assert metrics =~ "# TYPE paseo_relay_ingress_reserved_bytes gauge"
    assert metrics =~ "# TYPE paseo_relay_inflight_delivery_bytes gauge"
    assert metrics =~ "# TYPE paseo_relay_backpressured_sources gauge"
    assert metrics =~ "# TYPE paseo_relay_delivery_wait_seconds histogram"
    assert metrics =~ "# TYPE paseo_relay_frame_size_bytes histogram"
    assert metrics =~ "# TYPE paseo_relay_beam_binary_memory_bytes gauge"
    assert metrics =~ "paseo_relay_ready 1"
    assert metrics =~ "paseo_relay_draining 0"
  end

  test "readiness and its metric stay false until the configured cluster floor is present" do
    visible_cluster_size =
      length(:syn.subcluster_nodes(:registry, :paseo_relay_owners)) + 1

    config = %{
      PaseoRelay.Config.defaults()
      | minimum_cluster_size: visible_cluster_size + 1
    }

    readiness = Operations.response("/ready", config)
    {_status, _content_type, metrics} = Operations.response("/metrics", config)

    assert {elem(readiness, 0), elem(readiness, 2)} == {503, ~s({"status":"unready"})}
    assert metrics =~ "paseo_relay_ready 0"
  end

  test "metrics recovers from an abrupt process failure without taking down the relay" do
    supervisor = Process.whereis(PaseoRelay.Supervisor)
    metrics = Process.whereis(PaseoRelay.Metrics)
    metrics_down = Process.monitor(metrics)
    PaseoRelay.Metrics.inc(:reroute_responses)
    reroutes_before_failure = PaseoRelay.Metrics.value(:reroute_responses)

    Process.exit(metrics, :kill)

    assert_receive {:DOWN, ^metrics_down, :process, ^metrics, :killed}
    replacement = await_metrics_replacement(metrics)
    response = Operations.response("/metrics")

    assert Process.alive?(supervisor)
    assert Process.alive?(replacement)
    assert elem(response, 0) == 200
    assert PaseoRelay.Metrics.value(:reroute_responses) == reroutes_before_failure
  end

  defp await_metrics_replacement(previous) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    await_metrics_replacement(previous, deadline)
  end

  defp await_metrics_replacement(previous, deadline) do
    case Process.whereis(PaseoRelay.Metrics) do
      replacement when is_pid(replacement) and replacement != previous ->
        replacement

      _missing ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("metrics did not restart")
        end

        Process.sleep(10)
        await_metrics_replacement(previous, deadline)
    end
  end
end
