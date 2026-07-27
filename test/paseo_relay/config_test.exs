defmodule PaseoRelay.ConfigTest do
  use ExUnit.Case, async: true

  alias PaseoRelay.Config

  test "loads generic release settings with safe local defaults" do
    assert Config.load([]) ==
             {:ok,
              %{
                host: "127.0.0.1",
                ip: {127, 0, 0, 1},
                port: 4000,
                drain: false,
                acceptors: 100,
                connections_per_acceptor: 200,
                connection_retry_count: 5,
                connection_retry_wait_ms: 1_000,
                ingress_budget_bytes: 512 * 1024 * 1024,
                ingress_weight: 4,
                delivery_timeout_ms: 30_000,
                payload_timeout_ms: 30_000,
                data_attach_timeout_ms: 15_000,
                tcp_receive_buffer_bytes: 64 * 1024,
                websocket_max_heap_words: 32 * 1024 * 1024,
                memory_watermark_bytes: 0,
                node_name: nil,
                cookie: nil
              }}
  end

  test "rejects a listener hostname that the socket layer cannot bind" do
    assert Config.load([{"PASEO_RELAY_HOST", "not-an-ip"}]) ==
             {:error, "PASEO_RELAY_HOST must be an IP address"}
  end

  test "rejects an invalid port instead of starting on an unintended listener" do
    assert Config.load([{"PASEO_RELAY_PORT", "not-a-port"}]) ==
             {:error, "PASEO_RELAY_PORT must be an integer between 1 and 65535"}
  end

  test "recognizes drain mode from the release environment" do
    assert {:ok, %{drain: true, port: 4400}} =
             Config.load([{"PASEO_RELAY_DRAIN", "true"}, {"PASEO_RELAY_PORT", "4400"}])
  end

  test "requires the websocket heap fuse to admit a maximum legal frame" do
    assert Config.load([{"PASEO_RELAY_WEBSOCKET_MAX_HEAP_WORDS", "33554431"}]) ==
             {:error,
              "PASEO_RELAY_WEBSOCKET_MAX_HEAP_WORDS must be an integer between 33554432 and 134217728"}

    assert {:ok, %{websocket_max_heap_words: 33_554_432}} =
             Config.load([{"PASEO_RELAY_WEBSOCKET_MAX_HEAP_WORDS", "33554432"}])
  end

  test "loads and validates the listener ceiling as connections per acceptor" do
    assert {:ok, %{acceptors: 20, connections_per_acceptor: 750}} =
             Config.load([
               {"PASEO_RELAY_ACCEPTORS", "20"},
               {"PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR", "750"}
             ])

    assert Config.load([{"PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR", "0"}]) ==
             {:error,
              "PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR must be an integer between 1 and 1000000"}
  end

  test "validates the weighted ingress envelope and delivery limits" do
    assert {:ok,
            %{
              ingress_budget_bytes: 256_000_000,
              ingress_weight: 2,
              delivery_timeout_ms: 5_000,
              payload_timeout_ms: 10_000,
              tcp_receive_buffer_bytes: 32_768
            }} =
             Config.load([
               {"PASEO_RELAY_INGRESS_BUDGET_BYTES", "256000000"},
               {"PASEO_RELAY_INGRESS_WEIGHT", "2"},
               {"PASEO_RELAY_DELIVERY_TIMEOUT_MS", "5000"},
               {"PASEO_RELAY_PAYLOAD_TIMEOUT_MS", "10000"},
               {"PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES", "32768"}
             ])

    assert Config.load([
             {"PASEO_RELAY_INGRESS_BUDGET_BYTES", Integer.to_string(128 * 1024 * 1024)},
             {"PASEO_RELAY_INGRESS_WEIGHT", "8"}
           ]) ==
             {:error,
              "PASEO_RELAY_INGRESS_BUDGET_BYTES must admit one maximum assembled message at the configured weight"}

    assert {:ok, %{memory_watermark_bytes: 0}} =
             Config.load([{"PASEO_RELAY_MEMORY_WATERMARK_BYTES", "0"}])

    assert {:ok, %{memory_watermark_bytes: 1_500_000_000}} =
             Config.load([{"PASEO_RELAY_MEMORY_WATERMARK_BYTES", "1500000000"}])
  end

  test "requires capacity for one maximum assembled fragmented message" do
    weight = 5
    exact_budget = PaseoRelay.Protocol.maximum_message_payload_bytes() * weight

    assert {:ok, %{ingress_budget_bytes: ^exact_budget, ingress_weight: ^weight}} =
             Config.load([
               {"PASEO_RELAY_INGRESS_BUDGET_BYTES", Integer.to_string(exact_budget)},
               {"PASEO_RELAY_INGRESS_WEIGHT", Integer.to_string(weight)}
             ])

    assert Config.load([
             {"PASEO_RELAY_INGRESS_BUDGET_BYTES", Integer.to_string(exact_budget - 1)},
             {"PASEO_RELAY_INGRESS_WEIGHT", Integer.to_string(weight)}
           ]) ==
             {:error,
              "PASEO_RELAY_INGRESS_BUDGET_BYTES must admit one maximum assembled message at the configured weight"}
  end
end
