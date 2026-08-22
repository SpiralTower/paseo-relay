defmodule PaseoRelay.HandshakeValidationTest do
  use ExUnit.Case, async: true

  alias PaseoRelay.HandshakeValidation

  @other_unsupported_key_encodings [
    "0100000000000000000000000000000000000000000000000000000000000000",
    "E0EB7A7C3B41B8AE1656E3FAF19FC46ADA098DEB9C32B1FD866205165F49B800",
    "5F9C95BCA3508C24B1D0B1559C83EF5B04445CC4581C8E86D8224EDDD09F1157",
    "ECFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F",
    "EDFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F",
    "EEFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F"
  ]

  test "accepts valid keys for both handshake names and opcodes" do
    {public_key, _private_key} = :crypto.generate_key(:ecdh, :x25519)

    for {type, expected_type} <- [{"hello", :hello}, {"e2ee_hello", :e2ee_hello}],
        opcode <- [:text, :binary] do
      payload = handshake(type, Base.encode64(public_key))

      assert HandshakeValidation.check(opcode, payload) == {:accept, expected_type}
    end
  end

  test "rejects an unsupported public key" do
    payload = handshake("e2ee_hello", Base.encode64(<<0::256>>))

    assert HandshakeValidation.check(:text, payload) == {:reject, :e2ee_hello}
  end

  test "rejects other unsupported public keys" do
    for encoded <- @other_unsupported_key_encodings do
      public_key = Base.decode16!(encoded)
      payload = handshake("hello", Base.encode64(public_key))

      assert HandshakeValidation.check(:binary, payload) == {:reject, :hello}
    end
  end

  test "rejects malformed or incorrectly sized key encodings" do
    {public_key, _private_key} = :crypto.generate_key(:ecdh, :x25519)
    canonical = Base.encode64(public_key)

    invalid_pad_bits =
      <<9, 0::size(31 * 8)>>
      |> Base.encode64()
      |> String.replace_suffix("A=", "B=")

    invalid_keys = [
      nil,
      42,
      "not base64!",
      Base.encode64(:crypto.strong_rand_bytes(31)),
      Base.encode64(:crypto.strong_rand_bytes(33)),
      String.trim_trailing(canonical, "="),
      invalid_pad_bits
    ]

    for key <- invalid_keys do
      assert HandshakeValidation.check(:text, handshake("e2ee_hello", key)) ==
               {:reject, :e2ee_hello}
    end

    assert HandshakeValidation.check(:text, Jason.encode!(%{"type" => "e2ee_hello"})) ==
             {:reject, :e2ee_hello}
  end

  test "rejects a noncanonical key encoding" do
    invalid_basepoint = <<9, 0::size(30 * 8), 0x80>>
    payload = handshake("hello", Base.encode64(invalid_basepoint))

    assert HandshakeValidation.check(:binary, payload) == {:reject, :hello}
  end

  test "leaves non-handshake frames opaque" do
    assert HandshakeValidation.check(:binary, :binary.copy(<<0xFF>>, 64)) == :not_handshake
    assert HandshakeValidation.check(:text, ~s({"type":"ping"})) == :not_handshake
    assert HandshakeValidation.check(:text, ~s({"type":"hello")) == :not_handshake
  end

  test "renders fixed-cardinality handshake counters without route identifiers" do
    metrics = PaseoRelay.Metrics.render(:unavailable)

    assert metrics =~ "# TYPE paseo_relay_handshake_accepted_total counter"
    assert metrics =~ "# TYPE paseo_relay_handshake_rejected_total counter"

    for outcome <- [:accepted, :rejected], version <- [1, 2], type <- [:hello, :e2ee_hello] do
      assert metrics =~
               ~s(paseo_relay_handshake_#{outcome}_total{routing_version="v#{version}",type="#{type}"})
    end

    refute metrics =~ "serverId"
  end

  defp handshake(type, key) do
    Jason.encode!(%{"type" => type, "key" => key, "capabilities" => %{}})
  end
end
