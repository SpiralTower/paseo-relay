defmodule PaseoRelay.HandshakeValidation do
  @moduledoc false

  import Bitwise, only: [<<<: 2]

  @field_prime (1 <<< 255) - 19
  @handshake_types %{"hello" => :hello, "e2ee_hello" => :e2ee_hello}

  # Unsupported X25519 public-key encodings. The separate field check rejects
  # invalid coordinates, including high-bit forms.
  @unsupported_public_key_hex [
    "0000000000000000000000000000000000000000000000000000000000000000",
    "0100000000000000000000000000000000000000000000000000000000000000",
    "E0EB7A7C3B41B8AE1656E3FAF19FC46ADA098DEB9C32B1FD866205165F49B800",
    "5F9C95BCA3508C24B1D0B1559C83EF5B04445CC4581C8E86D8224EDDD09F1157",
    "ECFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F",
    "EDFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F",
    "EEFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F"
  ]
  @unsupported_public_keys Enum.map(
                             @unsupported_public_key_hex,
                             &Base.decode16!(&1, case: :mixed)
                           )

  @type handshake_type :: :hello | :e2ee_hello
  @type result :: :not_handshake | {:accept, handshake_type()} | {:reject, handshake_type()}

  @spec check(:text | :binary, binary()) :: result()
  def check(opcode, payload) when opcode in [:text, :binary] and is_binary(payload) do
    with {:ok, %{"type" => type} = message} <- Jason.decode(payload),
         {:ok, handshake_type} <- Map.fetch(@handshake_types, type) do
      if valid_public_key?(message["key"]) do
        {:accept, handshake_type}
      else
        {:reject, handshake_type}
      end
    else
      _not_handshake -> :not_handshake
    end
  end

  defp valid_public_key?(encoded) when is_binary(encoded) do
    with {:ok, public_key} <- Base.decode64(encoded, padding: true),
         32 <- byte_size(public_key),
         ^encoded <- Base.encode64(public_key),
         true <- canonical_coordinate?(public_key),
         false <- public_key in @unsupported_public_keys do
      true
    else
      _invalid -> false
    end
  end

  defp valid_public_key?(_invalid), do: false

  defp canonical_coordinate?(<<coordinate::little-unsigned-integer-size(256)>>),
    do: coordinate < @field_prime
end
