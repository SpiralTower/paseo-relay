defmodule PaseoRelay.Protocol do
  @moduledoc false

  @maximum_frame_wire_bytes 32 * 1024 * 1024
  @maximum_client_frame_header_bytes 14
  @maximum_message_payload_bytes 32 * 1024 * 1024

  def maximum_frame_wire_bytes, do: @maximum_frame_wire_bytes

  def maximum_client_frame_payload_bytes,
    do: @maximum_frame_wire_bytes - @maximum_client_frame_header_bytes

  def maximum_message_payload_bytes, do: @maximum_message_payload_bytes

  def websocket_options(additional \\ []) do
    Keyword.merge(
      [
        max_frame_size: @maximum_frame_wire_bytes,
        max_fragmented_message_size: @maximum_message_payload_bytes,
        compress: false
      ],
      additional
    )
  end
end
