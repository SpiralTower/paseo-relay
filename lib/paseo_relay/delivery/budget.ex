defmodule PaseoRelay.Delivery.Budget do
  @moduledoc false

  def reserved_bytes, do: PaseoRelay.Capacity.value(:ingress_reserved_bytes)
end
