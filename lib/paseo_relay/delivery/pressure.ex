defmodule PaseoRelay.Delivery.Pressure do
  @moduledoc false

  def check_now, do: PaseoRelay.Capacity.check_now()
end
