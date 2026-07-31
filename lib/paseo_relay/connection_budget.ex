defmodule PaseoRelay.ConnectionBudget do
  @moduledoc false

  def admit(namespace, limit), do: PaseoRelay.Capacity.admit_connection(namespace, limit)
  def attach(token), do: PaseoRelay.Capacity.attach_connection(token)
  def release(token), do: PaseoRelay.Capacity.release_connection(token)
  def active(namespace), do: PaseoRelay.Capacity.active_connections(namespace)
  def active_websockets, do: PaseoRelay.Capacity.value(:active_websockets)
end
