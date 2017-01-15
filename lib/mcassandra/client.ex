defmodule Mcassandra.Client do

  @doc """
  Get a single key from a table
  """
  def get(table_name, key) when is_atom(key) do
    IO.puts "Getting key: #{key} \n from table: #{table_name}"
    :ok
  end

  @doc """
  Get a range of keys from a table
  """
  def get(table_name, keys_range) do
    IO.puts "Getting keys: #{Enum.at keys_range, 0}..#{Enum.at keys_range, -1}
     from table: #{table_name}"
    :ok
  end
end