defmodule Mcassandra.Node do

  @doc """
  Starts a new Node Agent
  """
  def start_link do
    Agent.start_link(fn -> {%{}, %{}} end, name: __MODULE__)
  end

  @doc """
  Will create Maps for each of the columns in the schema
  :table_name -> :primary_key -> [:column1, :column2] -> :ok|:error
  Structure of a cable
  {:table_name => %{:column => %{:id => column_value}}}
  """
  def create_table(pid, table_name, _primary_key, columns) do
    Agent.get_and_update(pid, fn {format, data} ->
      new_format = Map.put_new(format, table_name, columns)
      data_cells = Enum.reduce(columns, %{}, fn column, columns ->
                    Map.put_new(columns, column, %{})
                  end)
      new_data = Map.put_new(data, table_name, data_cells)
      {{:ok, table_name}, {new_format, new_data}}
    end)
  end

  @doc """
  Will insert each column into respective Agents as {primany_key, column_values}
  """
  def insert(pid, table_name, primary_key, column_values) do
    Agent.get_and_update(pid, fn {format, data} ->
      columns = Map.get(format, table_name) # [:name, :email]
      table = Map.get(data, table_name)
      updated_table =
        Enum.zip(columns, column_values) # ["me", "me@email.com"]
        |> Map.new
        |> Enum.reduce(table, fn {column, value}, table ->
          updated_column = Map.get(table, column)
            |> Map.put(primary_key, value)
          Map.put(table, column, updated_column)
        end)
      state = Map.put(data, table_name, updated_table)
      {{:ok, primary_key}, {format, state}}
    end)
  end

  @doc """
  Get a single column value from a table
  """
  def get_column(pid, table_name, primary_key, column) do
    Agent.get(pid, fn {_format, data} ->
      value = Map.get(data, table_name)
        |> Map.get(column)
        |> Map.get(primary_key)
      {:ok, value}
    end)
  end


  @doc """
  Get a range of column values by primary_key from a table
  """
  def get_column_range(pid, table_name, primary_key_range, column_name) do
    Agent.get(pid, fn {_format, data} ->
        column = Map.get(data, table_name)
          |> Map.get(column_name)
        values = Enum.map(primary_key_range, fn primary_key -> Map.get(column, primary_key)
      end)
      {:ok, values}
    end)
  end

end