defmodule Mcassandra.Server do
  use GenServer

  # Client API

  def start_link() do
    GenServer.start_link(__MODULE__, {%{}, %{}})
  end


  @doc """
  Will create Agents for each of the columns in the schema
  :table_name -> :primary_key -> [:column1, :column2] -> :ok|:error
  Structure of a cable
  {:table_name => %{:column => %{:id => column_value}}}


  """
  def create_table(pid, table_name, primary_key, columns) do
    GenServer.call(pid, {:new_table, table_name, primary_key, columns})
  end

  @doc """
  Will insert each column into respective Agents as {primany_key, column_values}
  """
  def insert(pid, table_name, primary_key, columns) do
    GenServer.call(pid, {:insert, table_name, primary_key, columns})
  end

  @doc """
  Get a single column value from a table
  """
  def get_column(pid, table_name, primary_key, column) do
    GenServer.call(pid, {:get_column, table_name, primary_key, column})
  end

  @doc """
  Get a range of column values by primary_key from a table
  """
  def get_column_range(pid, table_name, primary_key_range, column) do
    GenServer.call(pid, {:get_column_range, table_name, primary_key_range, column})
  end

  ### Server API

  def handle_call({:new_table, table_name, _primary_key, columns}, _from, {format, data}) do
    new_format = Map.put_new(format, table_name, columns)
    data_cells =  Enum.reduce(columns, %{},
                  fn column, columns ->
                    Map.put_new(columns, column, %{})
                  end)
    new_data = Map.put_new(data, table_name, data_cells)
    {:reply, {:ok, table_name}, {new_format, new_data}}
  end

  def handle_call({:insert, table_name, primary_key, column_values}, _from, {format, data}) do
    columns = Map.get(format, table_name)
    table = Map.get(data, table_name)
    updated_table =
      Enum.zip(columns, column_values)
      |> Map.new
      |> Enum.reduce(table, fn {column, value}, table ->
          updated_column = Map.get(table, column)
            |> Map.put(primary_key, value)
          Map.put(table, column, updated_column)
        end)
    state = Map.put(data, table_name, updated_table)
    {:reply, {:ok, primary_key}, {format, state}}
  end

  def handle_call({:get_column, table_name, primary_key, column}, _from, {format, data}) do
    value = Map.get(data, table_name)
      |> Map.get(column)
      |> Map.get(primary_key)
    {:reply, {:ok, value}, {format, data}}
  end

  def handle_call({:get_column_range, table_name, primary_key_range, column}, _from, {format, data}) do
    column = Map.get(data, table_name)
      |> Map.get(column)
    values = Enum.map(primary_key_range, fn primary_key -> Map.get(column, primary_key) end)
    {:reply, {:ok, values}, {format, data}}
  end
end