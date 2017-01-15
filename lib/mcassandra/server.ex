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
    IO.puts "Creating table:#{table_name} with primary_key:#{primary_key} \
     with columns: #{Enum.join columns, ", "}"

    GenServer.call(pid, {:new_table, table_name, primary_key, columns})
  end

  @doc """
  Will insert each column into respective Agents as {primany_key, column_values}
  """
  def insert(pid, table_name, primary_key, columns) do
    IO.puts "Inserting columns: #{Enum.join columns, ", "}
    with primary_key: #{primary_key}
    in table: #{table_name}"
    GenServer.call(pid, {:insert, table_name, primary_key, columns})
  end

  # Server API

  def handle_call({:new_table, table_name, _primary_key, columns}, _from, {format, data}) do
    new_format = Map.put_new(format, table_name, columns)

    data_cells =  Enum.reduce(columns,
                  %{},
                  fn column, columns ->
                    Map.put_new(columns, column, %{})
                  end)

    new_data = Map.put_new(data, table_name, data_cells)

    {:reply, :ok, {new_format, new_data}}
  end

  def handle_call({:insert, table_name, primary_key, column_values}, _from, {format, data}) do
      columns = Map.get(format, table_name)

      data_cells = Map.get(data, table_name)

      new_data = Enum.zip(columns, column_values)
        |> Map.new
        |> Enum.reduce(data_cells,
          fn {column, value}, data_cells ->
            Map.put(data_cells, column, %{primary_key => value})
          end)

      {:reply, :ok, {format, new_data}}
  end

end