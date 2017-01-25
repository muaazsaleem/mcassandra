defmodule Mcassandra.NodeTest do
  use ExUnit.Case

  setup do
    {:ok, pid} = Mcassandra.Node.start_link
    %{pid: pid}
  end

  test "Creates a table", %{pid: pid} do
    assert {:ok, :employees} == Mcassandra.Node.create_table(pid, :employees, :id, [:name, :email])
  end

  test "Inserts a row in a table", %{pid: pid} do
    {:ok, :employees} = Mcassandra.Node.create_table(pid, :employees, :id, [:name, :email])
    assert {:ok, 42} == Mcassandra.Node.insert(pid, :employees, 42, ["me", "me.co"])
  end

  test "Gets a column key", %{pid: pid} do
    {:ok, :employees} = Mcassandra.Node.create_table(pid, :employees, :id, [:name, :email])
    {:ok, 42} = Mcassandra.Node.insert(pid, :employees, 42, ["me", "me.co"])
    assert {:ok, "me"} == Mcassandra.Node.get_column(pid, :employees, 42, :name)
  end

  test "Gets a column range", %{pid: pid} do
    {:ok, :employees} = Mcassandra.Node.create_table(pid, :employees, :id, [:name, :email])
    {:ok, 42} = Mcassandra.Node.insert(pid, :employees, 42, ["me", "me.co"])
    {:ok, 43} = Mcassandra.Node.insert(pid, :employees, 43, ["you", "you.co"])
    assert {:ok, ["me","you"]} == Mcassandra.Node.get_column_range(pid, :employees, 42..43, :name)
  end

  test "Gets a column range in reverse", %{pid: pid} do
    {:ok, :employees} = Mcassandra.Node.create_table(pid, :employees, :id, [:name, :email])
    {:ok, 42} = Mcassandra.Node.insert(pid, :employees, 42, ["me", "me.co"])
    {:ok, 43} = Mcassandra.Node.insert(pid, :employees, 43, ["you", "you.co"])
    assert {:ok, ["you","me"]} == Mcassandra.Node.get_column_range(pid, :employees, 43..42, :name)
  end
end