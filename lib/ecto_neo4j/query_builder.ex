defmodule Ecto.Adapters.Neo4j.QueryBuilder do
  @moduledoc false

  import Ecto.Query
  alias Ecto.Adapters.Neo4j.Condition
  alias Ecto.Adapters.Neo4j.Cql.Node, as: NodeCql
  alias Ecto.Adapters.Neo4j.Cql.Relationship, as: RelationshipCql
  alias Ecto.Adapters.Neo4j.Helper

  @valid_operators [:==, :in, :>, :>=, :<, :<, :min, :max, :count, :sum, :avg]
  def build(query_type, queryable_or_schema, sources, opts \\ [])

  def build(_query_type, %Ecto.Query{} = query, sources, [{:is_preload?, true} | _]) do
    {_, schema} = query.from.source

    %Ecto.Query.SelectExpr{expr: {:{}, [], [{{:., _, [_, foreign_key]}, _, _}, _]}} = query.select

    Enum.map(schema.__schema__(:associations), fn assoc ->
      schema.__schema__(:association, assoc)
    end)

    start_node_data =
      Enum.map(schema.__schema__(:associations), fn assoc ->
        schema.__schema__(:association, assoc)
      end)
      |> Enum.filter(fn
        %Ecto.Association.BelongsTo{owner_key: ^foreign_key} -> true
        _ -> false
      end)
      |> List.first()

    %Ecto.Association.BelongsTo{queryable: parent_schema} = start_node_data

    primary_key = parent_schema.__schema__(:primary_key) |> List.first()

    # cql_return =
    #   query.select
    #   |> Map.drop([:expr])
    #   |> build_return()

    {cql_where, where_params} = build_where(query.wheres, sources)

    cql_where =
      cql_where
      |> String.replace(Atom.to_string(foreign_key), format_field(primary_key))
      |> String.replace("n", "n0")

    params =
      where_params
      |> Map.put(primary_key, where_params[foreign_key])
      |> Map.drop([foreign_key])
      |> Helper.manage_id(:to_db)

    cql =
      RelationshipCql.get_related(
        parent_schema.__schema__(:source),
        schema.__schema__(:source),
        cql_where
      )

    {cql, params}
  end

  def build(query_type, %Ecto.Query{} = query, sources, opts) do
    {source, _schema} = query.from.source
    wheres = query.wheres

    #### Alternate build
    # Ecto.Adapters.Neo4j.QueryMapper.map(query, sources)

    ####################

    # {cql_match, match_params} = build_match(query.from, query.joins, sources)

    {cql_update, update_params} = build_update(query.updates, sources)

    {cql_where, where_params} = build_where(wheres, sources)

    cql_return = build_distinct(query.distinct) <> build_return(query.select)

    cql_order_by = build_order_bys(query.order_bys)

    cql_limit = build_limit(query.limit)

    cql_skip = build_skip(query.offset)

    cql =
      NodeCql.build_query(
        query_type,
        source,
        cql_where,
        cql_update,
        cql_return,
        cql_order_by,
        cql_limit,
        cql_skip,
        Keyword.get(opts, :batch, false)
      )

    params = Map.merge(update_params, where_params)

    {cql, Helper.manage_id(params, :to_db)}
  end

  def build(query_type, schema, sources, opts) do
    query = from(s in schema)
    build(query_type, query, sources, opts)
  end

  # DONE
  defp build_distinct(%Ecto.Query.QueryExpr{}) do
    "DISTINCT "
  end

  # DONE
  defp build_distinct(_) do
    ""
  end

  # DONE
  defp build_return(%{fields: []}) do
    "n"
  end

  # DONE
  defp build_return(%{expr: {:&, [], [0]}, fields: select_fields}) do
    build_return_fields(select_fields)
  end

  defp build_return(%{expr: {type, [], select_fields}, fields: alt_select_fields}) do
    case type in [:%{}, :{}] do
      true -> build_return_fields(select_fields)
      _ -> build_return_fields(alt_select_fields)
    end
  end

  # DONE
  defp build_return(%{expr: select_fields}) do
    build_return_fields(select_fields)
  end

  # DONE
  defp build_return(_) do
    "n"
  end

  defp build_match(%{source: {_source_label, _}}, joins, sources) do
    Enum.map(joins, &build_join(&1, sources))
    {"", %{}}
  end

  defp build_join(%Ecto.Query.JoinExpr{source: {join_label, _}, on: on}, sources) do
    # build_where(on, sources)
    # |> IO.inspect(label: "ON #{join_label}")

    build_conditions(on, sources)
    |> IO.inspect(label: "CONDITIONS vvvvvvvvvvvvvvvvvvvvvvvvvvvv\n")
    |> Condition.to_relationship_clauses()
  end

  defp build_return_fields(%Ecto.Query.Tagged{value: field}) do
    format_return_field(field)
  end

  defp build_return_fields(fields) do
    fields
    |> Enum.map(&format_return_field/1)
    |> Enum.join(", ")
  end

  defp format_return_field({aggregate, [], [field | distinct]}) do
    cql_distinct =
      if length(distinct) > 0 do
        "DISTINCT "
      else
        ""
      end

    format_operator(aggregate) <> "(" <> cql_distinct <> resolve_field_name(field) <> ")"
  end

  defp format_return_field(field) do
    resolve_field_name(field, true)
  end

  defp build_limit(%Ecto.Query.QueryExpr{expr: res_limit}) do
    res_limit
  end

  defp build_limit(_) do
    nil
  end

  defp build_skip(%Ecto.Query.QueryExpr{expr: res_skip}) do
    res_skip
  end

  defp build_skip(_) do
    nil
  end

  defp resolve_field_name(field_data, with_alias \\ false)

  defp resolve_field_name({{:., _, [{:&, [], [0]}, field_name]}, [], []}, with_alias) do
    field = format_field(field_name)
    "n." <> field <> field_alias(field, with_alias)
  end

  defp resolve_field_name(
         {field_alias, {{:., _, [{:&, [], [0]}, field_name]}, [], []}},
         with_alias
       ) do
    field = format_field(field_name)
    "n." <> field <> field_alias(field_alias, with_alias)
  end

  defp field_alias(field_alias, true) when is_atom(field_alias) do
    " AS #{Atom.to_string(field_alias)}"
  end

  defp field_alias(field_alias, true) do
    " AS #{field_alias}"
  end

  defp field_alias(_field_alias, false) do
    ""
  end

  defp build_conditions([%{expr: expression}], sources) do
    do_build_condition(expression, sources)
  end

  defp build_conditions(%{} = wheres, sources) do
    build_conditions([wheres], sources)
  end

  defp do_build_condition(
         {operator, _,
          [{{:., _, [{:&, _, [node_idx]}, field]}, [], []}, {:^, _, [sources_index]}]},
         sources
       ) do
    %Condition{
      source: node_idx,
      field: field,
      operator: operator,
      value: Enum.at(sources, sources_index),
      conditions: []
    }
  end

  defp do_build_condition(
         {operator, _,
          [{{:., _, [{:&, _, [node_idx]}, field]}, [], []}, {:^, _, [s_index, s_length]}]},
         sources
       ) do
    %Condition{
      source: node_idx,
      field: field,
      operator: operator,
      value: Enum.slice(sources, s_index, s_length),
      conditions: []
    }
  end

  defp do_build_condition(
         {operator, _, [{{:., _, [{:&, _, [node_idx]}, field]}, [], []}, value]},
         _sources
       ) do
    %Condition{
      source: node_idx,
      field: field,
      operator: operator,
      value: value,
      conditions: []
    }
  end

  defp do_build_condition(
         {operator, _, [{{:., _, [{:&, _, [node_idx]}, field]}, [], []}]},
         _sources
       ) do
    %Condition{
      source: node_idx,
      field: field,
      operator: operator,
      value: :no_value,
      conditions: []
    }
  end

  defp do_build_condition({operator, _, [arg]}, sources) do
    %Condition{
      operator: operator,
      conditions: do_build_condition(arg, sources)
    }
  end

  defp do_build_condition({operator, _, [arg1, arg2]}, sources) do
    %Condition{
      operator: operator,
      conditions: [
        do_build_condition(arg1, sources),
        do_build_condition(arg2, sources)
      ]
    }
  end

  defp build_where([%Ecto.Query.BooleanExpr{expr: expression, params: ecto_params}], sources) do
    {cql_where, unbound_params, _} = do_build_where(expression, sources)

    ecto_params = ecto_params || []
    # Merge unbound params and params explicitly bind in Query
    params =
      ecto_params
      |> Enum.into(%{}, fn {value, {0, field}} ->
        {field, value}
      end)
      |> Map.merge(unbound_params)

    {cql_where, params}
  end

  defp build_where([_ | _] = wheres, sources) do
    {cqls, params} =
      wheres
      |> Enum.map(&build_where(&1, sources))
      |> Enum.reduce({[], %{}}, fn {sub_cql, sub_param}, {cql, params} ->
        {cql ++ [sub_cql], Map.merge(params, sub_param)}
      end)

    # We have to use the operator of the BooleanExpr, not the one inside the expression
    # Because there is as many operators as sub query, we tkae only the last 2 operators
    # to build the final query
    cql =
      Enum.map(wheres, fn %Ecto.Query.BooleanExpr{op: operator} ->
        operator
        |> Atom.to_string()
        |> String.upcase()
      end)
      |> List.delete_at(0)
      |> Kernel.++([""])
      |> Enum.zip(cqls)
      |> Enum.reduce("", fn {sub_cql, operator}, cql ->
        cql <> " " <> operator <> " " <> sub_cql
      end)

    {cql, params}
  end

  defp build_where(%Ecto.Query.BooleanExpr{} = wheres, sources) do
    build_where([wheres], sources)
  end

  defp build_where([], _) do
    {"", %{}}
  end

  defp do_build_where(expression, sources, inc \\ 0)

  # TODO
  defp do_build_where(
         {operator, _, [_, %Ecto.Query.Tagged{type: {_, field}, value: value}]},
         _sources,
         inc
       ) do
    cql = "n.#{format_field(field)} #{format_operator(operator)} {param_#{inc}}"

    params =
      %{"param_#{inc}" => value}
      |> Enum.map(fn {k, v} -> {String.to_atom(k), v} end)
      |> Map.new()

    {cql, params, inc}
  end

  # DONE
  defp do_build_where(
         {operator, _, [{{:., _, [{:&, _, _}, field]}, [], []}, {:^, _, [sources_index]}]},
         sources,
         inc
       ) do
    cql = "n.#{format_field(field)} #{format_operator(operator)} {#{format_field(field)}}"

    params =
      %{}
      |> Map.put(String.to_atom(format_field(field)), Enum.at(sources, sources_index))

    {cql, params, inc}
  end

  # TODO
  defp do_build_where(
         {operator, _,
          [{{:., _, [{:&, _, _}, field]}, [], []}, %Ecto.Query.Tagged{value: {:^, _, [0]}}]},
         sources,
         inc
       ) do
    cql = "n.#{format_field(field)} #{format_operator(operator)} {#{format_field(field)}}"

    params =
      %{}
      |> Map.put(String.to_atom(format_field(field)), List.first(sources))

    {cql, params, inc}
  end

  # DONE
  defp do_build_where(
         {operator, _, [{{:., _, [{:&, _, _}, field]}, [], []}, {:^, _, [s_index, s_length]}]},
         sources,
         inc
       ) do
    cql = "n.#{format_field(field)} #{format_operator(operator)} {#{format_field(field)}}"

    params =
      %{}
      |> Map.put(String.to_atom(format_field(field)), Enum.slice(sources, s_index, s_length))

    {cql, params, inc}
  end

  # DONE
  defp do_build_where(
         {operator, _, [{{:., _, [{:&, _, _}, field]}, [], []}, value]},
         _sources,
         inc
       ) do
    params =
      %{"param_#{inc}" => value}
      |> Enum.map(fn {k, v} -> {String.to_atom(k), v} end)
      |> Map.new()

    cql = "n.#{format_field(field)} #{format_operator(operator)} {param_#{inc}}"
    {cql, params, inc}
  end

  defp do_build_where({operator, _, [{{:., _, [{:&, _, _}, field]}, [], []}]}, _sources, inc) do
    cql = "n.#{format_field(field)} #{format_operator(operator)}"
    {cql, %{}, inc}
  end

  defp do_build_where({operator, _, [arg]}, sources, inc) do
    {cql_sub, params, inc} = do_build_where(arg, sources, inc + 1)
    cql = "#{Atom.to_string(operator)} (#{cql_sub})"

    {cql, params, inc + 1}
  end

  defp do_build_where({operation, _, [arg1, arg2]}, sources, inc) do
    {cql1, params1, inc} = do_build_where(arg1, sources, inc + 1)
    {cql2, params2, _} = do_build_where(arg2, sources, inc + 1)

    cql = "#{cql1} #{Atom.to_string(operation)} #{cql2}"
    params = Map.merge(params1, params2)
    {cql, params, inc + 1}
  end

  defp build_update(updates, sources, update_data \\ [], inc \\ 0)

  defp build_update(
         [%Ecto.Query.QueryExpr{expr: expression} | rest],
         sources,
         update_data,
         inc
       ) do
    {data, inc} = do_build_update_data(:set, Keyword.get(expression, :set, []), sources, inc)

    {data, inc} =
      do_build_update_data(:inc, Keyword.get(expression, :inc, []), sources, inc, data)

    build_update(rest, sources, update_data ++ data, inc + 1)
  end

  defp build_update([], _, update_data, _) do
    {cqls, params} =
      Enum.reduce(update_data, {[], %{}}, fn {sub_cql, sub_params}, {cqls, params} ->
        {cqls ++ [sub_cql], Map.merge(params, sub_params)}
      end)

    {Enum.join(cqls, ", "), params}
  end

  defp do_build_update_data(update_type, expression, sources, inc, result \\ [])

  defp do_build_update_data(
         update_type,
         [{field, {:^, [], [sources_idx]}} | tail],
         sources,
         inc,
         result
       ) do
    cql = build_update_cql(update_type, Atom.to_string(field), inc)
    params = %{"param_up#{inc}" => Enum.at(sources, sources_idx)}

    do_build_update_data(update_type, tail, sources, inc + 1, result ++ [{cql, params}])
  end

  defp do_build_update_data(update_type, [{field, value} | tail], sources, inc, result) do
    cql = build_update_cql(update_type, Atom.to_string(field), inc)
    params = %{"param_up#{inc}" => value}

    do_build_update_data(update_type, tail, sources, inc + 1, result ++ [{cql, params}])
  end

  defp do_build_update_data(_, [], _, inc, result) do
    {result, inc}
  end

  defp build_update_cql(:set, field, inc) do
    "n.#{field} = {param_up#{inc}}"
  end

  defp build_update_cql(:inc, field, inc) do
    "n.#{field} = n.#{field} + {param_up#{inc}}"
  end

  defp build_order_bys([]) do
    ""
  end

  defp build_order_bys([%Ecto.Query.QueryExpr{expr: expression}]) do
    expression
    |> Enum.map(fn {order, fields} ->
      format_order_bys(fields)
      |> Enum.map(fn o -> "#{o} #{order |> Atom.to_string() |> String.upcase()}" end)
    end)
    |> List.flatten()
    |> Enum.join(", ")
  end

  defp format_order_bys(order_by_fields) when is_list(order_by_fields) do
    Enum.map(order_by_fields, &resolve_field_name/1)
  end

  defp format_order_bys(order_by_fields) do
    format_order_bys([order_by_fields])
  end

  defp format_operator(:==) do
    "="
  end

  defp format_operator(:!=) do
    "<>"
  end

  defp format_operator(:in) do
    "IN"
  end

  defp format_operator(:is_nil) do
    "IS NULL"
  end

  defp format_operator(operator) when operator in @valid_operators do
    Atom.to_string(operator)
  end

  defp format_field(:id), do: format_field(:nodeId)
  defp format_field(field), do: field |> Atom.to_string()
end
