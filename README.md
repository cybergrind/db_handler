# db_handler

Erlang integration with postgres

## DB API

See config example in `dev.config` file.

### Basic queries
Example:

```erlang
    %db_manager:cast_query(Query, Params, ReturnParams)
    3> db_manager:cast_query("SELECT 1", [], return_me).
    4> flush().
    Shell got {'$gen_cast',{return_me,{ok,[{column,<<"?column?">>,int4,4,-1,1}],
                                      [{1}]}}}
    ok

```

You can use 3 main API calls:

**{sql_query, Query, Params, {param_sql_cast, Pid, QParams}}**

Sends answer via *gen_server:cast(Pid, {QParams, Result}*.

Need for handle requests with *gen_server:handle_cast*.

```erlang
    %% example gen_server realization
    init([UserId]) ->
      process_flag(trap_exit, true),
      gen_server:cast(Out, {add_user, self()}),
      gen_server:cast(db_manager,
                      {sql_query,
                       ?SQL_USER_LOAD,
                       [UserId],
                       {param_sql_cast, self(), user_load}
                      }
                     ),
      State = #user{},
      {ok, State}.

    % user load callbacks
    handle_cast({user_load, {ok, _Columns, [FirstRow | RestRows]}},
              State) ->
    {noreply, State};
```

**{sql_query, Query, Params, {param_sql, Pid, QParams}}**

Send answer with simple message (use gen_server:handle_info) - *{QParams, Result}*

**{sql_query, Query, Params, Pid}**

Send answer with simple messsage without additional parameters


### Work with transactions: db_manager:with_connection

*with_connection* method will automatically add `BEGIN;` and `COMMIT;` sql's
before and after function call.

It accepts: {M, F, A} or {Function, Arguments} as parameters.

```erlang
    % File version:
    in_transaction([Connection]) ->
        {ok, _, _} = pgsql:equery(Connection, "select 1;"),
        {ok, _, _} = pgsql:equery(Connection, "select 2;").

    db_manager:with_connection({fun in_transaction/1, []}).

    $ Console version:
    IN_TRANSACTION = fun ([Connection]) ->
        {ok, _, _} = pgsql:equery(Connection, "select 1;"),
        {ok, _, _} = pgsql:equery(Connection, "select 2;") end.

    db_manager:with_connection({IN_TRANSACTION, []}).
```
