db_handler
==========

Erlang integration with postgres

DB API
======

Example: 

.. code-block:: erlang

    %gen_server:cast({sql_query, Query, Params, Pid})
    2> Pid = self().
    3> gen_server:cast(db_manager, {sql_query, "SELECT 1", [], Pid}).
    14> flush().
    Shell got {ok,[{column,<<"?column?">>,int4,4,-1,1}],[{1}]}


You can use 3 main API calls:

**{sql_query, Query, Params, {param_sql_cast, Pid, QParams}}**

Sends answer via *gen_server:cast(Pid, {QParams, Result}*. 

Need for handle requests with *gen_server:handle_cast*.

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

**{sql_query, Query, Params, {param_sql, Pid, QParams}}**

Send answer with simple message (use gen_server:handle_info) - *{QParams, Result}*

**{sql_query, Query, Params, Pid}**

Send answer with simple messsage without additional parameters

