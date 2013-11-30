
-module(pg_worker).
-behaviour(gen_server).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2]).
-export([code_change/3, terminate/2]).
-export([start_link/1]).
-import(proplists, [get_value/3, get_value/2]).
-record(db_state, {name, connection,
                   manager}).

-define(SHUTDOWN_TIMEOUT, 1000).

connect(Host, User, Pass, Opts) ->
  process_flag(trap_exit, true),
  lager:debug("Connect with args: ~p ~p ~p ~p~n", [Host, User, Pass, Opts]),
  Ret = pgsql:connect(Host, User, Pass, Opts),
  lager:debug("Got ret: ~p~n", [Ret]),
  {ok, Connection} = Ret,
  Connection.


start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Params) ->
  Name = get_value(name, Params, default),
  Host = get_value(host, Params, "localhost"),
  User = get_value(user, Params, "postgres"),
  Pass = get_value(password, Params),
  Database = get_value(database, Params),
  gen_server:cast(self(), {connect, Host, User, Pass, Database}),
  {ok, #db_state{name=Name, manager=no_manager}}.

handle_cast({connect, Host, User, Pass, Database},
            #db_state{name=Name, manager=Manager}=State) ->
  Opts = [{database, Database}],
  NewState = State#db_state{connection=connect(Host, User, Pass, Opts)},
  gen_server:cast(Manager, {db_worker, register, self(), Name}),
  {noreply, NewState};
handle_cast({sql_query, Query, Params, {param_sql_cast, Pid, QParams}},
             #db_state{name=Name, connection=C}=State) ->
  lager:debug("Handle new type query cast"),
  Result = pgsql:equery(C, Query, Params),
  lager:debug("Send result ~p to ~p~n", [Result, Pid]),
  gen_server:cast(Pid, {QParams, Result}),
  poolboy:checkin(Name, self()),
  lager:debug("Checkin ok ~p", [Name]),
  {noreply, State};
handle_cast({sql_query, Query, Params, {param_sql, Pid, QParams}},
             #db_state{name=Name, connection=C, manager=Manager}=State) ->
  lager:debug("Handle new type query"),
  Result = pgsql:equery(C, Query, Params),
  lager:debug("Send result ~p to ~p~n", [Result, Pid]),
  Pid ! {QParams, Result},
  gen_server:cast(Manager, {db_worker, register, self(), Name}),
  {noreply, State};
handle_cast({sql_query, Query, Params, Pid},
            #db_state{name=Name, connection=C, manager=Manager}=State) ->
  lager:debug("Old query PID ~p~n", [Pid]),
  Result = pgsql:equery(C, Query, Params),
  Pid ! Result,
  gen_server:cast(Manager, {db_worker, register, self(), Name}),
  {noreply, State};
handle_cast(Req, State) ->
  lager:warning("Unhandled cast ~p", [Req]),
  {noreply, State}.

handle_call({sql_query, Query, Params, {param_sql_cast, Pid, QParams}}, _,
             #db_state{connection=C}=State) ->
  lager:debug("Handle new type query cast"),
  Result = pgsql:equery(C, Query, Params),
  lager:debug("Send result ~p to ~p~n", [Result, Pid]),
  gen_server:cast(Pid, {QParams, Result}),
  {reply, ok, State};
handle_call(Req, _, State) ->
  lager:warning("Unhandled call ~p", [Req]),
  {noreply, State}.

handle_info(Req, State) ->
  lager:warning("Unhandled info ~p", [Req]),
  {noreply, State}.

terminate(_, _) ->
  timer:sleep(?SHUTDOWN_TIMEOUT),
  ok.

code_change(_, _, _) ->
  ok.

