
-module(db_manager).
-behaviour(gen_server).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2]).
-export([code_change/3, terminate/2]).
-export([start_link/0]).
-export([cast_query/4]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Id, I, Opts), {Id, {I, start_link, Opts}, temporary, brutal_kill, worker, [I]}).
-define(DEFAULT_WORKERS, 1).
-define(RESTART_MS, 2000).

-record(dbm_state, {queues, sql_queues, refs}).

start_link() ->
    gen_server:start_link({local, db_manager}, ?MODULE, [], []).

init([]) ->
    self() ! start_workers,
    {ok, #dbm_state{queues=dict:new(), sql_queues=dict:new(),
                    refs=ets:new(set, [])}}.

cast_query(Name, Query, Params, ReturnParams) ->
  gen_server:cast(db_manager, {sql_query, Name, Query, Params, {param_sql_cast, self(), ReturnParams}}).

handle_cast({db_worker, register, Pid, Name},
            #dbm_state{queues=Qs, sql_queues=SQs}=OldState) ->
  SQ = SQs:fetch(Name),
  Q1 = Qs:fetch(Name),
  case queue:len(SQ) > 0 of
    true ->
      {{value, Query}, SQ1} = queue:out(SQ),
      gen_server:cast(Pid, Query),
      {noreply, OldState#dbm_state{sql_queues=SQs:store(Name, SQ1)}};
    false ->
      Q2 = queue:in(Pid, Q1),
      {noreply, OldState#dbm_state{queues=Qs:store(Name, Q2)}}
  end;

handle_cast({sql_query, Name, Query, Par, Pid},
            #dbm_state{queues=Qs, sql_queues=SQs}=OldState) ->
  SQ = SQs:fetch(Name),
  Q1 = Qs:fetch(Name),
  case queue:out(Q1) of
    {{value, Worker}, Q2} ->
      lager:debug("PID ~p~n", [Pid]),
      gen_server:cast(Worker, {sql_query, Query, Par, Pid}),
      {noreply, OldState#dbm_state{queues=Qs:store(Name, Q2)}};
    {empty, _} ->
      SQ1 = queue:in({sql_query, Query, Par, Pid}, SQ),
      {noreply, OldState#dbm_state{sql_queues=SQs:store(Name, SQ1)}}
  end;

handle_cast(_, State) ->
    {noreply, State}.

handle_call({cast, Name, Sql, Params, Ident}, From, State) ->
  gen_server:cast(self(), {sql_query, Name, Sql, Params, {param_sql_cast, From, Ident}}),
  {reply, ok, State};
handle_call({Name, Sql, Params, Ident}, From, State) ->
  gen_server:cast(self(), {sql_query, Name, Sql, Params, {param_sql, From, Ident}}),
  {reply, ok, State};
handle_call(_, _, State) ->
  {noreply, State}.

handle_info(start_workers, State) ->
    case application:get_env(connections) of
      undefined ->
        {noreply, State};
      {ok, ConnList} ->
        Names = [Name || {Name, _, _, _} <- ConnList],
        NewState = lists:foldl(fun check_name/2, State, Names),
        [handle_params(Name, Type, Args, Opts, NewState) ||
          {Name, Type, Args, Opts} <- ConnList],
        {noreply, NewState}
    end;

handle_info({'DOWN', Ref, process, _Pid, Rsn}, #dbm_state{refs=Tbl, queues=Qs}=State) ->
  case ets:lookup(Tbl, Ref) of
    [] -> {noreply, State};
    [{Ref, Name, AllParams}] ->
      Q1 = Qs:fetch(Name),
      Q2 = queue:filter(fun(P) -> P =/= _Pid end, Q1),
      lager:warning("Got 'DOWN' from ~p worker => ~p", [lists:nth(3, AllParams),
                                                        Rsn]),
      timer:send_after(?RESTART_MS, self(), {restart_worker, AllParams}),
      {noreply, State#dbm_state{queues=Qs:store(Name, Q2)}}
  end;
handle_info({restart_worker, AllParams}, State) ->
  erlang:apply(fun start_worker/8, AllParams),
  {noreply, State};
handle_info(Req, State) ->
  lager:warning("Unhandled info ~p", [Req]),
  {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

check_name(Name, #dbm_state{queues=Qs, sql_queues=SQs}=State) ->
  lager:info("Try to register name ~p", [Name]),
  case Qs:find(Name) of
    error ->
      State#dbm_state{queues = Qs:store(Name, queue:new()),
                      sql_queues = SQs:store(Name, queue:new())};
    _ -> State
  end.

handle_params(Name, Type, Args, Opts, #dbm_state{refs=Tbl}=_State) ->
  Host = proplists:get_value(host, Args),
  User = proplists:get_value(user, Args),
  Pw = proplists:get_value(password, Args),
  Database = proplists:get_value(database, Args),
  NumWorkers = proplists:get_value(workers, Opts, ?DEFAULT_WORKERS),
  [start_worker(Type, Tbl, Name, Num, Host, User, Pw, Database) ||
    Num <- lists:seq(1, NumWorkers)].

start_worker(pg, Tbl, Name, Num, Host, User, Pw, Database) ->
  C = ?CHILD({Name, Num}, pg_worker, [[Name, Host, User, Pw, Database, self()]]),
  lager:debug("start pg_worker ~p", [Name]),
  {ok, Pid} = supervisor:start_child(db_worker_sup, C),
  Ref = erlang:monitor(process, Pid),
  lager:warning("MonRef ~p", [Ref]),
  AllOpts = [pg, Tbl, Name, Num, Host, User, Pw, Database],
  ets:insert(Tbl, {Ref, Name, AllOpts}).
  
