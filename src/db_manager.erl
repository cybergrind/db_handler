
-module(db_manager).
-behaviour(gen_server).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2]).
-export([code_change/3, terminate/2]).
-export([start_link/0]).
-export([cast_query/3]).

-define(START_WORKERS, 5).
-record(dbm_state, {queue,
                    sql_queue}).

start_link() ->
    gen_server:start_link({local, db_manager}, ?MODULE, [], []).

init([]) ->
    self() ! {start_workers},
    Q = queue:new(),
    {ok, #dbm_state{queue=Q, sql_queue=queue:new()}}.

start_worker(Num) ->
  case Num > 0 of
    true ->
      lager:debug("start child~n"),
      case supervisor:start_child({db_worker_sup, node()}, [self()]) of
        {ok, Pid} ->
          _Ref = erlang:monitor(process, Pid);
        {error, Error} ->
          error_logger:error_msg("Cannot start child ~p", [Error])
      end,
      start_worker(Num - 1);
    false ->
      ok
  end.

cast_query(Query, Params, ReturnParams) ->
  gen_server:cast(db_manager, {sql_query, Query, Params, {param_sql_cast, self(), ReturnParams}}).

handle_cast({db_worker, register, Pid}, #dbm_state{queue=Q1, sql_queue=SQ}=OldState) ->
  case queue:len(SQ) > 0 of
    true ->
      {{value, Query}, SQ1} = queue:out(SQ),
      gen_server:cast(Pid, Query),
      State = OldState#dbm_state{sql_queue=SQ1};
    false ->
      Q2 = queue:in(Pid, Q1),
      State = OldState#dbm_state{queue=Q2}
  end,
  {noreply, State};

handle_cast({sql_query, Query, Par, Pid}, #dbm_state{queue=Q1, sql_queue=SQ}=OldState) ->
  case queue:out(Q1) of
    {{value, Worker}, Q2} ->
      lager:debug("PID ~p~n", [Pid]),
      gen_server:cast(Worker, {sql_query, Query, Par, Pid}),
      State = OldState#dbm_state{queue=Q2};
    {empty, _} ->
      State = OldState#dbm_state{sql_queue=queue:in({sql_query, Query, Par, Pid}, SQ)}
  end,
  {noreply, State};

handle_cast(_, State) ->
    {noreply, State}.

handle_call({get_queue}, _From, State) ->
  {reply, State#dbm_state.queue, State};
handle_call({cast, Sql, Params, Ident}, From, State) ->
  gen_server:cast(self(), {sql_query, Sql, Params, {param_sql_cast, From, Ident}}),
  {reply, ok, State};
handle_call({Sql, Params, Ident}, From, State) ->
  gen_server:cast(self(), {sql_query, Sql, Params, {param_sql, From, Ident}}),
  {reply, ok, State};
handle_call(_, _, State) ->
  {noreply, State}.

handle_info({start_workers}, State) ->
    start_worker(?START_WORKERS),
    {noreply, State};
handle_info({'DOWN', _Ref, process, _Pid, _}, #dbm_state{queue=Q1}=State) ->
    Q2 = queue:filter(fun(P) -> P =/= _Pid end, Q1),
    {noreply, State#dbm_state{queue=Q2}};
handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

