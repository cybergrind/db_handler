
-module(db_manager).
-behaviour(gen_server).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2]).
-export([code_change/3, terminate/2]).
-export([start_link/0]).
-export([cast_query/4, cast_query/3, add_worker/1]).
-export([sync_send/1]).

-define(DEFAULT_WORKERS, 1).
-define(RESTART_MS, 2000).


-record(dbm_state, {queues, sql_queues, refs, default}).

start_link() ->
    gen_server:start_link({local, db_manager}, ?MODULE, [], []).

init([]) ->
  lager:debug("init db_manager"),
  self() ! start_workers,
  {ok, #dbm_state{refs=ets:new(set, [])}}.

% db_manager:cast_query(test1, "select 1;", [], ret).
% [db_manager:cast_query(test1, "select 1;", [], ret) || X <- lists:seq(1, 10)].
cast_query(Name, Query, Params, ReturnParams) ->
  gen_server:cast(db_manager, {sql_query, Name, Query, Params, {param_sql_cast, self(), ReturnParams}}).
cast_query(Query, Params, ReturnParams) ->
  gen_server:cast(db_manager, {sql_query, Query, Params, {param_sql_cast, self(), ReturnParams}}).


add_worker(WorkerSpec) ->
  gen_server:cast(db_manager, {add_worker, WorkerSpec}).

sync_send({sql_query, Name, Query, Par, Pid})->
  Worker = poolboy:checkout(Name, true, infinity),
  gen_server:call(Worker, {sql_query, Query, Par, Pid}, infinity).

handle_cast({add_worker, WorkerSpec}, State) ->
  {Name, Type, Args, Opts} = WorkerSpec,
  handle_params(Name, Type, Args, Opts, State),
  {noreply, State};

handle_cast({sql_query, Query, Par, Pid},
            #dbm_state{default=Name}=State) ->
  handle_cast({sql_query, Name, Query, Par, Pid}, State);

handle_cast({sql_query, Name, Query, Par, Pid}=Params,
            State) ->
  case poolboy:checkout(Name, false) of
    full ->
      % TODO: handle full queues in separate process
      lager:debug("run into separate process due full pool"),
      spawn(?MODULE, sync_send, [Params]),
      {noreply, State};
    Worker ->
      gen_server:cast(Worker, {sql_query, Query, Par, Pid}),
      {noreply, State} end;
handle_cast(Req, State) ->
  lager:warning("Unhandled cast ~p", [Req]),
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
      [handle_params(Name, Type, Args, Opts, State) ||
        {Name, Type, Args, Opts} <- ConnList],
      [{Default, _, _, _} | _] = ConnList,
      {noreply, State#dbm_state{default=Default}}
  end;

handle_info(Req, State) ->
  lager:warning("Unhandled info ~p", [Req]),
  {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

handle_params(Name, Type, Args, Opts, _State) ->
  WorkerModule =
    case Type of
        pg -> pg_worker end,
  Spec = poolboy:child_spec(Name, [{name, {local, Name}},
                                   {worker_module, WorkerModule} | Opts],
                            [{name, Name} | Args]),
  lager:info("start ~p", [Spec]),
  {ok, _Pid} = supervisor:start_child(db_worker_sup, Spec).
