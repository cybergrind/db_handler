
-module(db_handler_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(I, Type, Opts), {I, {I, start_link, Opts}, permanent, 5000, Type, [I]}).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    DbW = ?CHILD(db_worker_sup, supervisor, []),
    Man = ?CHILD(db_manager, worker, []),
    {ok, { {one_for_all, 5, 10}, [DbW, Man]} }.

