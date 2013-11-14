
-module(db_worker_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Opts), {I, {I, start_link, Opts}, permanent, brutal_kill, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
  {ok, Host} = application:get_env(host),
  {ok, User} = application:get_env(user),
  {ok, Password} = application:get_env(password),
  {ok, Database} = application:get_env(database),
  Opts = [Host, User, Password, [{database, Database}]],
  Child = ?CHILD(db_worker, worker, [Opts]),
  {ok, { {simple_one_for_one, 100, 1}, [Child]} }.
