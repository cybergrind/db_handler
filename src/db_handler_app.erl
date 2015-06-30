-module(db_handler_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, config_change/3]).
-export([full_start/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  db_handler_sup:start_link().

stop(_State) ->
  ok.

config_change(Changed, New, Removed) ->
  lager:error("Change config: ~p ~p ~p", [Changed, New, Removed]),
  gen_server:call(db_manager, config_change),
  ok.

full_start() ->
  application:ensure_all_started(db_handler).
