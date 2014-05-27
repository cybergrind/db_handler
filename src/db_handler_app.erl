-module(db_handler_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, config_change/3]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  db_handler_sup:start_link().

stop(_State) ->
  ok.

config_change(Changed, New, Removed) ->
  lager:error("Change config: ~p ~p ~p", [Changed, New, Removed]),
  case lists:any(fun (X) -> X end,
                 [proplists:get_value(need_restart, Changed),
                  proplists:get_value(need_restart, New)]) of
    true ->
      spawn(db_handler, restart_app, []);
    false ->
      % TODO: handle config update
      ok end.
