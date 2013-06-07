%% @author Uvarov Michael <freeakk@gmail.com>
%% @end
-module(etorrent_azdht_search).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).


% Public interface
-export([find_value/2]).

-include_lib("etorrent_core/include/etorrent_azdht.hrl").

% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    key, encoded_key, contacts
}).

%
% Type definitions and function specifications
%


%
% Contacts and settings
%


%
% Public interface
%
find_value(Key, Contacts) ->
    gen_server:start_link(?MODULE, [Key, Contacts], []).

%% ==================================================================

init([Key, Contacts]) ->
    [async_ping(Contact) || Contact <- Contacts],
    State = #state{key=Key,
                   encoded_key=encode_key(Key),
                   contacts=Contacts},
    {ok, State}.

handle_call(x, From, State) ->
    {reply, x, State}.

handle_cast({async_ping_reply, Contact, Reply}, State) ->
    lager:debug("Received ping reply from ~p.", [Contact]),
    {noreply, State};
handle_cast({async_ping_error, Contact, Reason}, State) ->
    lager:debug("~p is unreachable. Reason ~p.", [Contact, Reason]),
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_, _State) ->
    ok.

code_change(_, _, State) ->
    {ok, State}.

%% ==================================================================


async_find_value(Contact, Key) ->
    Parent = self(),
    spawn_link(fun() ->
            case etorrent_azdht_net:find_value(Contact, Key) of
                {ok, Reply} ->
                    gen_server:cast(Parent, {async_find_value_reply, Contact, Reply});
                {error, Reason} ->
                    gen_server:cast(Parent, {async_find_value_error, Contact, Reason})
            end
        end).

async_ping(Contact) ->
    Parent = self(),
    spawn_link(fun() ->
            case etorrent_azdht_net:ping(Contact) of
                {ok, Reply} ->
                    gen_server:cast(Parent, {async_ping_reply, Contact, Reply});
                {error, Reason} ->
                    gen_server:cast(Parent, {async_ping_error, Contact, Reason})
            end
        end).


encode_key(Key) ->
    crypto:sha(Key).

compute_distance(<<ID1:160>>, <<ID2:160>>) ->
    <<(ID1 bxor ID2):160>>.
    
