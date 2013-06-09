%% @author Uvarov Michael <freeakk@gmail.com>
%% @end
-module(etorrent_azdht_search).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).
-import(etorrent_azdht, [
        compact_contact/1,
        compact_contacts/1,
        node_id/1]).



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
    key, encoded_key, called_contacts, waiting_contacts,
    collected_values, answered_contacts, answered_count
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
    EncodedKey = encode_key(Key),
    [async_find_value(Contact, EncodedKey) || Contact <- Contacts],
    State = #state{key=Key,
                   encoded_key=EncodedKey,
                   called_contacts=sets:from_list(Contacts),
                   waiting_contacts=[],
                   collected_values=[],
                   answered_count=0,
                   answered_contacts=sets:new()},
    schedule_next_step(),
    {ok, State}.

handle_call(x, From, State) ->
    {reply, x, State}.

handle_cast({async_find_value_reply, Contact,
             #find_value_reply{has_values=false,
                               contacts=ReceivedContacts}},
             #state{called_contacts=Contacts,
                    waiting_contacts=WaitingContacts,
                    encoded_key=EncodedKey}=State) ->
    lager:debug("Received reply from ~p with contacts:~n~p",
                [compact_contact(Contact), compact_contacts(ReceivedContacts)]),
    Contacts1 = drop_farther_contacts(ReceivedContacts, Contact, EncodedKey),
    Contacts2 = drop_duplicates(Contacts1, Contacts),
    {noreply, State#state{waiting_contacts=Contacts2 ++ WaitingContacts}};
handle_cast({async_find_value_reply, Contact,
             #find_value_reply{has_values=true, values=Values}},
             #state{answered_count=AnsweredCount,
                    answered_contacts=Answered,
                    collected_values=CollectedValues}=State) ->
    lager:debug("Received reply from ~p with values:~n~p",
                [compact_contact(Contact), Values]),
    State2 = State#state{answered_count=AnsweredCount+1,
                         answered_contacts=sets:add_element(Contact, Answered),
                         collected_values=Values ++ CollectedValues},
    {noreply, State2};
handle_cast({async_find_value_error, Contact, Reason}, State) ->
    lager:debug("~p is unreachable. Reason ~p.",
                [compact_contact(Contact), Reason]),
    {noreply, State}.

handle_info(next_step,
            State=#state{key=Key,
                         waiting_contacts=[],
                         collected_values=[]}) ->
    lager:debug("Key ~p was not found.", [Key]),
    {noreply, State};
handle_info(next_step,
            State=#state{collected_values=Values,
                         answered_count=AnsweredCount}) when AnsweredCount > 3 ->
    lager:debug("Values ~p", [Values]),
    {noreply, State};
handle_info(next_step,
            State=#state{collected_values=Values,
                         waiting_contacts=[],
                         answered_count=AnsweredCount}) when AnsweredCount > 0 ->
    lager:debug("Not enough nodes were called. Values ~p", [Values]),
    {noreply, State};
handle_info(next_step,
            State=#state{encoded_key=EncodedKey,
                         called_contacts=Contacts,
                         waiting_contacts=WaitingContacts}) ->
    %% Run the next search iteration.
    BestContacts = best_contacts(WaitingContacts, EncodedKey),
    lager:debug("Best contacts:~n~p", [BestContacts]),
    [async_find_value(Contact, EncodedKey) || Contact <- BestContacts],
    State2 = State#state{waiting_contacts=[],
                         called_contacts=sets:union(sets:from_list(BestContacts),
                                                    Contacts)},
    schedule_next_step(),
    {noreply, State2}.

terminate(_, _State) ->
    ok.

code_change(_, _, State) ->
    {ok, State}.

%% ==================================================================


async_find_value(Contact, EncodedKey) ->
    Parent = self(),
    spawn_link(fun() ->
            case etorrent_azdht_net:find_value(Contact, EncodedKey) of
                {ok, Reply} ->
                    gen_server:cast(Parent, {async_find_value_reply, Contact, Reply});
                {error, Reason} ->
                    gen_server:cast(Parent, {async_find_value_error, Contact, Reason})
            end
        end).

encode_key(Key) ->
    crypto:sha(Key).

compute_distance(<<ID1:160>>, <<ID2:160>>) ->
    <<(ID1 bxor ID2):160>>.


%% Returns all contacts with distance lower than the distance beetween
%% `BaseContact' and `EncodedKey'.
drop_farther_contacts(Contacts, BaseContact, EncodedKey) ->
    BaseDistance = compute_distance(node_id(BaseContact), EncodedKey),
    [C || C <- Contacts, compute_distance(node_id(C), EncodedKey) < BaseDistance].

drop_duplicates(UnfilteredContacts, ContactSet) ->
    [C || C <- UnfilteredContacts, not sets:is_element(C, ContactSet)].

best_contacts(Contacts, EncodedKey) ->
    D2C1 = [{compute_distance(node_id(C), EncodedKey), C} || C <- Contacts],
    D2C2 = lists:usort(D2C1),
    Best = lists:sublist(D2C2, 32),
    [C || {_D,C} <- Best].



schedule_next_step() ->
    erlang:send_after(5000, self(), next_step),
    ok.
