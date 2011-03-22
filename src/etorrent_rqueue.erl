%% @author Magnus Klaar <magnus.klaar@gmail.com>
%% @doc Request queue for peers
%% This module provides a wrapper for the queue module from stdlib, it
%% assumes that responses to chunk requests are delivered in the same
%% order as they were sent.
%%
%% The module provides additional functions for validating incoming
%% requests against the head of the queue. We want to verify that the
%% piece, offset and chunk length matches. We also want to be able to
%% detect if the piece and offset but not the chunk length matches.
%%
%% The module also provides additional functions for checking if the
%% request pipeline needs to be refilled or not. The thresholds are
%% encapsulated in the request queue.
%% @end
-module(etorrent_rqueue).

-export([new/0,
         new/2,
         to_list/1,
         push/2,
         push/4,
         pop/1,
         peek/1,
         size/1,
         is_head/4,
         has_offset/3,
         is_low/1,
         needs/1]).


-type pieceindex() :: etorrent_types:piece_index().
-type chunkoffset() :: non_neg_integer().
-type chunklength() :: pos_integer().
-type requestspec() :: {pieceindex(), chunkoffset(), chunklength()}.

-record(requestqueue, {
    low_limit  :: non_neg_integer(),
    high_limit :: pos_integer(),
    queue      :: queue()}).
-opaque rqueue() :: #requestqueue{}.
-export_type([rqueue/0]).


%% @doc Create an empty request queue with default pipeline thresholds
%% @end
-spec new() -> rqueue().
new() ->
    new(2, 10).


%% @doc Get the list of chunks in the request queue
%% @end
-spec to_list(#requestqueue{}) -> [requestspec()].
to_list(Requestqueue) ->
    #requestqueue{queue=Queue} = Requestqueue,
    queue:to_list(Queue).


%% @doc Create an empty request queue and specify pipeline thresholds
%% @end
-spec new(non_neg_integer(), pos_integer()) -> rqueue().
new(Lowthreshold, Highthreshold) ->
    InitQueue = #requestqueue{
        low_limit=Lowthreshold,
        high_limit=Highthreshold,
        queue=queue:new()},
    InitQueue.


%% @doc Push a request onto the end of the request queue
%% The queue returns a new queue including the new request.
%% @end
-spec push(pieceindex(), chunkoffset(),
           chunklength(), #requestqueue{}) -> rqueue().
push(Pieceindex, Offset, Length, Requestqueue) ->
    #requestqueue{queue=Queue} = Requestqueue,
    NewQueue = queue:in({Pieceindex, Offset, Length}, Queue),
    Requestqueue#requestqueue{queue=NewQueue}.


%% @doc Push a list of requests onto the end of the request queue
%% The function returns a new queue including the new requests.
%% @end
-spec push([requestspec()], #requestqueue{}) -> rqueue().
push(Requests, Requestqueue) ->
    #requestqueue{queue=Queue} = Requestqueue,
    TmpQueue = queue:from_list(Requests),
    NewQueue = queue:join(Queue, TmpQueue),
    Requestqueue#requestqueue{queue=NewQueue}.


%% @doc Return the head of the request queue and the tail of the queue
%% If the request queue is empty the function will throw a badarg error.
%% @end
-spec pop(#requestqueue{}) -> {requestspec(), rqueue()}.
pop(Requestqueue) ->
    #requestqueue{queue=Queue} = Requestqueue,
    case queue:out(Queue) of
        {empty, _} ->
            erlang:error(badarg);
        {{value, Head}, Tail} ->
            NewReqs = Requestqueue#requestqueue{queue=Tail},
            {Head, NewReqs}
    end.


%% @doc Return the head of the queue
%% If the queue is empty this function will return false.
%% @end
-spec peek(rqueue()) -> false | requestspec().
peek(Requestqueue) ->
    #requestqueue{queue=Queue} = Requestqueue,
    case queue:peek(Queue) of
        empty -> false;
        {value, {_,_,_}=Head} -> Head
    end.


%% @doc Return the number of requests in the queue.
%% @end
-spec size(#requestqueue{}) -> non_neg_integer().
size(Requestqueue) ->
    #requestqueue{queue=Queue} = Requestqueue,
    queue:len(Queue).


%% @doc Check if a request is at the head of the request queue
%% @end
-spec is_head(pieceindex(), chunkoffset(),
              chunklength(), rqueue()) -> boolean().
is_head(Pieceindex, Offset, Length, Requestqueue) ->
    I = Pieceindex,
    O = Offset,
    L = Length,
    case peek(Requestqueue) of
        false   -> false;
        {I,O,L} -> true;
        _       -> false
    end.


%% @doc Check if the offset of a request matches the head of the queue
%% @end
-spec has_offset(pieceindex(), chunkoffset(), rqueue()) -> boolean().
has_offset(Pieceindex, Offset, Requestqueue) ->
    I = Pieceindex,
    O = Offset,
    case peek(Requestqueue) of
        false   -> false;
        {I,O,_} -> true;
        _       -> false
    end.


%% @doc Check if the number or open requests is below the pipeline threshold
%% @end
-spec is_low(#requestqueue{}) -> boolean().
is_low(Requestqueue) ->
    #requestqueue{low_limit=Low, queue=Queue} = Requestqueue,
    queue:len(Queue) =< Low.


%% @doc Return the number of requests needed to fill the queue.
%% If the queue already contains the number of requests specified
%% by the high threshold of the request queue zero is returned.
%% If not, the number of requests needed to hit the high threshold
%% is returned regardless of whether the queue is low or not.
%% @end
-spec needs(#requestqueue{}) -> non_neg_integer().
needs(Requestqueue) ->
    #requestqueue{high_limit=High, queue=Queue} = Requestqueue,
    Length = queue:len(Queue),
    case Length < High of
        true  -> High - Length;
        false -> 0
    end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(rqueue, etorrent_rqueue).

empty_test_() ->
    Q0 = ?rqueue:new(),
    [?_assertEqual(0, ?rqueue:size(Q0)),
     ?_assertError(badarg, ?rqueue:pop(Q0)),
     ?_assertEqual(false, ?rqueue:peek(Q0)),
     ?_assertNot(?rqueue:is_head(0, 0, 0, Q0)),
     ?_assertNot(?rqueue:has_offset(0, 0, Q0)),
     ?_assertEqual([], ?rqueue:to_list(Q0))].

one_request_test_() ->
    Q0 = ?rqueue:new(),
    Q1 = ?rqueue:push(0, 0, 1, Q0),
    {Req, Q2} = ?rqueue:pop(Q1),
    [?_assertEqual(1, ?rqueue:size(Q1)),
     ?_assertEqual({0,0,1}, ?rqueue:peek(Q1)),
     ?_assertEqual({0,0,1}, Req),
     ?_assertEqual([{0,0,1}], ?rqueue:to_list(Q1)),
     ?_assertEqual([], ?rqueue:to_list(Q2))].

head_check_test_() ->
    Q0 = ?rqueue:new(),
    Q1 = ?rqueue:push(1, 2, 3, Q0),
    [?_assertNot(?rqueue:is_head(1, 2, 3, Q0)),
     ?_assert(?rqueue:is_head(1, 2, 3, Q1)),
     ?_assertNot(?rqueue:is_head(1, 2, 2, Q1)),
     ?_assertNot(?rqueue:is_head(1, 2, 4, Q1)),
     ?_assert(?rqueue:has_offset(1, 2, Q1)),
     ?_assertNot(?rqueue:has_offset(0, 2, Q1)),
     ?_assertNot(?rqueue:has_offset(1, 1, Q1))].

low_check_test_() ->
    Q0 = ?rqueue:new(1, 3),
    Q1 = ?rqueue:push(0, 0, 1, Q0),
    Q2 = ?rqueue:push(0, 1, 1, Q1),
    [?_assert(?rqueue:is_low(Q0)),
     ?_assert(?rqueue:is_low(Q1)),
     ?_assertNot(?rqueue:is_low(Q2))].

needs_test_() ->
    Q0 = ?rqueue:new(1, 3),
    Q1 = ?rqueue:push(0, 0, 1, Q0),
    Q2 = ?rqueue:push(0, 1, 1, Q1),
    Q3 = ?rqueue:push(0, 2, 1, Q2),
    Q4 = ?rqueue:push(0, 3, 1, Q3),
    [?_assertEqual(3, ?rqueue:needs(Q0)),
     ?_assertEqual(2, ?rqueue:needs(Q1)),
     ?_assertEqual(1, ?rqueue:needs(Q2)),
     ?_assertEqual(0, ?rqueue:needs(Q3)),
     ?_assertEqual(0, ?rqueue:needs(Q4))].

push_list_test_() ->
    Q0 = ?rqueue:new(),
    Q1 = ?rqueue:push(0, 0, 1, Q0),
    Q2 = ?rqueue:push([{0,1,1},{0,2,1}], Q1),
    {R0, OQ0} = ?rqueue:pop(Q2),
    {R1, OQ1} = ?rqueue:pop(OQ0),
    {R2, OQ2} = ?rqueue:pop(OQ1),
    [?_assertEqual({0,0,1}, R0),
     ?_assertEqual({0,1,1}, R1),
     ?_assertEqual({0,2,1}, R2),
     ?_assertEqual([{0,0,1},{0,1,1},{0,2,1}], ?rqueue:to_list(Q2)),
     ?_assertEqual([], ?rqueue:to_list(OQ2))].



-endif.

