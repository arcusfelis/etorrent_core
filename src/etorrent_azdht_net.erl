%% @author Magnus Klaar <magnus.klaar@sgsstudentbostader.se>
%% @doc TODO
%% @end
-module(etorrent_azdht_net).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

%
% Implementation notes
%     RPC calls to remote nodes in the DHT are exposed to
%     clients using the gen_server call mechanism. In order
%     to make this work the client reference passed to the
%     handle_call/3 callback is stored in the server state.
%
%     When a response is received from the remote node, the
%     source IP and port combined with the message id is used
%     to map the response to the correct client reference.
%
%     A timer is used to notify the server of requests that
%     time out, if a request times out {error, timeout} is
%     returned to the client. If a response is received after
%     the timer has fired, the response is dropped.
%
%     The expected behavior is that the high-level timeout fires
%     before the gen_server call times out, therefore this interval
%     should be shorter then the interval used by gen_server calls.
%
%     The find_node_search/1 and get_peers_search/1 functions
%     are almost identical, they both recursively search for the
%     nodes closest to an id. The difference is that get_peers should
%     return as soon as it finds a node that acts as a tracker for
%     the infohash.

% Public interface
-export([start_link/2,
         node_port/0,
         contact/3,
         ping/1,
         find_node/2,
         find_value/2,
         find_node_search/1,
         find_node_search/2,
         get_peers/2,
         get_peers_search/1,
         get_peers_search/2,
         announce/4]).


-define(LONG_MSB, (1 bsl 63)).

%% Totally "magic" number.
-define(MAX_TRANSACTION_ID, 16#FFFFFF).
-define(MAX_UINT, 16#FFFFFFFF).

-type nodeinfo() :: etorrent_types:nodeinfo().
-type peerinfo() :: etorrent_types:peerinfo().
-type trackerinfo() :: etorrent_types:trackerinfo().
-type infohash() :: etorrent_types:infohash().
-type token() :: etorrent_types:token().
-type dht_qtype() :: etorrent_types:dht_qtype().
-type ipaddr() :: etorrent_types:ipaddr().
-type nodeid() :: etorrent_types:nodeid().
-type portnum() :: etorrent_types:portnum().
-type transaction() :: etorrent_types:transaction().

-type long()  :: non_neg_integer().
-type int()   :: non_neg_integer().
-type short() :: non_neg_integer().
%% -type byte() :: 0 .. 255.
-type address() :: {ipaddr(), portnum()}.
-type network_coordinates() :: term().
-type transaction_id() :: non_neg_integer().
-type instance_id() :: non_neg_integer().
-type contact() :: term().
-type contacts() :: [contact()].
-type position_version() :: atom().

-record(position, {
    x = 0 :: float(),
    y = 0 :: float(),
    z = 0 :: float(),
    error = 0 :: float(),
    type :: position_version()
}).

-type position() :: #position{}.
-type diversification_type() :: none | frequency | size.


-record(request_header, {
    %% Random number with most significant bit set to 1.
    connection_id :: long(),
    %% Type of the packet.
    action :: int(),
    %% Unique number used through the communication;
    %% it is randomly generated at the start of the application and
    %% increased by 1 with each sent packet.
    transaction_id :: int(),
    %% version of protocol used in this packet.
    protocol_version :: byte(),
    %% ID of the DHT implementator; 0 = Azureus, 1 = ShareNet, 255 = unknown
    %% ≥VENDOR_ID
    vendor_id = 0 :: byte(),
    %% ID of the network; 0 = stable version; 1 = CVS version
    %% ≥NETWORKS
    network_id = 0 :: int(),
    %% Maximum protocol version this node supports;
    %% if this packet's protocol version is <FIX_ORIGINATOR
    %% then the value is stored at the end of the packet
    %% ≥FIX_ORIGINATOR
    local_protocol_version :: byte(),
    %% Address of the local node
    node_address :: address(),
    %% Application's helper number; randomly generated at the start
    instance_id :: int(),     
    %% Time of the local node;
    %% stored as number of milliseconds since Epoch.
    time :: long()
}).

-record(reply_header, {
    %% Type of the packet.
    action :: int(),
    %% Must be equal to TRANSACTION_ID from the request.
    transaction_id :: int(),     
    %% must be equal to CONNECTION_ID from the request.
    connection_id :: long(),
    %% Version of protocol used in this packet.
    protocol_version :: byte(),
    %% Same meaning as in the request.
    %% ≥VENDOR_ID
    vendor_id = 0 :: byte(),
    %% Same meaning as in the request.
    %% ≥NETWORKS
    network_id = 0 :: int(),
    %% Instance id of the node that replies to the request.
    instance_id :: int()
}).

-record(find_node_request, {
    %% ID to search
    id :: binary(),
    %% Node status.
    %% ≥MORE_NODE_STATUS
    node_status = 0 :: int(), 
    %% Estimated size of the DHT; Unknown value can be indicated as zero.
    %% ≥MORE_NODE_STATUS
    dht_size = 0 :: int()
}).

-record(find_node_reply, {
    %% Spoof ID of the requesting node;
    %% it should be constructed from information known about
    %% requesting contact and not easily guessed by others.
    %% ≥ANTI_SPOOF
    spoof_id :: int(),
    %% Type of the replying node;
    %% Possible values are
    %% 0 for bootstrap node,
    %% 1 for ordinary node and ffffffffh for unknown type.
    %% ≥XFER_STATUS
    node_type :: int(),
    %% Estimated size of the DHT;
    %% Unknown value can be indicated as zero.
    %% ≥SIZE_ESTIMATE
    dht_size :: int(),
    %% Network coordinates of replying node.
    %% ≥VIVALDI
    network_coordinates :: network_coordinates(),
    %% Number of carried contacts.
    contacts_count :: short(),
    %% List with contacts. 
    contacts :: contacts()
}).

-record(ping_reply, {
    network_coordinates :: position()
}).

-record(find_value_request, {
    %% ID (key) to search.
    %% Key for which the values are requested.
    id :: binary(),
    %% Flags for the operation.
    flags = 0 :: byte(), 
    %% Maximum number of returned values. 
    max_values = 16 :: byte()
}).

-record(find_value_reply, {
    %% Indicates whether there is at least one other packet with values.
    %% protocol version ≥DIV_AND_CONT
    has_continuation :: boolean(), 
    %% Indicates whether this packet carries values or contacts.
    has_values  :: boolean(),
    %% Number of stored contacts.
    %% has_values == false
    contacts_count :: short(),
    %% Stored contacts that are close to the searched key.
    %% has_values == false
    contacts :: contacts(),
    %% Network coordinates of the replying node.
    %% HAS_VALUES == false && protocol version ≥VIVALDI_FINDVALUE
    network_coordinates ::  network_coordinates(),
    %% Type of key's diversification.
    %% HAS_VALUES == true && protocol version ≥DIV_AND_CONT
    diversification_type :: diversification_type(),
    %% Values that match searched key.
    %% HAS_VALUES == true
    values % value_group() 
}).

-record(transport_value, {
    created :: long(),
    value :: binary(),
    originator :: contacts(),
    flags :: byte(),
    life_hours :: byte() | undefined,
    replication_control :: byte() | undefined
}).

% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    socket :: inet:socket(),
    sent   :: gb_tree(),
    tokens :: queue(),

    node_address :: address(),
    next_transaction_id :: transaction_id(),
    instance_id :: instance_id()
}).

%
% Type definitions and function specifications
%


%
% Contacts and settings
%
srv_name() ->
   azdht_socket_server.

query_timeout() ->
    2000.

search_width() ->
    32.

search_retries() ->
    4.

socket_options() ->
    [list, inet, {active, true}, {mode, binary}].
%   ++ case etorrent_config:listen_ip() of all -> []; IP -> [{ip, IP}] end.


%
% Public interface
%
start_link(DHTPort, ExternalIP) ->
    gen_server:start_link({local, srv_name()}, ?MODULE, [DHTPort, ExternalIP], []).


-spec node_port() -> portnum().
node_port() ->
    gen_server:call(srv_name(), get_node_port).


contact(ProtoVer, IP, Port) ->
    {ProtoVer, {IP, Port}}.

%
%
%
-spec ping(contact()) -> pang | nodeid().
ping(Contact) ->
    case gen_server:call(srv_name(), {ping, Contact}) of
        timeout -> pang;
        Values -> decode_reply_body(ping, Values)
    end.

%
%
%
-spec find_node(contact(), nodeid()) ->
    {'error', 'timeout'} | {nodeid(), list(nodeinfo())}.
find_node(Contact, Target)  ->
    case gen_server:call(srv_name(), {find_node, Contact, Target}) of
        timeout ->
            {error, timeout};
        Values  ->
            decode_reply_body(find_node, Values)
            % TODO
%           etorrent_dht_state:log_request_success(ID, IP, Port),
    end.


find_value(Contact, Target) ->
    case gen_server:call(srv_name(), {find_value, Contact, Target}) of
        timeout ->
            {error, timeout};
        Values  ->
            decode_reply_body(find_value, Values)
    end.

%
%
%
-spec get_peers(contact(), infohash()) ->
    {nodeid(), token(), list(peerinfo()), list(nodeinfo())}.
get_peers(Contact, InfoHash) ->
    Call = {get_peers, Contact, InfoHash},
    case gen_server:call(srv_name(), Call) of
        timeout ->
            {error, timeout};
        Values ->
            decode_reply_body(get_peers, Values)
    end.

%
% Recursively search for the 100 nodes that are the closest to
% the local DHT node.
%
% Keep tabs on:
%     - Which nodes has been queried?
%     - Which nodes has responded?
%     - Which nodes has not been queried?
% FIXME: It returns `[{649262719799963483759422800960489108797112648079,{127,0,0,2},1743},{badrpc,{127,0,0,4},1763}]'.
-spec find_node_search(nodeid()) -> list(nodeinfo()).
find_node_search(NodeID) ->
    Width = search_width(),
    Retry = search_retries(),
    Nodes = etorrent_dht_state:closest_to(NodeID, Width),
    dht_iter_search(find_node, NodeID, Width, Retry, Nodes).

-spec find_node_search(nodeid(), list(nodeinfo())) -> list(nodeinfo()).
find_node_search(NodeID, Nodes) ->
    Width = search_width(),
    Retry = search_retries(),
    dht_iter_search(find_node, NodeID, Width, Retry, Nodes).

-spec get_peers_search(infohash()) ->
    {list(trackerinfo()), list(peerinfo()), list(nodeinfo())}.
get_peers_search(InfoHash) ->
    Width = search_width(),
    Retry = search_retries(),
    Nodes = etorrent_dht_state:closest_to(InfoHash, Width), 
    dht_iter_search(get_peers, InfoHash, Width, Retry, Nodes).

-spec get_peers_search(infohash(), list(nodeinfo())) ->
    {list(trackerinfo()), list(peerinfo()), list(nodeinfo())}.
get_peers_search(InfoHash, Nodes) ->
    Width = search_width(),
    Retry = search_retries(),
    dht_iter_search(get_peers, InfoHash, Width, Retry, Nodes).
    

dht_iter_search(SearchType, Target, Width, Retry, Nodes)  ->
    WithDist = [{etorrent_dht:distance(ID, Target), ID, IP, Port} || {ID, IP, Port} <- Nodes],
    dht_iter_search(SearchType, Target, Width, Retry, 0, WithDist,
                    gb_sets:empty(), gb_sets:empty(), []).

dht_iter_search(SearchType, _, _, Retry, Retry, _,
                _, Alive, WithPeers) ->
    TmpAlive  = gb_sets:to_list(Alive),
    AliveList = [{ID, IP, Port} || {_, ID, IP, Port} <- TmpAlive],
    case SearchType of
        find_node ->
            AliveList;
        get_peers ->
            Trackers = [{ID, IP, Port, Token}
                      ||{ID, IP, Port, Token, _} <- WithPeers],
            Peers = [Peer || {_, _, _, _, Peers} <- WithPeers,
                             Peer <- Peers],
            {Trackers, Peers, AliveList}
    end;

dht_iter_search(SearchType, Target, Width, Retry, Retries,
                Next, Queried, Alive, WithPeers) ->

    % Mark all nodes in the queue as queried
    AddQueried = [{ID, IP, Port} || {_, ID, IP, Port} <- Next],
    NewQueried = gb_sets:union(Queried, gb_sets:from_list(AddQueried)),

    ThisNode = node(),
    Callback =
    case SearchType of
        find_node ->
            fun({_,_,IP,Port}) ->
                rpc:async_call(ThisNode, ?MODULE, find_node,
                               [IP, Port, Target])
                end;
        get_peers ->
            fun({_,_,IP,Port}) ->
                rpc:async_call(ThisNode, ?MODULE, get_peers,
                               [IP, Port, Target])
                end
    end,
    % Query all nodes in the queue and generate a list of
    % {Dist, ID, IP, Port, Nodes} elements
    Promises = lists:map(Callback, Next),
    ReturnValues = lists:map(fun rpc:yield/1, Promises),
    WithArgs = lists:zip(Next, ReturnValues),

    FailedCall = make_ref(),
    TmpSuccessful = [case {repack, SearchType, RetVal} of
        {repack, _, {badrpc, Reason}} ->
%           lager:error("A RPC process crashed while sending a request ~p "
%                       "to ~p:~p with reason ~p.",
%                       [SearchType, IP, Port, Reason]),
            FailedCall;
        {repack, _, {error, timeout}} ->
            FailedCall;
        {repack, _, {error, response}} ->
            FailedCall;
        {repack, find_node, {NID, Nodes}} ->
            {{Dist, NID, IP, Port}, Nodes};
        {repack, get_peers, {NID, Token, Peers, Nodes}} ->
            {{Dist, NID, IP, Port}, {Token, Peers, Nodes}}
    end || {{Dist, _ID, IP, Port}, RetVal} <- WithArgs],
    Successful = [E || E <- TmpSuccessful, E =/= FailedCall],

    % Mark all nodes that responded as alive
    AddAlive = [N ||{{_, _, _, _}=N, _} <- Successful],
    NewAlive = gb_sets:union(Alive, gb_sets:from_list(AddAlive)),

    % Accumulate all nodes from the successful responses.
    % Calculate the relative distance to all of these nodes
    % and keep the closest nodes which has not already been
    % queried in a previous iteration
    NodeLists = [case {acc_nodes, {SearchType, Res}} of
        {acc_nodes, {find_node, Nodes}} ->
            Nodes;
        {acc_nodes, {get_peers, {_, _, Nodes}}} ->
            Nodes
    end || {_, Res} <- Successful],
    AllNodes  = lists:flatten(NodeLists),
    NewNodes  = [Node || Node <- AllNodes,
                         not gb_sets:is_member(Node, NewQueried)],
    NewNext   = [{etorrent_dht:distance(ID, Target), ID, IP, Port}
                ||{ID, IP, Port} <- etorrent_dht:closest_to(Target, NewNodes, Width)],

    % Check if the closest node in the work queue is closer
    % to the target than the closest responsive node that was
    % found in this iteration.
    MinAliveDist = case gb_sets:size(NewAlive) of
        0 ->
            infinity;
        _ ->
            {IMinAliveDist, _, _, _} = gb_sets:smallest(NewAlive),
            IMinAliveDist
    end,

    MinQueueDist = case NewNext of
        [] ->
            infinity;
        Other ->
            {MinDist, _, _, _} = lists:min(Other),
            MinDist
    end,

    % Check if the closest node in the work queue is closer
    % to the infohash than the closest responsive node.
    NewRetries = if
        (MinQueueDist <  MinAliveDist) -> 0;
        (MinQueueDist >= MinAliveDist) -> Retries + 1
    end,

    % Accumulate the trackers and peers found if this is a get_peers search.
    NewWithPeers = case SearchType of
        find_node -> []=WithPeers;
        get_peers ->
            Tmp=[{ID, IP, Port, Token, Peers}
                || {{_, ID, IP, Port},
                    {Token, Peers, _}} <- Successful,
                   Peers > []],
            WithPeers ++ Tmp
    end,

    NewNext2 = lists:usort(NewNext),
    dht_iter_search(SearchType, Target, Width, Retry, NewRetries,
                    NewNext2, NewQueried, NewAlive, NewWithPeers).


%
%
%
-spec announce(contact(), infohash(), token(), portnum()) ->
    {'error', 'timeout'} | nodeid().
announce(Contact, InfoHash, Token, BTPort) ->
    Announce = {announce, Contact, InfoHash, Token, BTPort},
    case gen_server:call(srv_name(), Announce) of
        timeout -> {error, timeout};
        Values -> decode_reply_body(announce, Values)
    end.

%% ==================================================================

init([DHTPort, ExternalIP]) ->
    {ok, Socket} = gen_udp:open(DHTPort, socket_options()),
    State = #state{socket=Socket,
                   sent=gb_trees:empty(),
                   node_address={ExternalIP, DHTPort},
                   next_transaction_id=new_transaction_id(),
                   instance_id=new_instance_id()},
    {ok, State}.

handle_call({ping, Contact}, From, State) ->
    Action = ping,
    Args = undefined,
    do_send_query(Action, Args, Contact, From, State);

handle_call({find_node, Contact, Target}, From, State) ->
    Action = find_node,
    Args = #find_node_request{id=Target},
    do_send_query(Action, Args, Contact, From, State);

handle_call({find_value, Contact, Key}, From, State) ->
    Action = find_value,
    Args = #find_value_request{id=Key},
    do_send_query(Action, Args, Contact, From, State);

handle_call({get_peers, Contact, InfoHash}, From, State) ->
    Action = get_peers,
    Args = InfoHash,
    do_send_query(Action, Args, Contact, From, State);

handle_call({announce, Contact, InfoHash, Token, BTPort}, From, State) ->
    Action = announce,
    Args = {InfoHash, Token, BTPort},
    do_send_query(Action, Args, Contact, From, State);

handle_call(get_node_port, _From, State) ->
    #state{
        socket=Socket} = State,
    {ok, {_, Port}} = inet:sockname(Socket),
    {reply, Port, State}.

handle_cast(not_implemented, State) ->
    {noreply, State}.

handle_info({timeout, _, IP, Port, ID}, State) ->
    #state{sent=Sent} = State,

    NewState = case find_sent_query(IP, Port, ID, Sent) of
        error ->
            State;
        {ok, {Client, _Timeout, Action}} ->
            _ = gen_server:reply(Client, timeout),
            NewSent = clear_sent_query(IP, Port, ID, Sent),
            State#state{sent=NewSent}
    end,
    {noreply, NewState};

handle_info({udp, _Socket, IP, Port, Packet}, State) ->
    #state{sent=Sent} = State,
    io:format(user, "Receiving a packet from ~p:~p~n", [IP, Port]),
    io:format(user, "~p~n", [Packet]),
    NewState =
    case packet_type(Packet) of
        request ->
            spawn_link(fun() ->
%                       etorrent_dht_state:safe_insert_node(IP, Port),
                        ok
                end),
            spawn_link(fun() ->
                        handle_request_packet(Packet),
                        ok
                end),
            State;
        reply ->
            {ReplyHeader, Body} = decode_reply_header(Packet),
            #reply_header{action=ActionNum,
                          connection_id=ConnId,
                          protocol_version=Version} = ReplyHeader,
            Action = action_name(ActionNum),
            io:format(user, "Reply ~ts~n", [pretty(ReplyHeader)]),

            case find_sent_query(IP, Port, ConnId, Sent) of
                error ->
                    State;
                {ok, {Client, Timeout, Action}} ->
                    _ = cancel_timeout(Timeout),
                    _ = gen_server:reply(Client, {Action, Version, Body}),
                    NewSent = clear_sent_query(IP, Port, ConnId, Sent),
                    State#state{sent=NewSent}
            end
    end,
    {noreply, NewState};

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_, _State) ->
    ok.

code_change(_, _, State) ->
    {ok, State}.

%% ==================================================================

handle_request_packet(Packet) ->
    {RequestHeader, Body} = decode_request_header(Packet),
    #request_header{
        action=ActionNum,
        transaction_id=TranId,
        connection_id=ConnId,
        protocol_version=Version
    } = RequestHeader,
    io:format("Decoded header: ~ts~n", [pretty(RequestHeader)]),
    io:format("Body: ~p~n", [Body]),
    Action = action_name(ActionNum),
    {RequestBody, _} = decode_request_body(Action, Version, Body),
    io:format("Decoded body: ~ts~n", [pretty(RequestBody)]),

    ok.

do_send_query(Action, Args, {Version, {IP, Port}}, From, State) ->
    #state{sent=Sent,
           socket=Socket} = State,
    #state{sent=Sent,
           socket=Socket,
           node_address=NodeAddress,
           next_transaction_id=TranId,
           instance_id=InstanceId} = State,
    ConnId = unique_connection_id(IP, Port, Sent),
%   min(proto_version_num(supported), Version),
    ActionNum = action_request_num(Action),
    [erlang:error({bad_action, Action, ActionNum})
     || not is_integer(ActionNum)],
    RequestHeader = #request_header{
        action=ActionNum,
        connection_id=ConnId,
        transaction_id=TranId,
        instance_id=InstanceId,
        local_protocol_version=proto_version_num(supported),
        node_address=NodeAddress,
        protocol_version=Version,
        time=milliseconds_since_epoch()
    },
    Request = [encode_request_header(RequestHeader)
              |encode_request_body(Action, Version, Args)],

    case gen_udp:send(Socket, IP, Port, Request) of
        ok ->
            TRef = timeout_reference(IP, Port, ConnId),
%           lager:info("Sent ~w to ~w:~w", [Action, IP, Port]),

            NewSent = store_sent_query(IP, Port, ConnId, From, TRef, Action, Sent),
            NewState = State#state{
                    sent=NewSent,
                    next_transaction_id=next_transaction_id(TranId)},
            {noreply, NewState};
        {error, einval} ->
%           lager:error("Error (einval) when sending ~w to ~w:~w",
%                       [Action, IP, Port]),
            {reply, timeout, State};
        {error, eagain} ->
%           lager:error("Error (eagain) when sending ~w to ~w:~w",
%                       [Action, IP, Port]),
            {reply, timeout, State}
    end.


unique_connection_id(IP, Port, Open) ->
    ConnId = new_connection_id(),
    IsLocal  = gb_trees:is_defined(tkey(IP, Port, ConnId), Open),
    if IsLocal -> unique_connection_id(IP, Port, Open);
       true    -> ConnId
    end.

store_sent_query(IP, Port, ID, Client, Timeout, Action, Open) ->
    K = tkey(IP, Port, ID),
    V = tval(Client, Timeout, Action),
    gb_trees:insert(K, V, Open).

find_sent_query(IP, Port, ID, Open) ->
    case gb_trees:lookup(tkey(IP, Port, ID), Open) of
       none -> error;
       {value, Value} -> {ok, Value}
    end.

clear_sent_query(IP, Port, ID, Open) ->
    gb_trees:delete(tkey(IP, Port, ID), Open).

tkey(IP, Port, ID) ->
   {IP, Port, ID}.

tval(Client, TimeoutRef, Action) ->
    {Client, TimeoutRef, Action}.

timeout_reference(IP, Port, ID) ->
    Msg = {timeout, self(), IP, Port, ID},
    erlang:send_after(query_timeout(), self(), Msg).

cancel_timeout(TimeoutRef) ->
    erlang:cancel_timer(TimeoutRef).


%% ==================================================================
%% Serialization

decode_byte(<<H, T/binary>>) -> {H, T}.
decode_short(<<H:16/big-integer, T/binary>>) -> {H, T}.
decode_int(<<H:32/big-integer, T/binary>>) -> {H, T}.
decode_long(<<H:64/big-integer, T/binary>>) -> {H, T}.
decode_connection_id(<<1:1, H:63/big-integer, T/binary>>) -> {H, T}.
decode_none(Bin) -> {undefined, Bin}.
decode_float(<<H:32/big-float, T/binary>>) -> {H, T}.
%% transport/udp/impl/DHTUDPUtils.java:    deserialiseVivaldi
decode_network_coordinates(<<EntriesCount, Bin/binary>>) ->
    decode_network_coordinate_n(Bin, EntriesCount, []).

decode_network_coordinate_n(Bin, 0, Acc) ->
    {lists:reverse(Acc), Bin};
decode_network_coordinate_n(Bin, Left, Acc) ->
    {Pos, Bin1} = decode_network_position(Bin),
    decode_network_coordinate_n(Bin1, Left-1, [Pos|Acc]).

decode_network_position(<<Type, Size, Body:Size/binary, Bin/binary>>) ->
    {decode_network_position_1(Type, Body), Bin}.

decode_network_position_1(0, _) ->
    #position{type=none};
decode_network_position_1(1, Bin) ->
    {X, Bin1} = decode_float(Bin),
    {Y, Bin2} = decode_float(Bin1),
    {Z, Bin3} = decode_float(Bin2),
    {E, <<>>} = decode_float(Bin3),
    #position{type=vivaldi_v1, x=X, y=Y, z=Z, error=E};
decode_network_position_1(_, _) ->
    #position{type=unknown}.

decode_boolean(<<0, T/binary>>) -> {false, T};
decode_boolean(<<1, T/binary>>) -> {true, T}.

decode_sized_binary(<<Len, H:Len/binary, T/binary>>) ->
    {H, T}.

%% First byte indicates length of the IP address (4 for IPv4, 16 for IPv6);
%% next comes the address in network byte order;
%% the last value is port number as short
decode_address(<<4, A, B, C, D, Port:16/big-integer, T/binary>>) ->
    {{{A,B,C,D}, Port}, T}.


%% First byte indicates contact type, which must be UDP (1);
%% second byte indicates the contact's protocol version;
%% the rest is an address.
decode_contact(<<1, ProtoVer, T/binary>>) ->
    {Address, T1} = decode_address(T),
    {{ProtoVer, Address}, T1}.


decode_contacts(Bin) ->
    {ContactsCount, Bin1} = decode_short(Bin),
    decode_contacts_n(Bin1, ContactsCount, []).

decode_contacts_n(Bin, 0, Acc) ->
    {lists:reverse(Acc), Bin};
decode_contacts_n(Bin, Left, Acc) ->
    {Contact, Bin1} = decode_contact(Bin),
    decode_contacts_n(Bin1, Left-1, [Contact|Acc]).


%% transport/udp/impl/DHTUDPUtils.deserialiseTransportValues
decode_value_group(Bin, Version) ->
    {ValueCount, Bin1} = decode_value(Bin, Version),
    decode_values_n(Bin1, Version, ValueCount, []).

decode_values_n(Bin, _Version, 0, Acc) ->
    {lists:reverse(Acc), Bin};
decode_values_n(Bin, Version, Left, Acc) ->
    {Value, Bin1} = decode_value(Version, Bin),
    decode_values_n(Bin1, Version, Left-1, [Value|Acc]).

decode_value(PacketVersion, Bin) ->
    {Version, Bin1} =
    case higher_or_equal_version(PacketVersion, remove_dist_add_ver) of
        true  -> decode_int(Bin);
        false -> decode_none(Bin)
    end,
    %% final long  created     = is.readLong() + skew;
    {Created, Bin2} = decode_long(Bin1),
    {Value, Bin3} = decode_sized_binary(Bin2),
    {Originator, Bin4} = decode_contact(Bin3),
    {Flags, Bin5} = decode_byte(Bin4),
    {LifeHours, Bin6} =
    case higher_or_equal_version(PacketVersion, longer_life) of
        true  -> decode_byte(Bin5);
        false -> decode_none(Bin5)
    end,
    {RepControl, Bin7} =
    case higher_or_equal_version(PacketVersion, replication_control) of
        true  -> decode_byte(Bin6);
        false -> decode_none(Bin6)
    end,
    ValueRec = #transport_value{
        created = Created,
        value = Value,
        originator = Originator,
        flags = Flags,
        life_hours = LifeHours,
        replication_control = RepControl},
    {ValueRec, Bin7}.


encode_byte(X)  when is_integer(X) -> <<X>>.
encode_short(X) when is_integer(X) -> <<X:16/big-integer>>.
encode_int(X)   when is_integer(X) -> <<X:32/big-integer>>.
encode_long(X)  when is_integer(X) -> <<X:64/big-integer>>.

encode_boolean(true)  -> <<1>>;
encode_boolean(false) -> <<0>>.

%% First byte indicates length of the IP address (4 for IPv4, 16 for IPv6);
%% next comes the address in network byte order;
%% the last value is port number as short
encode_address({{A,B,C,D}, Port}) ->
    <<4, A, B, C, D, Port:16/big-integer>>.


%% First byte indicates contact type, which must be UDP (1);
%% second byte indicates the contact's protocol version;
%% the rest is an address.
encode_contact({ProtoVer, Address}) ->
    <<1, ProtoVer, (encode_address(Address))/binary>>.

encode_sized_binary(ID) when is_binary(ID) ->
    [encode_byte(byte_size(ID)), ID].

encode_request_header(#request_header{
        connection_id=ConnId,
        action=Action,
        transaction_id=TranId,
        protocol_version=ProtoVer,
        vendor_id=VendorId,
        network_id=NetworkId,
        local_protocol_version=LocalProtoVer,
        node_address=NodeAddress,
        instance_id=InstanceId,
        time=Time}) ->
    [encode_long(ConnId),
     encode_int(Action),
     encode_int(TranId),
     encode_byte(ProtoVer),
     encode_byte(VendorId),
     encode_int(NetworkId),
     encode_byte(LocalProtoVer),
     encode_address(NodeAddress),
     encode_int(InstanceId),
     encode_long(Time)
    ].

encode_reply_header(#reply_header{
        action=Action,
        transaction_id=TranId,
        connection_id=ConnId,
        protocol_version=ProtoVer,
        vendor_id=VendorId,
        network_id=NetworkId,
        instance_id=InstanceId
    }) ->
    [encode_int(Action),
     encode_int(TranId),
     encode_long(ConnId),
     encode_byte(ProtoVer),
     encode_byte(VendorId),
     encode_int(NetworkId),
     encode_int(InstanceId)
    ].


encode_request_body(find_node, Version, #find_node_request{
                id=ID,
                node_status=NodeStatus,
                dht_size=DhtSize
                }) ->
    [encode_sized_binary(ID),
     [[encode_int(NodeStatus), encode_int(DhtSize)]
     || higher_or_equal_version(Version, more_node_status)]
    ];
encode_request_body(find_value, Version, #find_value_request{
                id=ID,
                flags=Flags,
                max_values=MaxValues}) ->
    [encode_sized_binary(ID), encode_byte(Flags), encode_byte(MaxValues)];
encode_request_body(ping, Version, _) ->
    [].

encode_reply_body(find_value, Version, _) ->
    [];
encode_reply_body(ping, Version, _) ->
    [].


decode_request_body(ping, Version, Bin) ->
    {ping, Bin};
decode_request_body(find_node, Version, Bin) ->
    {ID, Bin1} = decode_sized_binary(Bin),
    {NodeStatus, Bin2} =
    case higher_or_equal_version(Version, more_node_status) of
        true -> decode_int(Bin1);
        false -> decode_none(Bin1)
    end,
    {DhtSize, Bin3} =
    case higher_or_equal_version(Version, more_node_status) of
        true -> decode_int(Bin2);
        false -> decode_none(Bin2)
    end,
    Request = #find_node_request{
        id=ID,
        node_status=NodeStatus,
        dht_size=DhtSize
    },
    {Request, Bin3};
decode_request_body(Action, Version, Bin) ->
    {unknown, Bin}.

decode_reply_body(ExpectedAction, {ReceivedAction, Version, Bin}) ->
    {error, {ExpectedAction, ReceivedAction}};
decode_reply_body(Action, {Action, Version, Bin}) ->
    decode_reply_body(Action, Version, Bin).

decode_reply_body(find_node, Version, Bin) ->
    {SpoofId, Bin1} =
    case higher_or_equal_version(Version, anti_spoof) of
        true -> decode_int(Bin);
        false -> decode_none(Bin)
    end,
    {NodeType, Bin2} =
    case higher_or_equal_version(Version, xfer_status) of
        true -> decode_int(Bin1);
        false -> decode_none(Bin1)
    end,
    {DhtSize, Bin3} =
    case higher_or_equal_version(Version, size_estimate) of
        true -> decode_int(Bin2);
        false -> decode_none(Bin2)
    end,
    {NetworkCoordinates, Bin4} =
    case higher_or_equal_version(Version, vivaldi) of
        true -> decode_network_coordinates(Bin3);
        false -> decode_none(Bin3)
    end,
    {Contacts, Bin5} = decode_contacts(Bin4),
    Reply = #find_node_reply{
        spoof_id=SpoofId,
        node_type=NodeType,
        dht_size=DhtSize,
        network_coordinates=NetworkCoordinates,
        contacts=Contacts
        },
    {Reply, Bin5};
decode_reply_body(find_value, Version, Bin) ->
    {HasContinuation, Bin1} =
    case higher_or_equal_version(Version, div_and_cont) of
        true -> decode_boolean(Bin);
        flase -> {false, Bin}
    end,
    {HasValues, Bin2} = decode_boolean(Bin1),
    case HasValues of
        true ->
            %% Decode values.
            {DivType, BinV1} =
            case higher_or_equal_version(Version, div_and_cont) of
                 true -> diversification_type(decode_byte(Bin2));
                 false -> {none, Bin2}
            end,
            {Values, _} = decode_value_group(BinV1, Version),
            Reply = #find_value_reply{
                has_continuation=HasContinuation,
                has_values=HasValues,
                diversification_type=DivType,
                values=Values},
            {Reply, BinV1};
        false ->
            {Contacts, BinC1} = decode_contacts(Bin2),
            {NetworkCoordinates, BinC2} =
            case higher_or_equal_version(Version, vivaldi) of
                true -> decode_network_coordinates(BinC1);
                false -> decode_none(BinC1)
            end,
            Reply = #find_value_reply{
                has_continuation=HasContinuation,
                has_values=HasValues,
                contacts=Contacts,
                network_coordinates=NetworkCoordinates},
            {Reply, BinC2}
    end;
decode_reply_body(ping, Version, Bin) ->
    {NetworkCoordinates, Bin1} =
    case higher_or_equal_version(Version, vivaldi) of
        true -> decode_network_coordinates(Bin);
        false -> decode_none(Bin)
    end,
    Reply = #ping_reply{
            network_coordinates=NetworkCoordinates
            },
    {Reply, Bin1}.


decode_request_header(Bin) ->
    {ConnId,  Bin1} = decode_long(Bin),
    {Action,  Bin2} = decode_int(Bin1),
    {TranId,  Bin3} = decode_int(Bin2),
    {Version, Bin4} = decode_byte(Bin3),
    {VendorId, Bin5} =
    case higher_or_equal_version(Version, vendor_id) of
        true -> decode_byte(Bin4);
        false -> decode_none(Bin4)
    end,
    {NetworkId, Bin6} =
    case higher_or_equal_version(Version, networks) of
        true -> decode_int(Bin5);
        false -> decode_none(Bin5)
    end,
    {LocalProtoVer, Bin7} =
    case higher_or_equal_version(Version, fix_originator) of
        true -> decode_byte(Bin6);
        false -> decode_none(Bin6)
    end,
    {NodeAddress,    Bin8} = decode_address(Bin7),
    {InstanceId,     Bin9} = decode_int(Bin8),
    {Time,           BinA} = decode_long(Bin9),
    {LocalProtoVer1, BinB} =
    case lower_version(Version, fix_originator) of
        true -> decode_byte(BinA);
        false -> {LocalProtoVer, BinA}
    end,
    Header = #request_header{
        connection_id=ConnId,
        action=Action,
        transaction_id=TranId,
        protocol_version=Version,
        vendor_id=VendorId,
        network_id=NetworkId,
        local_protocol_version=LocalProtoVer1,
        node_address=NodeAddress,
        instance_id=InstanceId,
        time=Time},
    {Header, BinB}.

decode_reply_header(Bin) ->
    {Action,   Bin1} = decode_int(Bin),
    {TranId,   Bin2} = decode_int(Bin1),
    {ConnId,   Bin3} = decode_long(Bin2),
    {Version,  Bin4} = decode_byte(Bin3),
    {VendorId, Bin5} =
    case higher_or_equal_version(Version, vendor_id) of
        true -> decode_byte(Bin4);
        false -> decode_none(Bin4)
    end,
    {NetworkId, Bin6} =
    case higher_or_equal_version(Version, networks) of
        true -> decode_int(Bin5);
        false -> decode_none(Bin5)
    end,
    {InstanceId, Bin7} = decode_int(Bin6),
    Header = #reply_header{
        action=Action,
        transaction_id=TranId,
        connection_id=ConnId,
        protocol_version=Version,
        vendor_id=VendorId,
        network_id=NetworkId,
        instance_id=InstanceId
    },
    {Header, Bin7}.

new_connection_id() ->
    ?LONG_MSB bor crypto:rand_uniform(0, ?LONG_MSB).

new_instance_id() ->
    crypto:rand_uniform(0, ?MAX_UINT+1).

%% Init a transaction counter.
new_transaction_id() ->
    crypto:rand_uniform(0, ?MAX_TRANSACTION_ID).

next_transaction_id(TranId) -> TranId + 1.


packet_type(<<1:1, _/bitstring>>) -> request;
packet_type(<<0:1, _/bitstring>>) -> reply.


higher_or_equal_version(VersionName1, VersionName2) ->
    proto_version_num(VersionName1) >=
    proto_version_num(VersionName2).

lower_version(VersionName1, VersionName2) ->
    proto_version_num(VersionName1) <
    proto_version_num(VersionName2).

proto_version_num(VersionNum) when is_integer(VersionNum) ->
    VersionNum;
proto_version_num(VersionName) ->
    case VersionName of
    div_and_cont         -> 6;
    anti_spoof           -> 7;
    anti_spoof2          -> 8;
    fix_originator       -> 9;
    networks             -> 9;
    vivaldi              -> 10;
    remove_dist_add_ver  -> 11;
    xfer_status          -> 12;
    size_estimate        -> 13;
    vendor_id            -> 14;
    block_keys           -> 14;
    generic_netpos       -> 15;
    vivaldi_findvalue    -> 16;
    anon_values          -> 17;
    cvs_fix_overload_v1  -> 18;
    cvs_fix_overload_v2  -> 19;
    more_stats           -> 20;
    cvs_fix_overload_v3  -> 21;
    more_node_status     -> 22;
    longer_life          -> 23;
    replication_control  -> 24;
    restrict_id_ports    -> 32;
    restrict_id_ports2   -> 33;
    restrict_id_ports2x  -> 34;
    restrict_id_ports2y  -> 35;
    restrict_id_ports2z  -> 36;
    restrict_id3         -> 50;
    minimum_acceptable   -> 16;
    supported            -> 51
    end.


action_request_num(ActionName) when is_atom(ActionName) ->
    case ActionName of
        ping       -> 1024;
        find_node  -> 1028;
        find_value -> 1030;
        _          -> undefined
    end.

action_reply_num(ActionName) when is_atom(ActionName) ->
    case ActionName of
        ping       -> 1025;
        find_node  -> 1029;
        find_value -> 1031;
        _          -> undefined
    end.

action_name(ActionNum) when is_integer(ActionNum) ->
    case ActionNum of
        1024 -> ping;
        1025 -> ping;
        1028 -> find_node;
        1029 -> find_node;
        1030 -> find_value;
        1031 -> find_value;
        _    -> undefined
    end.

diversification_type(1) -> none;
diversification_type(2) -> frequency;
diversification_type(3) -> size.

milliseconds_since_epoch() ->
    {MegaSeconds, Seconds, MicroSeconds} = os:timestamp(),
    MegaSeconds * 1000000000 + Seconds * 1000 + MicroSeconds div 1000.

-ifdef(TEST).

decode_request_header_test_() ->
    Decoded = #request_header{
        connection_id=17154702304824391947,
        action=1024,
        transaction_id=4192055156,
        protocol_version=26,
        vendor_id=0,
        network_id=0,
        local_protocol_version=26,
        node_address={{2,94,163,239},7000},
        instance_id=1993199759,
        time=1370016047962},
    Encoded = <<238,17,190,219,82,161,249,11, %% connection_id
                0,0,4,0, %% action
                249,221,175,116, %% transaction_id
                26,  0, %% protocol_version, vendor_id
                0,0,0,0,  26, %% network_id, local_protocol_version
                4, 2,94,163,239, 27,88, %% node_address
                118,205,208,143, %% instance_id
                0,0,1,62,251,81,227,90>>, %% time
    [?_assertEqual({Decoded, <<>>},
                   decode_request_header(Encoded))
    ,?_assertEqual(Encoded,
                   iolist_to_binary(encode_request_header(Decoded)))
    ].

decode_find_node_reply_v50_test() ->
    Encoded = 
<<0,0,4,5,0,23,91,158,133,10,54,16,79,15,21,209,26,0,0,0,0,0,163,63,81,111,0,0,
  0,0,0,0,0,0,0,15,89,82,
  %% Vivaldi (Count=1, Type=1, Size=16)
  1,1,16,66,172,48,247,193,222,165,78,66,138,147,38,64,117,5,250,
  0,20,1,50,4,136,169,240,183,187,38,1,50,4,178,126,109,151,50,87,1,
  51,4,180,194,225,103,44,246,1,51,4,212,187,99,32,240,105,1,51,4,71,203,192,
  83,79,9,1,50,4,176,32,156,176,213,111,1,50,4,74,100,191,149,26,225,1,50,4,74,
  115,1,238,207,167,1,50,4,178,185,63,108,136,81,1,50,4,159,146,164,62,196,162,
  1,51,4,78,12,77,131,192,1,1,51,4,66,68,151,141,145,67,1,51,4,82,224,238,96,
  127,55,1,50,4,178,140,190,197,97,51,1,50,4,82,140,224,208,195,32,1,50,4,217,
  118,81,11,236,127,1,51,4,2,121,14,147,108,54,1,50,4,109,162,3,144,157,145,1,
  51,4,74,128,154,253,100,76,1,50,4,2,92,237,112,44,19>>,
    {ReplyHeader, EncodedBody} = decode_reply_header(Encoded),
    decode_reply_body(find_node, 50, EncodedBody).

decode_ping_reply_v50_test() ->
    Encoded = 
<<0,0,4,8,0,236,211,236,221,101,78,233,38,92,44,150,26,0,0,0,0,0,142,62,187,89,
  0,0,0,1,4,2,93,75,156,26,10>>,
    {ReplyHeader, EncodedBody} = decode_reply_header(Encoded),
    decode_reply_body(ping, 50, EncodedBody).

decode_find_value_reply_v50_test() ->
    Encoded = 
<<0,0,4,7,0,10,59,231,236,78,46,77,80,232,184,143,50,0,0,0,0,0,202,9,186,151,0,
  0,0,20,1,51,4,176,205,120,124,102,46,1,50,4,86,183,19,22,237,207,1,51,4,109,
  11,140,246,234,96,1,51,4,24,72,68,22,215,46,1,50,4,178,47,116,49,197,24,1,51,
  4,109,65,167,187,253,198,1,51,4,78,226,84,17,172,217,1,51,4,123,194,241,201,
  59,6,1,51,4,90,210,133,94,158,139,1,51,4,89,141,28,134,227,11,1,50,4,95,28,
  215,202,81,50,1,50,4,108,16,231,161,203,207,1,51,4,112,209,137,10,174,101,1,
  51,4,101,162,163,22,69,26,1,50,4,72,9,31,224,26,225,1,51,4,81,57,81,147,121,
  152,1,50,4,89,235,246,223,231,230,1,51,4,80,230,5,103,73,111,1,51,4,123,243,
  133,26,100,215,1,50,4,178,185,42,250,248,88,1,1,16,65,227,179,146,66,151,181,
  220,66,129,151,217,62,99,194,145>>,
    {ReplyHeader, EncodedBody} = decode_reply_header(Encoded),
    ReplyBody = decode_reply_body(find_value, 50, EncodedBody),
    io:format(user, "ReplyBody ~p~n", [ReplyBody]),
    ok.

decode_error_reply_v50_test() ->
    Encoded = <<0,0,4,8,0,130,225,204,154,253,215,52,255,72,14,158,50,0,0,0,
                0,0,202,9,186,151,0,0,0,1,4,2,93,190,244,27,88>>,
    {ReplyHeader, EncodedBody} = decode_reply_header(Encoded),
    ReplyBody = decode_reply_body(error, 50, EncodedBody),
    io:format(user, "ReplyBody ~p~n", [ReplyBody]),
    ok.


decode_network_coordinates_test_() ->
    [?_assertEqual(decode_network_coordinates(<<0,0,0,1,4,2,93,75,156,26,10>>),
                   {[], <<0,0,1,4,2,93,75,156,26,10>>})
    ,?_assertEqual(decode_network_coordinates(<<1,1,16,66,172,48,247,193,222,
                                         165,78,66,138,147,38,64,117,5,250>>),
                   {[#position{type = vivaldi_v1,
                               x = 86.09563446044922,
                               y = -27.83071517944336,
                               z = 69.28739929199219,
                               error = 3.8284897804260254}], <<>>})
    ].

-endif.


%% ======================================================================
%% Helpers for debugging.

pretty(Term) ->
    io_lib_pretty:print(Term, fun record_definition/2).

record_definition(Name, FieldCount) ->
%   io:format(user, "record_definition(~p, ~p)~n", [Name, FieldCount]),
%   io:format(user, "record_definition_list() = ~p~n", [record_definition_list()]),
    record_definition_1(Name, FieldCount+1, record_definition_list()).

record_definition_1(Name, Size, [{Name, Size, Fields}|_]) ->
    Fields;
record_definition_1(Name, Size, [{_, _, _}|T]) ->
    record_definition_1(Name, Size, T);
record_definition_1(_Name, _Size, []) ->
    no.


-define(REC_DEF(Name),
        {Name, record_info(size, Name), record_info(fields, Name)}).

record_definition_list() ->
    [?REC_DEF(find_node_reply)
    ,?REC_DEF(find_node_request)
    ,?REC_DEF(request_header)
    ,?REC_DEF(reply_header)
    ].
