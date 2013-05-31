%% @doc AZDHT.
%% @end
-module(etorrent_vuze_dht_net).
-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

% Public interface
-export([start_link/1,
         ping/2,
         find_node/3,
         get_peers/3,
         announce/5]).

% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

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

-type long() :: non_neg_integer().
-type int()  :: non_neg_integer().
%% -type byte() :: 0 .. 255.
-type address() :: {ipaddr(), portnum()}.
-type transaction_id() :: non_neg_integer().
-type instance_id() :: non_neg_integer().

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
    %% Time of the local node; stored as number of milliseconds since Epoch.
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
    

-record(state, {
    socket :: inet:socket(),
    node_address :: address(),
    next_transaction_id :: transaction_id(),
    instance_id :: instance_id()
}).

%
% Type definitions and function specifications
%


%
% Contansts and settings
%
srv_name() ->
   vuze_dht_socket_server.

socket_options() ->
    [list, inet, {active, true}, {mode, binary}].
%   ++ case etorrent_config:listen_ip() of all -> []; IP -> [{ip, IP}] end.

%
% Public interface
%
start_link(DHTPort) ->
    gen_server:start_link({local, srv_name()}, ?MODULE, [DHTPort], []).


%
%
%
-spec ping(ipaddr(), portnum()) -> pang | nodeid().
ping(IP, Port) ->
    gen_server:call(srv_name(), {ping, IP, Port}).

%
%
%
-spec find_node(ipaddr(), portnum(), nodeid()) ->
    {'error', 'timeout'} | {nodeid(), list(nodeinfo())}.
find_node(IP, Port, Target)  -> {error, timeout}.

%
%
%
-spec get_peers(ipaddr(), portnum(), infohash()) ->
    {nodeid(), token(), list(peerinfo()), list(nodeinfo())}.
get_peers(IP, Port, InfoHash)  ->
    ok.

%
%
%
-spec announce(ipaddr(), portnum(), infohash(), token(), portnum()) ->
    {'error', 'timeout'} | nodeid().
announce(IP, Port, InfoHash, Token, BTPort) ->
    ok.

%% ==================================================================

init([DHTPort]) ->
    {ok, Socket} = gen_udp:open(DHTPort, socket_options()),
    State = #state{socket=Socket,
                   node_address={{0,0,0,0}, DHTPort},
                   next_transaction_id=new_transaction_id(),
                   instance_id=new_instance_id()},
    {ok, State}.


handle_call({ping, IP, Port}, _From, State) ->
    #state{socket=Socket,
           node_address=NodeAddress,
           next_transaction_id=TransactionId,
           instance_id=InstanceId} = State,
    ConnectionId = new_connection_id(),
    RequestHeader = #request_header{
        action=action_num(ping_request),
        connection_id=ConnectionId,
        transaction_id=TransactionId,
        instance_id=InstanceId,
        local_protocol_version=proto_version_num(supported),
        node_address=NodeAddress,
        protocol_version=proto_version_num(supported),
        time=milliseconds_since_epoch()
    },
    Request = encode_request_header(RequestHeader),
    io:format(user, "Request ~p~n", [Request]),
    gen_udp:send(Socket, IP, Port, Request),
    State2 = State#state{next_transaction_id=next_transaction_id(TransactionId)},
    {reply, ok, State2}.

handle_cast(not_implemented, State) ->
    {noreply, State}.

handle_info({udp, Socket, IP, Port, Packet} = Mess, State) ->
    io:format("UDP in: ~p~n", [Mess]),
    #state{socket=Socket,
           instance_id=InstanceId} = State,
    case packet_type(Packet) of
        request ->
            {RequestHeader, _} = decode_request_header(Packet),
            #request_header{
                action=ActionNum,
                transaction_id=TransactionId,
                connection_id=ConnectionId
            } = RequestHeader,
            case action_name(ActionNum) of
                ping_request ->
                    ReplyHeader = #reply_header{
                        action=action_num(ping_reply),
                        transaction_id=TransactionId,
                        connection_id=ConnectionId,
                        protocol_version=proto_version_num(supported),
                        instance_id=InstanceId
                    },
                    Request = encode_reply_header(ReplyHeader),
                    gen_udp:send(Socket, IP, Port, Request),
                    ok;
                undefined -> ok;
                _ -> ok
            end;
        reply ->
            {ReplyHeader, _} = decode_reply_header(Packet),
            #reply_header{action=ActionNum} = ReplyHeader,
            io:format(user, "Reply ~p~n", [ReplyHeader]),
            case action_name(ActionNum) of
                ping_reply ->
                    ok;
                undefined -> ok;
                _ -> ok
            end
    end,
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_, _State) ->
    ok.

code_change(_, _, State) ->
    {ok, State}.


%% ==================================================================
%% Serialization

decode_byte(<<H, T/binary>>) -> {H, T}.
decode_short(<<H:16/big-integer, T/binary>>) -> {H, T}.
decode_int(<<H:32/big-integer, T/binary>>) -> {H, T}.
decode_long(<<H:64/big-integer, T/binary>>) -> {H, T}.
decode_connection_id(<<1:1, H:63/big-integer, T/binary>>) -> {H, T}.

decode_boolean(<<0, T/binary>>) -> {false, T};
decode_boolean(<<1, T/binary>>) -> {true, T}.

%% First byte indicates length of the IP address (4 for IPv4, 16 for IPv6);
%% next comes the address in network byte order;
%% the last value is port number as short
decode_address(<<4, A, B, C, D, Port:16/big-integer, T/binary>>) ->
    {{{A,B,C,D}, Port}, T}.


%% First byte indicates contact type, which must be UDP (1);
%% second byte indicates the contact's protocol version;
%% the rest is an address.
decode_contact(<<1, ProtoVersion, T/binary>>) ->
    {Address, T1} = decode_address(T),
    {{ProtoVersion, Address}, T1}.



encode_byte(X) -> <<X>>.
encode_short(X) -> <<X:16/big-integer>>.
encode_int(X) -> <<X:32/big-integer>>.
encode_long(X) -> <<X:64/big-integer>>.

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
encode_contact({ProtoVersion, Address}) ->
    <<1, ProtoVersion, (encode_address(Address))/binary>>.

encode_request_header(#request_header{
        connection_id=ConnectionId,
        action=Action,
        transaction_id=TransactionId,
        protocol_version=ProtoVersion,
        vendor_id=VendorId,
        network_id=NetworkId,
        local_protocol_version=LocalProtoVersion,
        node_address=NodeAddress,
        instance_id=InstanceId,
        time=Time}) ->
    [encode_long(ConnectionId),
     encode_int(Action),
     encode_int(TransactionId),
     encode_byte(ProtoVersion),
     encode_byte(VendorId),
     encode_int(NetworkId),
     encode_byte(LocalProtoVersion),
     encode_address(NodeAddress),
     encode_int(InstanceId),
     encode_long(Time)
    ].

encode_reply_header(#reply_header{
        action=Action,
        transaction_id=TransactionId,
        connection_id=ConnectionId,
        protocol_version=ProtoVersion,
        vendor_id=VendorId,
        network_id=NetworkId,
        instance_id=InstanceId
    }) ->
    [encode_int(Action),
     encode_int(TransactionId),
     encode_long(ConnectionId),
     encode_byte(ProtoVersion),
     encode_byte(VendorId),
     encode_int(NetworkId),
     encode_int(InstanceId)
    ].

decode_request_header(Bin) ->
    {ConnectionId,  Bin1} = decode_long(Bin),
    {Action,        Bin2} = decode_int(Bin1),
    {TransactionId, Bin3} = decode_int(Bin2),
    {ProtoVersion,  Bin4} = decode_byte(Bin3),
    {VendorId,      Bin5} = decode_byte(Bin4),
    {NetworkId,     Bin6} = decode_int(Bin5),
    {LocalProtoVersion, Bin7} = decode_byte(Bin6),
    {NodeAddress,   Bin8} = decode_address(Bin7),
    {InstanceId,    Bin9} = decode_int(Bin8),
    {Time,          BinT} = decode_long(Bin9),
    Header = #request_header{
        connection_id=ConnectionId,
        action=Action,
        transaction_id=TransactionId,
        protocol_version=ProtoVersion,
        vendor_id=VendorId,
        network_id=NetworkId,
        local_protocol_version=LocalProtoVersion,
        node_address=NodeAddress,
        instance_id=InstanceId,
        time=Time},
    {Header, BinT}.

decode_reply_header(Bin) ->
    {Action,        Bin1} = decode_int(Bin),
    {TransactionId, Bin2} = decode_int(Bin1),
    {ConnectionId,  Bin3} = decode_long(Bin2),
    {ProtoVersion,  Bin4} = decode_byte(Bin3),
    {VendorId,      Bin5} = decode_byte(Bin4),
    {NetworkId,     Bin6} = decode_int(Bin5),
    {InstanceId,    BinT} = decode_int(Bin6),
    Header = #reply_header{
        action=Action,
        transaction_id=TransactionId,
        connection_id=ConnectionId,
        protocol_version=ProtoVersion,
        vendor_id=VendorId,
        network_id=NetworkId,
        instance_id=InstanceId
    },
    {Header, BinT}.

new_connection_id() ->
    ?LONG_MSB bor crypto:rand_uniform(0, ?LONG_MSB).

new_instance_id() ->
    crypto:rand_uniform(0, ?MAX_UINT+1).

%% Init a transaction counter.
new_transaction_id() ->
    crypto:rand_uniform(0, ?MAX_TRANSACTION_ID).

next_transaction_id(TransactionId) -> TransactionId + 1.

packet_type(<<1:1, _/bitstring>>) -> request;
packet_type(<<0:1, _/bitstring>>) -> reply.


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
    supported            -> 26
    end.

action_num(ActionName) ->
    case ActionName of
        ping_request -> 1024;
        ping_reply   -> 1025;
        _            -> undefined
    end.

action_name(ActionNum) ->
    case ActionNum of
        1024 -> ping_request;
        1025 -> ping_reply;
        _    -> undefined
    end.

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
    [?_assertEqual({Decoded, <<>>}, decode_request_header(Encoded))
    ,?_assertEqual(Encoded, iolist_to_binary(encode_request_header(Decoded)))
    ].

-endif.
