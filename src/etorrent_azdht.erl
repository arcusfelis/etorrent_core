%% @doc AZDHT utility functions.
-module(etorrent_azdht).
-export([contact/1,
         contact/2,
         contact/3,
         contacts/1,
         compact_contact/1,
         compact_contacts/1,
         node_id/1,
         node_id/3,
         higher_or_equal_version/2,
         lower_version/2,
         proto_version_num/1,
         action_name/1,
         action_request_num/1,
         action_reply_num/1,
         diversification_type/1
        ]).

-include_lib("etorrent_core/include/etorrent_azdht.hrl").

%% Long.MAX_VALUE = 9223372036854775807 = 2^63-1
-define(MAX_LONG, (1 bsl 63 - 1)).

-spec contact(proto_version(), address()) -> contact().
contact(ProtoVer, {IP, Port}) ->
    contact(ProtoVer, IP, Port).

-spec contact(proto_version(), ipaddr(), portnum()) ->
    contact().
contact(ProtoVer, IP, Port) ->
    #contact{version=ProtoVer,
             address={IP, Port},
             node_id=node_id(ProtoVer, IP, Port)}.

%% Create a contact from the compact form.
contact({ProtoVer, IP, Port}) ->
    contact(ProtoVer, IP, Port).

contacts(Constants) ->
    [contact(Constant) || Constant <- Constants].

%% Make contacts easy to print and read.
compact_contacts(Contacts) ->
    [compact_contact(Contact) || Contact <- Contacts].
compact_contact(#contact{version=ProtoVer, address={IP, Port}}) ->
    {ProtoVer, IP, Port}.

%% Node ID calculation
%% ===================

-spec node_id(contact()) -> node_id().
node_id(#contact{node_id=NodeId}) ->
    NodeId.

%% See DHTUDPUtils.getNodeID(InetSocketAddress, byte) in vuze.
-spec node_id(proto_version(), ipaddr(), portnum()) -> node_id().
node_id(ProtoVer, IP, Port) ->
    crypto:sha(node_id_key(ProtoVer, IP, Port)).

-spec node_id_key(proto_version(), ipaddr(), portnum()) -> iolist().
node_id_key(Version, IP, Port) ->
    case {version_to_key_type(Version), ip_version(IP)} of
        {uow, ipv4}          -> uow_key({IP, Port});
        %% stick with existing approach for IPv6 at the moment
        {uow, ipv6}          -> restrict_ports2_key({IP, Port});
        {restrict_ports2, _} -> restrict_ports2_key({IP, Port});
        {restrict_ports,  _} -> restrict_ports_key({IP, Port});
        {none,            _} -> simple_key({IP, Port})
    end.

version_to_key_type(Version) ->
    case higher_or_equal_version(Version, restrict_id3) of
        true -> uow;
        false ->
            case higher_or_equal_version(Version, restrict_id_ports2) of
                true -> restrict_ports2;
                false ->
                    case higher_or_equal_version(Version, restrict_id_ports) of
                        true -> restrict_ports;
                        false -> none
                    end
            end
        end.


%% restrictions suggested by UoW researchers as effective at reducing Sybil opportunity but 
%% not so restrictive as to impact DHT performance
uow_key({{A,B,C,D}, Port}) ->
    K0 = ?MAX_LONG,
    K1 = ?MAX_LONG,
    K2 = 2500,
    K3 = 50,
    K4 = 5,
    %% long    result = address.getPort() % K4;
    R0 = Port rem K4,
    %% result = ((((long)bytes[3] << 8 ) &0x000000ff00L ) | result ) % K3;
    %% result = ((((long)bytes[2] << 16 )&0x0000ff0000L ) | result ) % K2;
    %% result = ((((long)bytes[1] << 24 )&0x00ff000000L ) | result ); // % K1;
    %% result = ((((long)bytes[0] << 32 )&0xff00000000L ) | result ); // % K0;
    R1 = ((D bsl  8) bor R0) rem K3,
    R2 = ((C bsl 16) bor R1) rem K2,
    R3 = ((B bsl 24) bor R2) rem K1,
    R4 = ((A bsl 32) bor R3) rem K0,
    integer_to_list(R4).
    

restrict_ports2_key({IP, Port}) ->
    %% more draconian limit, analysis shows that of 500,000 node addresses only
    %% 0.01% had >= 8 ports active. ( 1% had 2 ports, 0.1% 3)
    %% Having > 1 node with the same ID doesn't actually cause too much grief
    %% 
    %% ia.getHostAddress() + ":" + ( address.getPort() % 8 );
    [print_ip(IP), $:, $0 + (Port rem 8)].

restrict_ports_key({IP, Port}) ->
    %% limit range to around 2000 (1999 is prime)
    %% ia.getHostAddress() + ":" + ( address.getPort() % 1999 );
    io_lib:format("~s:~B", [print_ip(IP), Port rem 1999]).

simple_key({IP, Port}) ->
    io_lib:format("~s:~B", [print_ip(IP), Port]).

ip_version(IP) ->
    case tuple_size(IP) of
        4 -> ipv4;
        8 -> ipv6
    end.

print_ip({A,B,C,D}) ->
    io_lib:format("~B.~B.~B.~B", [A,B,C,D]);
print_ip({_,_,_,_, _,_,_,_}=IP) ->
    %% 39 chars
    io_lib:format("~4.16.0B:~4.16.0B:~4.16.0B:~4.16.0B:"
                  "~4.16.0B:~4.16.0B:~4.16.0B:~4.16.0B", tuple_to_list(IP)).


%% Constants
%% =========

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
    supported            -> 50
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
        1032 -> error;
        _    -> undefined
    end.

diversification_type(1) -> none;
diversification_type(2) -> frequency;
diversification_type(3) -> size.

