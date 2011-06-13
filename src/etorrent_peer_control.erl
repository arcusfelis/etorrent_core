%% @author Jesper Louis Andersen <jesper.louis.andersen@gmail.com>
%% @doc Manage and control peer communication.
%% <p>This gen_server handles the communication with a single peer. It
%% handles incoming connections, and transmits the right messages back
%% to the peer, according to the specification of the BitTorrent
%% procotol.</p>
%% <p>Each peer runs one gen_server of this kind. It handles the
%% queueing of pieces, requestal of new chunks to download, choking
%% states, the remotes request queue, etc.</p>
%% @end
-module(etorrent_peer_control).

-behaviour(gen_server).

-include("etorrent_rate.hrl").
-include("log.hrl").

%% API
-export([start_link/7,
        choke/1,
        unchoke/1,
        initialize/2,
        incoming_msg/2,
        stop/1]).

%% gproc registry entries
-export([register_server/2,
         lookup_server/1,
         await_server/1,
         lookup_peers/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         format_status/2]).

-type torrentid() :: etorrent_types:torrent_id().
-type pieceindex() :: etorrent_types:pieceindex().
-type pieceset() :: etorrent_pieceset:pieceset().
-type peerstate() :: etorrent_peerstate:peerstate().
-type peerconf() :: etorrent_peerconf:peerconf().
-type tservices() :: etorrent_download:tservices().
-record(state, {
    torrent_id = exit(required) :: integer(),
    info_hash = exit(required) ::  binary(),
    socket = none  :: none | gen_tcp:socket(),
    send_pid :: pid(),

    download = exit(required) :: tservices(),
    rate :: etorrent_rate:rate(),

    remote = exit(required) :: peerstate(),
    local  = exit(required) :: peerstate(),
    config = exit(required) :: peerconf()}).


-define(DEFAULT_CHUNK_SIZE, 16384). % Default size for a chunk. All clients use this.
-define(HIGH_WATERMARK, 30). % How many chunks to queue up to
-define(LOW_WATERMARK, 5).  % Requeue when there are less than this number of pieces in queue

-ignore_xref([{start_link, 6}]).

%%====================================================================

%% @doc Register the current process as a peer process
register_server(TorrentID, Socket) ->
    etorrent_utils:register(server_name(Socket)),
    etorrent_utils:register_member(group_name(TorrentID)).

%% @doc Lookup the process id of a specific peer.
lookup_server(Socket) ->
    etorrent_utils:lookup(server_name(Socket)).

%% @doc
await_server(Socket) ->
    etorrent_utils:await(server_name(Socket)).

%% @doc
-spec lookup_peers(torrentid()) -> [pid()].
lookup_peers(TorrentID) ->
    etorrent_utils:lookup_members(group_name(TorrentID)).


%% @doc Name of a specific peer process
server_name(Socket) ->
    {etorrent, Socket, peer}.

%& @doc Name of all peers in a torrent
group_name(TorrentID) ->
    {etorrent, TorrentID, peers}.




%% @doc Starts the server
%% @end
start_link(TrackerUrl, LocalPeerId, InfoHash, Id, {IP, Port}, Caps, Socket)
  when is_binary(LocalPeerId) ->
    gen_server:start_link(?MODULE, [TrackerUrl, LocalPeerId, InfoHash,
                                    Id, {IP, Port}, Caps, Socket], []).

%% @doc Gracefully ask the server to stop.
%% @end
stop(Pid) ->
    gen_server:cast(Pid, stop).

%% @doc Choke the peer.
%% <p>The intended caller of this function is the {@link etorrent_choker}</p>
%% @end
choke(Pid) ->
    gen_server:cast(Pid, choke).

%% @doc Unchoke the peer.
%% <p>The intended caller of this function is the {@link etorrent_choker}</p>
%% @end
unchoke(Pid) ->
    gen_server:cast(Pid, unchoke).

%% @doc Initialize the connection.
%% <p>The `Way' parameter tells the client of the connection is
%% `incoming' or `outgoing'. They are handled differently since part
%% of the handshake is already completed for incoming connections.</p>
%% @end
-type direction() :: incoming | outgoing.
-spec initialize(pid(), direction()) -> ok.
initialize(Pid, Way) ->
    gen_server:cast(Pid, {initialize, Way}).


%% @doc Inject an incoming message to the process.
%% <p>This is the main "Handle-incoming-messages" call. The intended
%% caller is {@link etorrent_peer_recv}, whenever a message arrives on
%% the socket.</p>
%% @end
incoming_msg(Pid, Msg) ->
    gen_server:cast(Pid, {incoming_msg, Msg}).


%% @doc Check if a peer provided an interesting piece.
%% This function should be called when a have-message is received.
%% If the piece is interesting and we are not already interested a
%% status update is sent to the local process to trigger a state
%% change.
%% @end
-spec check_local_interest(pieceindex() | pieceset(),
                           peerstate(), pid()) -> peerstate().
check_local_interest(Pieces, Local, SendPid) ->
    case etorrent_peerstate:interesting(Pieces, Local) of
        Local ->
            Local;
        NewLocal ->
            ok = etorrent_peer_send:interested(SendPid),
            NewLocal
    end.


%% @doc Check if a peer still provides interesting pieces.
%% This function should be called when a have-message is sent. If the
%% peer no longer provides any interesting pieces and we are interested
%% a status update is sent to the local process to trigger a state change.
%% @end
-spec recheck_local_interest(pieceindex(), peerstate(),
                             peerstate(), pid()) -> peerstate().
recheck_local_interest(Piece, Remote, Local, SendPid) ->
    case etorrent_peerstate:interesting(Piece, Remote, Local) of
        Local ->
            Local;
        NewLocal ->
            ok = etorrent_peer_send:not_interested(SendPid),
            NewLocal
    end.


%% @doc Check the remote peer has become a seeder
%% This is only worth checking if we are also seeding the torrent.
%% We are expected to close the connection if both peers are seeders,
%% exit with reason badarg if we find ourselves in this situation,
%% it should have been handled elsewhere.
%% Return an updated copy of the remote state and send a notification
%% to ourselves if the remote peer became a seeder.
%% @end
-spec check_remote_seeder(peerstate(), peerstate()) -> peerstate().
check_remote_seeder(Remote, Local) ->
    case etorrent_peerstate:seeding(Remote, Local) of
        Remote ->
            Remote;
        _ ->
            exit(seeder)
    end.

%% @doc Check if the request queue is low
-spec poll_queue(tservices(), pid(), peerstate(), peerstate()) -> peerstate().
poll_queue(Download, SendPid, Remote, Local) ->
    case etorrent_peerstate:needreqs(Local) of
        false ->
            Local;
        true  ->
            Requests = etorrent_peerstate:requests(Local),
            Pieces = etorrent_peerstate:pieces(Remote),
            Needs = etorrent_rqueue:needs(Requests),
            case etorrent_download:request_chunks(Needs, Pieces, Download) of
                {ok, assigned} ->
                    Local;
                {ok, Chunks} ->
                    [etorrent_peer_send:local_request(SendPid, Chunk)
                    || Chunk <- Chunks],
                    NewRequests = etorrent_rqueue:push(Chunks, Requests),
                    etorrent_peerstate:requests(NewRequests, Local)
            end
    end.



%% @private
init([TrackerUrl, LocalPeerID, InfoHash, TorrentID, {IP, Port}, Caps, Socket]) ->
    random:seed(now()),
    %% Use socket handle as remote peer-id.
    register_server(TorrentID, Socket),
    Download = etorrent_download:await_servers(TorrentID),

    %% Keep track of the local state and the remote state
    TorrentPid  = etorrent_torrent_ctl:await_server(TorrentID),
    {ok, Valid} = etorrent_torrent_ctl:valid_pieces(TorrentPid),
    Numpieces   = etorrent_pieceset:capacity(Valid),
    Local0 = etorrent_peerstate:new(Numpieces),
    Local  = etorrent_peerstate:pieces(Valid, Local0),
    Remote = etorrent_peerstate:new(Numpieces),

    Extended = proplists:get_bool(extended_messaging, Caps),
    Config0  = etorrent_peerconf:new(),
    Config1  = etorrent_peerconf:localid(LocalPeerID, Config0),
    Config   = etorrent_peerconf:extended(Extended, Config1),


    ok = etorrent_table:new_peer(TrackerUrl, IP, Port, TorrentID, self(), leeching),
    ok = etorrent_choker:monitor(self()),
    State = #state{
        torrent_id=TorrentID,
        info_hash=InfoHash,
        socket=Socket,
        download=Download,
        remote=Remote,
        local=Local,
        config=Config},
    {ok, State}.

%% @private
handle_cast({initialize, Way}, S) ->
    case etorrent_counters:obtain_peer_slot() of
        ok ->
            case connection_initialize(Way, S) of
                {ok, NS} -> {noreply, NS};
                {stop, Type} -> {stop, Type, S}
            end;
        full ->
            {stop, normal, S}
    end;
handle_cast({incoming_msg, Msg}, S) ->
    case handle_message(Msg, S) of
        {ok, NS} -> {noreply, NS};
        {stop, Reason, NS} -> {stop, Reason, NS}
    end;
handle_cast(choke, S) ->
    etorrent_peer_send:choke(S#state.send_pid),
    {noreply, S};

handle_cast(unchoke, S) ->
    etorrent_peer_send:unchoke(S#state.send_pid),
    {noreply, S};

handle_cast(interested, State) ->
    self() ! {interested, true},
    {noreply, State};

handle_cast(stop, S) ->
    {stop, normal, S};

handle_cast(Msg, State) ->
    ?WARN([unknown_msg, Msg]),
    {noreply, State}.


%% @private
handle_info({chunk, {fetched, Index, Offset, Length, _}}, State) ->
    #state{send_pid=SendPid, local=Local} = State,
    Requests = etorrent_peerstate:requests(Local),
    Hasrequest = etorrent_rqueue:member(Index, Offset, Length, Requests),
    NewLocal = if
        not Hasrequest ->
            Local;
        Hasrequest ->
            etorrent_peer_send:cancel(SendPid, Index, Offset, Length),
            NewReqs = etorrent_rqueue:delete(Index, Offset, Length, Requests),
            etorrent_peerstate:requests(NewReqs, Local)
    end,
    NewState = State#state{local=NewLocal},
    {noreply, NewState};

handle_info({piece, {valid, Piece}}, State) ->
    #state{send_pid=SendPid, download=Download, local=Local, remote=Remote} = State,
    WithLocal = etorrent_peerstate:hasone(Piece, Local),
    ok        = etorrent_peer_send:have(SendPid, Piece),
    TmpLocal  = recheck_local_interest(Piece, Remote, WithLocal, SendPid),
    NewLocal  = poll_queue(Download, SendPid, Remote, TmpLocal),
    NewState  = State#state{local=NewLocal},
    {noreply, NewState};

handle_info({piece, {unassigned, _}}, State) ->
    #state{download=Download, send_pid=SendPid, local=Local, remote=Remote} = State,
    NewLocal = poll_queue(Download, SendPid, Remote, Local),
    NewState = State#state{local=NewLocal},
    {noreply, NewState};

%% etorrent_peerstate:interested(self()),
handle_info({peer, {check, seeder}}, State) ->
    {noreply, State};


handle_info({download, Update}, State) ->
    #state{download=Download} = State,
    NewDownload = etorrent_download:update(Update, Download),
    NewState = State#state{download=NewDownload},
    {noreply, NewState};
            
handle_info({tcp, _, _}, State) ->
    ?ERR([wrong_controller]),
    {noreply, State};

handle_info(Info, State) ->
    ?WARN([unknown_msg, Info]),
    {noreply, State}.

%% @private
terminate(_Reason, _S) ->
    ok.

%% @private
handle_call(Request, _From, State) ->
    ?WARN([unknown_handle_call, Request]),
    {noreply, State}.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_Pdict, S]) ->
    #state{config=Config} = S,
    RemoteID = etorrent_peerconf:remoteid(Config),
    IPPort = case inet:peername(S#state.socket) of
        {ok, IPP} -> IPP;
        {error, Reason} -> {port_error, Reason}
    end,
    Term = [
        {torrent_id,     S#state.torrent_id},
        {remote_peer_id, RemoteID},
        {info_hash,      S#state.info_hash},
        {socket_info,    IPPort}],
    [{data,  [{"State",  Term}]}].
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Func: handle_message(Msg, State) -> {ok, NewState} | {stop, Reason, NewState}
%% Description: Process an incoming message Msg from the wire. Return either
%%  {ok, S} if the processing was ok, or {stop, Reason, S} in case of an error.
%%--------------------------------------------------------------------
-spec handle_message(_,_) -> {'ok',_} | {'stop', _, _}.
handle_message(keep_alive, S) ->
    {ok, S};
handle_message(choke, State) ->
    #state{
        torrent_id=TorrentID,
        local=Local,
        config=Config,
        download=Download} = State,
    ok = etorrent_peer_states:set_choke(TorrentID, self()),
    NewState = case etorrent_peerconf:fast(Config) of
        true ->
            %% If the Fast Extension is enabled a CHOKE message does
            %% not imply that all outstanding requests are dropped.
            NewLocal = etorrent_peerstate:choked(true, Local),
            State#state{local=NewLocal};
        false ->
            %% A CHOKE message implies that all outstanding requests has been dropped.
            Requests = etorrent_peerstate:requests(Local),
            Pieces = etorrent_rqueue:pieces(Requests),
            Chunks = etorrent_rqueue:to_list(Requests),
            Peers  = etorrent_peer_control:lookup_peers(TorrentID),
            ok = etorrent_piecestate:unassigned(Pieces, Peers),
            ok = etorrent_download:chunks_dropped(Chunks, Download),
            NewReqs = etorrent_rqueue:flush(Requests),
            TmpLocal = etorrent_peerstate:choked(true, Local),
            NewLocal = etorrent_peerstate:requests(NewReqs, TmpLocal),
            State#state{local=NewLocal}
    end,
    {ok, NewState};

handle_message(unchoke, State) ->
    #state{torrent_id=TorrentID} = State,
    #state{send_pid=SendPid, download=Download, local=Local, remote=Remote} = State,
    ok = etorrent_peer_states:set_unchoke(TorrentID, self()),
    TmpLocal = etorrent_peerstate:choked(false, Local),
    NewLocal = poll_queue(Download, SendPid, Remote, TmpLocal),
    NewState = State#state{local=NewLocal},
    {ok, NewState};

handle_message(interested, State) ->
    #state{torrent_id=TorrentID, send_pid=SendPid, remote=Remote} = State,
    ok = etorrent_peer_states:set_interested(TorrentID, self()),
    ok = etorrent_peer_send:check_choke(SendPid),
    NewRemote = etorrent_peerstate:interested(true, Remote),
    NewState = State#state{remote=NewRemote},
    {ok, NewState};

handle_message(not_interested, State) ->
    #state{torrent_id=TorrentID, send_pid=SendPid, remote=Remote} = State,
    ok = etorrent_peer_states:set_not_interested(TorrentID, self()),
    ok = etorrent_peer_send:check_choke(SendPid),
    NewRemote = etorrent_peerstate:interested(false, Remote),
    NewState = State#state{remote=NewRemote},
    {ok, NewState};

handle_message({request, Index, Offset, Len}, S) ->
    etorrent_peer_send:remote_request(S#state.send_pid, Index, Offset, Len),
    {ok, S};

handle_message({cancel, Index, Offset, Len}, S) ->
    etorrent_peer_send:cancel(S#state.send_pid, Index, Offset, Len),
    {ok, S};

handle_message({suggest, Piece}, State) ->
    #state{config=Config} = State,
    PeerID = etorrent_peerconf:remoteid(Config),
    ?INFO([{peer_id, PeerID}, {suggest, Piece}]),
    {ok, State};

handle_message({have, Piece}, State) ->
    #state{torrent_id=TorrentID, send_pid=SendPid, download=Download, remote=Remote, local=Local} = State,
    TmpRemote = etorrent_peerstate:hasone(Piece, Remote),
    Pieceset  = etorrent_peerstate:pieces(TmpRemote),
    %% TODO - see etorrent_peerstate:haspieces/1
    HasPieces = etorrent_peerstate:haspieces(Remote),
    HasPieces orelse etorrent_scarcity:add_peer(TorrentID, Pieceset),
    ok        = etorrent_scarcity:add_piece(TorrentID, Piece, Pieceset),
    TmpLocal  = check_local_interest(Piece, Local, SendPid),
    NewRemote = check_remote_seeder(TmpRemote, TmpLocal),
    NewLocal  = poll_queue(Download, SendPid, NewRemote, TmpLocal),
    NewState  = State#state{remote=NewRemote, local=NewLocal},
    {ok, NewState};

handle_message(have_none, State) ->
    #state{torrent_id=TorrentID, remote=Remote, config=Config} = State,
    etorrent_peerconf:fast(Config) orelse erlang:error(badarg),
    NewRemote = etorrent_peerstate:hasnone(Remote),
    Pieceset  = etorrent_peerstate:pieces(NewRemote),
    ok        = etorrent_scarcity:add_peer(TorrentID, Pieceset),
    NewState  = State#state{remote=NewRemote},
    {ok, NewState};

%%IsSeeder andalso etorrent_table:statechange_peer(self(), seeder),
handle_message(have_all, State) ->
    #state{torrent_id=TorrentID, send_pid=SendPid, download=Download, remote=Remote, local=Local, config=Config} = State,
    etorrent_peerconf:fast(Config) orelse erlang:error(badarg),
    TmpRemote = etorrent_peerstate:hasall(Remote),
    Pieceset  = etorrent_peerstate:pieces(TmpRemote),
    ok        = etorrent_scarcity:add_peer(TorrentID, Pieceset),
    TmpLocal  = check_local_interest(Pieceset, Local, SendPid),
    NewRemote = check_remote_seeder(TmpRemote, TmpLocal),
    NewLocal  = poll_queue(Download, SendPid, NewRemote, TmpLocal),
    NewState  = State#state{remote=NewRemote, local=NewLocal},
    {ok, NewState};

handle_message({bitfield, Bitfield}, State) ->
    #state{torrent_id=TorrentID, send_pid=SendPid, download=Download, local=Local, remote=Remote} = State,
    TmpRemote = etorrent_peerstate:hasset(Bitfield, Remote),
    Pieceset  = etorrent_peerstate:pieces(TmpRemote),
    ok        = etorrent_scarcity:add_peer(TorrentID, Pieceset),
    TmpLocal  = check_local_interest(Pieceset, Local, SendPid),
    NewRemote = check_remote_seeder(TmpRemote, TmpLocal),
    NewLocal  = poll_queue(Download, SendPid, NewRemote, TmpLocal),
    NewState  = State#state{remote=NewRemote, local=NewLocal},
    {ok, NewState};

handle_message({piece, Index, Offset, Data}, State) ->
    #state{torrent_id=TorrentID} = State,
    #state{send_pid=SendPid, download=Download, local=Local, remote=Remote} = State,
    Length = byte_size(Data),
    Requests = etorrent_peerstate:requests(Local),
    NewLocal = case etorrent_rqueue:is_head(Index, Offset, Length, Requests) of
        true ->
            ok = etorrent_download:chunk_fetched(Index, Offset, Length, Download),
            ok = etorrent_io:write_chunk(TorrentID, Index, Offset, Data),
            ok = etorrent_download:chunk_stored(Index, Offset, Length, Download),
            NewRequests = etorrent_rqueue:pop(Requests),
            TmpLocal = etorrent_peerstate:requests(NewRequests, Local),
            poll_queue(Download, SendPid, Remote, TmpLocal);
        false ->
            %% Stray piece, we could try to get hold of it but for now we just
            %% throw it on the floor. TODO - crash if the fast extension is enabled?
            Local
    end,
    NewState = State#state{local=NewLocal},
    {ok, NewState};

handle_message({extended, 0, _Data}, State) ->
    #state{config=Config} = State,
    etorrent_peerconf:extended(Config) orelse erlang:error(badarg),
    %% Disable the extended messaging for now,
    %?INFO([{extended_message, etorrent_bcoding:decode(BCode)}]),
    %% We could consider storing the information here, if needed later on,
    %% but for now we simply ignore that.
    {ok, State};

handle_message(Unknown, State) ->
    ?WARN([unknown_message, Unknown]),
    {stop, normal, State}.


% @doc Initialize the connection, depending on the way the connection is
connection_initialize(incoming, State) ->
    #state{
        torrent_id=TorrentID,
        socket=Socket,
        info_hash=Infohash,
        local=Local,
        config=Config} = State,
    Extended = etorrent_peerconf:extended(Config),
    LocalID = etorrent_peerconf:localid(Config),
    Valid = etorrent_peerstate:pieces(Local),
    case etorrent_proto_wire:complete_handshake(Socket, Infohash, LocalID) of
        ok ->
            SendPid = complete_connection_setup(Socket, TorrentID, Extended, Valid),
            NewState = State#state{send_pid=SendPid},
            {ok, NewState};
        {error, stop} ->
            {stop, normal}
    end;

connection_initialize(outgoing, State) ->
    #state{torrent_id=TorrentID, socket=Socket, local=Local, config=Config} = State,
    Extended = etorrent_peerconf:extended(Config),
    Valid = etorrent_peerstate:pieces(Local),
    SendPid = complete_connection_setup(Socket, TorrentID, Extended, Valid),
    NewState = State#state{send_pid=SendPid},
    {ok, NewState}.

%%--------------------------------------------------------------------
%% Function: complete_connection_setup(Socket, TorrentId, ExtendedMSG)
%%              -> SendPid
%% Description: Do the bookkeeping needed to set up the peer:
%%    * enable passive messaging mode on the socket.
%%    * Start the send pid
%%    * Send off the bitfield
%%--------------------------------------------------------------------
complete_connection_setup(Socket, _TorrentID, Extended, Valid) ->
    SendPid = etorrent_peer_send:await_server(Socket),
    Bitfield = etorrent_pieceset:to_binary(Valid),
    Extended andalso etorrent_peer_send:extended_msg(SendPid),
    etorrent_peer_send:bitfield(SendPid, Bitfield),
    SendPid.
