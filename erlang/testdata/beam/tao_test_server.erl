-module(tao_test_server).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    val = 0 :: integer(),
    log = [] :: list()
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #state{}}.

handle_call({echo, Term}, _From, State) ->
    {reply, {ok, Term}, State};

handle_call({sleep_reply, Ms, Value}, _From, State) ->
    timer:sleep(Ms),
    {reply, {ok, Value}, State};

handle_call({async_helper_reply, DelayMs, Value}, From, State) ->
    spawn(fun() ->
        case DelayMs > 0 of
            true -> timer:sleep(DelayMs);
            false -> ok
        end,
        gen_server:reply(From, {helper, self(), Value})
    end),
    {noreply, State};

handle_call({reset, InitVal}, _From, _State) ->
    {reply, ok, #state{val = InitVal, log = []}};

handle_call({add, Delta}, _From, #state{val = V, log = L} = State) ->
    V1 = V + Delta,
    {reply, {ok, V1}, State#state{val = V1, log = [{call, Delta} | L]}};

handle_call(get_state, _From, #state{val = V, log = L} = State) ->
    {reply, {ok, V, lists:reverse(L)}, State};

handle_call({error_tuple, Reason}, _From, State) ->
    {reply, {error, Reason}, State};

handle_call(stop_normal, _From, State) ->
    {stop, normal, ok, State};

handle_call({crash, Reason}, _From, _State) ->
    exit(Reason);

handle_call(otp_info, _From, State) ->
    Rel = list_to_binary(erlang:system_info(otp_release)),
    Erts = list_to_binary(erlang:system_info(version)),
    Creation = erlang:system_info(creation),
    {reply, {ok, #{otp_release => Rel, erts_version => Erts, node => node(), creation => Creation}}, State};

handle_call({net_adm_ping, TargetNode}, _From, State) ->
    Res = net_adm:ping(TargetNode),
    {reply, {Res, nodes(), nodes(hidden)}, State};

handle_call({call_target, Target, Req, Timeout}, _From, State) ->
    Res =
        try
            {ok, gen_server:call(Target, Req, Timeout)}
        catch
            Class:Reason ->
                {caught, Class, Reason}
        end,
    {reply, Res, State};

handle_call({send_request_target, Target, Req, Timeout}, From, State) ->
    spawn(fun() ->
        ReqId = gen_server:send_request(Target, Req),
        Res =
            case gen_server:wait_response(ReqId, Timeout) of
                {reply, Reply} -> {ok, Reply};
                timeout -> {error, timeout};
                {error, {Reason, _ServerRef}} -> {down, Reason}
            end,
        gen_server:reply(From, Res)
    end),
    {noreply, State};

handle_call({nested_alias_call, Target, Req, Timeout}, _From, State) ->
    Alias = alias([reply]),
    Tag = [[alias | Alias] | extra_meta],
    erlang:send(Target, {'$gen_call', {self(), Tag}, Req}),
    receive
        {Tag, Reply} ->
            unalias(Alias),
            {reply, {ok, Reply}, State}
    after Timeout ->
        unalias(Alias),
        {reply, {error, timeout}, State}
    end;

handle_call({plain_ref_call, Target, Req, Timeout}, _From, State) ->
    Ref = make_ref(),
    erlang:send(Target, {'$gen_call', {self(), Ref}, Req}),
    receive
        {Ref, Reply} ->
            {reply, {ok, Reply}, State}
    after Timeout ->
        {reply, {error, timeout}, State}
    end;

handle_call({cast_target, Target, Req}, _From, State) ->
    ok = gen_server:cast(Target, Req),
    {reply, ok, State};

handle_call({cast_then_call, Target, CastReqs, CallReq, Timeout}, _From, State) ->
    lists:foreach(fun(C) -> ok = gen_server:cast(Target, C) end, CastReqs),
    Res =
        try
            {ok, gen_server:call(Target, CallReq, Timeout)}
        catch
            Class:Reason ->
                {caught, Class, Reason}
        end,
    {reply, Res, State};

handle_call({callback_target, Target, InnerReq, Timeout}, _From, State) ->
    Res =
        try
            {callback_ok, gen_server:call(Target, InnerReq, Timeout)}
        catch
            Class:Reason ->
                {caught, Class, Reason}
        end,
    {reply, Res, State};

handle_call({send_raw, Target, Msg}, _From, State) ->
    erlang:send(Target, Msg),
    {reply, ok, State};

handle_call({sys_get_state, Target, Timeout}, _From, State) ->
    Res =
        try
            {ok, sys:get_state(Target, Timeout)}
        catch
            Class:Reason ->
                {caught, Class, Reason}
        end,
    {reply, Res, State};

handle_call({gen_stop, Target, Timeout}, _From, State) ->
    Res =
        try
            {ok, gen_server:stop(Target, normal, Timeout)}
        catch
            Class:Reason ->
                {caught, Class, Reason}
        end,
    {reply, Res, State};

handle_call({watch_target, Target}, _From, State) ->
    Parent = self(),
    Watcher = spawn(fun() ->
        MRef = erlang:monitor(process, Target),
        Parent ! {watcher_ready, self(), MRef},
        watcher_loop(MRef, none)
    end),
    receive
        {watcher_ready, Watcher, MRef} ->
            {reply, {ok, Watcher, MRef}, State}
    after 5000 ->
        {reply, {error, watcher_timeout}, State}
    end;

handle_call({await_down, WatcherPid, Timeout}, _From, State) ->
    Ref = make_ref(),
    WatcherPid ! {get_down, self(), Ref, Timeout},
    receive
        {down_result, Ref, Res} ->
            {reply, Res, State}
    after Timeout + 1000 ->
        {reply, {error, await_timeout}, State}
    end;

handle_call({demonitor_watcher, WatcherPid}, _From, State) ->
    Ref = make_ref(),
    WatcherPid ! {do_demonitor, self(), Ref},
    receive
        {demonitored, Ref} ->
            {reply, ok, State}
    after 2000 ->
        {reply, {error, timeout}, State}
    end;

handle_call({spawn_worker, TrapExit}, _From, State) ->
    Parent = self(),
    Worker = spawn(fun() ->
        process_flag(trap_exit, TrapExit),
        Parent ! {worker_ready, self()},
        worker_loop([])
    end),
    receive
        {worker_ready, Worker} ->
            {reply, {ok, Worker}, State}
    after 5000 ->
        {reply, {error, worker_timeout}, State}
    end;

handle_call({worker_op, Worker, Op}, _From, State) ->
    Ref = make_ref(),
    Worker ! {worker_cmd, self(), Ref, Op},
    receive
        {worker_resp, Ref, Resp} ->
            {reply, Resp, State}
    after 5000 ->
        {reply, {error, worker_op_timeout}, State}
    end;

handle_call({send_exit2, TargetPid, Reason}, _From, State) ->
    erlang:exit(TargetPid, Reason),
    {reply, ok, State};

handle_call(_Unknown, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({add, Delta}, #state{val = V, log = L} = State) ->
    V1 = V + Delta,
    {noreply, State#state{val = V1, log = [{cast, Delta} | L]}};

handle_cast({notify, Target, Term}, State) ->
    erlang:send(Target, {cast_seen, Term}),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({ping_info, ReplyTo, Term}, #state{val = V, log = L} = State) ->
    erlang:send(ReplyTo, {pong_info, Term}),
    {noreply, State#state{log = [{info, Term} | L], val = V}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

watcher_loop(MRef, CachedDown) ->
    receive
        {'DOWN', MRef, process, Obj, Reason} when CachedDown =:= none ->
            watcher_loop(MRef, {down, MRef, Obj, Reason});
        {get_down, ReplyTo, ReqRef, _Timeout} when CachedDown =/= none ->
            ReplyTo ! {down_result, ReqRef, CachedDown};
        {get_down, ReplyTo, ReqRef, Timeout} ->
            receive
                {'DOWN', MRef, process, Obj, Reason} ->
                    ReplyTo ! {down_result, ReqRef, {down, MRef, Obj, Reason}}
            after Timeout ->
                ReplyTo ! {down_result, ReqRef, {error, timeout}},
                watcher_loop(MRef, none)
            end;
        {do_demonitor, ReplyTo, ReqRef} ->
            erlang:demonitor(MRef, [flush]),
            ReplyTo ! {demonitored, ReqRef}
    end.

worker_loop(Inbox) ->
    receive
        {worker_cmd, ReplyTo, Ref, {link_to, TargetPid}} ->
            Res =
                try
                    erlang:link(TargetPid),
                    ok
                catch
                    Class:Reason -> {caught, Class, Reason}
                end,
            ReplyTo ! {worker_resp, Ref, Res},
            worker_loop(Inbox);
        {worker_cmd, ReplyTo, Ref, {unlink_from, TargetPid}} ->
            erlang:unlink(TargetPid),
            ReplyTo ! {worker_resp, Ref, ok},
            worker_loop(Inbox);
        {worker_cmd, ReplyTo, Ref, {send_exit, TargetPid, Reason}} ->
            erlang:exit(TargetPid, Reason),
            ReplyTo ! {worker_resp, Ref, ok},
            worker_loop(Inbox);
        {worker_cmd, ReplyTo, Ref, {exit_self, Reason}} ->
            ReplyTo ! {worker_resp, Ref, ok},
            exit(Reason);
        {worker_cmd, ReplyTo, Ref, drain_inbox} ->
            ReplyTo ! {worker_resp, Ref, {ok, lists:reverse(Inbox)}},
            worker_loop([]);
        Other ->
            worker_loop([Other | Inbox])
    end.
