-module(bucket_store_ets).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-define(TAB, bucket_store_ets_docs).
-define(IDXTAB, bucket_store_ets_buckets).


-record(bucket_entry, {hash, key, bucket, doc=[]}).
-record(bucket_idx_entry, {bucket, hashes=dict:new()}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% bucket_server Function Exports
%% ------------------------------------------------------------------
-export([
	 start_link/0, 
	 start/0,
	 store/3,
	 fetch/2,
	 list_keys/1,
	 list_buckets/0,
	 map/2,
	 foldl/3,
	 find/2
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


 
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start() ->
    gen_server:start({local, ?SERVER}, ?MODULE, [],[]).

store(Bucket, Key, Doc) ->
    gen_server:call(?SERVER, {store, Bucket, Key, Doc}).

fetch(Bucket, Key) ->
    case ets:lookup(?TAB, make_hash(Bucket, Key)) of
	[Doc] -> {ok, Doc};
	[] ->
	    list_keys(Bucket),
	    throw({badarg, [{key, Key}]})
    end.

list_keys(Bucket) ->
    Hashes = case ets:lookup(?IDXTAB, Bucket) of
		      [#bucket_idx_entry{hashes=HashesFound}] -> HashesFound;
		      [] -> throw({badarg, [{bucket, Bucket}]})
		  end,
    dict:fetch_keys(Hashes).

list_buckets() ->
    ets:select(?IDXTAB,[{ #bucket_idx_entry{bucket='$1',hashes='_'}, 
		       [],
		       ['$1']}]).

map(Bucket, MapFn) ->
    lists:map(MapFn, fetch_all(Bucket)).

foldl(Bucket, FoldFn, Acc0) ->
    lists:foldl(FoldFn, Acc0, fetch_all(Bucket)).

find(Bucket, DocSpec) ->
    ets:select(?TAB,[{#bucket_entry{hash='_',bucket=Bucket,key='_',doc=DocSpec}, 
		       [],
		       ['$_']}]).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    ets:new(?TAB, [set, named_table, {keypos, 2}]),
    ets:new(?IDXTAB, [set, named_table, {keypos, 2}]),
    {ok, []}.

handle_call({store, Bucket, Key, Doc}, _From, State) ->
    Hash = make_hash(Bucket, Key),
    Entry = #bucket_entry{hash=Hash, key=Key, bucket=Bucket, doc=Doc},
    io:format("Inserting following entry...~n~w~n",[Entry]),
    ets:insert(?TAB, Entry),
    Hashes = case ets:lookup(?IDXTAB, Bucket) of
		      [#bucket_idx_entry{hashes=HashesFound}] -> HashesFound;
		      [] -> dict:new()
		  end,
    Hashes2 = dict:store(Key, Hash, Hashes),
    ets:insert(?IDXTAB, #bucket_idx_entry{bucket=Bucket,hashes=Hashes2}),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

fetch_all(Bucket) ->
    [Doc || {ok, Doc} <- [fetch(Bucket, K) || K <- list_keys(Bucket)]].
 

make_hash(Bucket, Key) ->
    BucketIOData = case is_iodata(Bucket) of
		       true -> Bucket;
		       false -> io_lib:format("~w",[Bucket])
		   end,
    KeyIOData = case is_iodata(Key) of
		       true -> Key;
		       false -> io_lib:format("~w",[Key])
		   end,
    Ctx = crypto:sha_init(),
    Ctx2 = crypto:sha_update(Ctx, BucketIOData),
    Ctx3 = crypto:sha_update(Ctx2, KeyIOData),
    crypto:sha_final(Ctx3).


%% Below is adapted from http://erlang.org/pipermail/erlang-questions/2009-May/044074.html, Maxim Treskin
is_iodata(B) when is_binary(B) -> true;
is_iodata(L) -> is_iolist(L).

is_iolist([]) -> true;
is_iolist([X|Xs]) when is_integer(X), X >= 0, X =< 255 ->
     is_iodata(Xs);
is_iolist([X|Xs]) ->
    case is_iodata(X) of
	true -> is_iodata(Xs);
	false -> false
    end;
is_iolist(_) -> false.
