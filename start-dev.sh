#!/bin/sh
cd `dirname $0`

# NOTE: mustache templates need \ because they are not awesome.
exec erl -pa $PWD/ebin edit $PWD/deps/*/ebin \
    -boot start_sasl \
    -sname etorrent \
    -eval "io:format(\"Starting ~p~n\", [application:ensure_all_started(etorrent_core)])." \
    -config ~/.config/etorrent

